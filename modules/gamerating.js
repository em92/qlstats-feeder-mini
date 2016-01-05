const
  fs = require("graceful-fs"),
  pg = require("pg"),
  zlib = require("zlib"),
  glicko = require("./glicko1"),
  log4js = require("log4js"),
  Q = require("q");

// calculate a value for "c" so that an average RD value of 85 changes back to 350 when a player is inactive for 180 rating periods (=days)
const g2 = new glicko.Glicko({ rating: 1500, rd: 350, c: Math.sqrt((Math.pow(350, 2) - Math.pow(82, 2)) / 180) });

// values for DB column games.g2_status
const
  ERR_NOTRATEDYET = 0,
  ERR_OK = 1,
  ERR_ABORTED = 2,
  ERR_ROUND_OR_TIMELIMIT = 3,
  ERR_BOTMATCH = 4,
  ERR_TEAMTIMEDIFF = 5,
  ERR_MINPLAYERS = 6,
  ERR_DATAFILEMISSING = 7,
  ERR_FACTORY_OR_SETTINGS = 8;

const rateEachSingleMatch = true;
var _config;
var resetRating;
var updateDatabase;
var onlyProcessMatchesBefore;
var printResult;
var gametype;
var strategy;
var playersBySteamId = {};
var _lastProcessedMatchStartDt;

exports.rateAllGames = rateAllGames;
exports.rateSingleGame = rateSingleGame;

const _logger = log4js.getLogger("gamerating");

function init() {
  _config = JSON.parse(fs.readFileSync(__dirname + "/../cfg.json")); 
  //Q.longStackSupport = true;
}

function rateAllGames(gt, options) {
  applyOptions(gt, options);

  return dbConnect()
    .then(function(cli) {
      return resetRatingsInDb(cli)
        .then(function() { return loadPlayers(cli); })
        .then(function() { return getMatchIds(cli); })
        .then(function(matches) { return reprocessMatches(cli, matches); })
        .then(function() { return savePlayerRatings(cli) })
        .then(function () { return playersBySteamId; })
        .finally(function() { cli.release(); });
    })
    .catch(function(err) {
      _logger.error(err);
      throw err;
    });
}

function rateSingleGame(gameId, game) {
  var gt = game.matchStats.GAME_TYPE;
  applyOptions(gt, { resetRating: false, updateDatabase: true, onlyProcessMatchesBefore: null, printResult: false });

  var steamIds = {};
  game.playerStats.forEach(function(p) {
    steamIds[p.STEAM_ID] = true;
  });
  steamIds = Object.keys(steamIds);
  
  var ratingPeriod = glickoPeriod(new Date(game.gameEndTimestamp * 1000));
  g2.setPeriod(ratingPeriod);

  dbConnect()
    .then(function(cli) {
      return Q()
        .then(function() { return loadPlayers(cli, steamIds); })
        .then(function() { return processGame(cli, gameId, game); })
        .then(function () { return savePlayerRatings(cli); })
        .then(function (results) { return printResults(results); })
        .then(Q(true))
        .finally(function() { cli.release(); });
    })
    .catch(function(err) {
      _logger.error(err);
      return false;
    });
}

function applyOptions(gt, options) {
  gametype = gt.toLowerCase();
  strategy = createGameTypeStrategy(gametype);
  resetRating = options.resetRating;
  updateDatabase = options.updateDatabase;
  printResult = options.printResult;
  onlyProcessMatchesBefore = options.onlyProcessMatchesBefore;

  playersBySteamId = {};
  _lastProcessedMatchStartDt = null;
}

function createGameTypeStrategy(gametype) {
  var ValidFactoriesForGametype = {
    "duel": ["duel", "qcon_duel"],
    "ffa": ["ffa", "mg_ffa_classic"],
    "ca": ["ca", "capickup"],
    "tdm": ["ctdm", "qcon_tdm"],
    "ctf": ["ctf", "ctf2", "qcon_ctf"],
    "ft": ["freeze", "cftag", "ft", "ftclassic", "ft_classic", "mg_ft_fullclassic", "vft"]
  }
  var MinRequiredPlayersForGametype = {
    "duel": 2,
    "ffa": 4,
    "ca": 6,
    "tdm": 6,
    "ctf": 6,
    "ft": 6
  }
  var ValidateMatchForGametype = {
    "duel": function(game) { return game.matchStats.GAME_LENGTH >= 10 * 60 - 5 || game.matchStats.EXIT_MSG.indexOf("forfeited") >= 0 },
    "ffa": function(game) { return game.matchStats.FRAG_LIMIT >= 50 },
    "ca": function(game) { return Math.max(game.matchStats.TSCORE0, game.matchStats.TSCORE1) >= 8 /* old JSONS have no ROUND_LIMIT */ },
    "tdm": function(game) { return Math.max(game.matchStats.TSCORE0, game.matchStats.TSCORE1) >= 100 || game.matchStats.GAME_LENGTH >= 15 * 10 },
    "ctf": function(game) { return Math.max(game.matchStats.TSCORE0, game.matchStats.TSCORE1) >= 8 || game.matchStats.GAME_LENGTH >= 15 * 10 },
    "ft": function(game) { return Math.max(game.matchStats.TSCORE0, game.matchStats.TSCORE1) >= 8 /* old JSONS have no ROUND_LIMIT */ }
  }
  
  // a and b are performance scores adjusted for participation time
  var IsDrawForGametype = {
    "duel": function() { return false; },
    "ffa": function(a, b, game) { return Math.abs(a - b) <= 2 * Math.max(1, game.matchStats.FRAG_LIMIT/50) },
    "ca": function(a, b) { return Math.abs(a - b) <= 2 },
    "tdm": function(a, b) { return a / b <= 1.1 && b / a <= 1.1 },
    "ctf": function(a, b) { return a / b <= 1.1 && b / a <= 1.1 },
    "ft": function(a, b) { return Math.abs(a - b) <= 5; }
  }

  return {
    validFactories: ValidFactoriesForGametype[gametype],
    minPlayers: MinRequiredPlayersForGametype[gametype],
    validateGame: ValidateMatchForGametype[gametype],
    maxTeamTimeDiff: 1.05, // in a 4on4 game with 10 mins (or rounds) it will allow up to 42 player minutes in one team
    isDraw: IsDrawForGametype[gametype]
  }
}

function dbConnect() {
  var defConnect = Q.defer();
  pg.connect(_config.webapi.database, function(err, cli, release) {
    if (err)
      defConnect.reject(new Error(err));
    else {
      cli.release = release;
      defConnect.resolve(cli);
    }
  });
  return defConnect.promise;
}

function resetRatingsInDb(cli) {
  if (!resetRating || !updateDatabase)
    return Q();

  var factories = "'" + strategy.validFactories.join("','") + "'";
  return Q()
    .then(function() { return Q.ninvoke(cli, "query", "update player_elos set g2_games=0, g2_r=0, g2_rd=0, g2_dt=null where game_type_cd=$1", [gametype]) })
    .then(function() { return Q.ninvoke(cli, "query", "update player_game_stats pgs set g2_score=null, g2_delta_r=null, g2_delta_rd=null from games g where pgs.game_id=g.game_id and g.game_type_cd=$1", [gametype]) })
    .then(function() { return Q.ninvoke(cli, "query", "update games set g2_status=$2 where game_type_cd=$1 and g2_status<>$3", [gametype, ERR_NOTRATEDYET, ERR_DATAFILEMISSING]) })
    .then(function() { return Q.ninvoke(cli, "query", "update games set g2_status=$2 where game_type_cd=$1 and mod not in (" + factories + ")", [gametype, ERR_FACTORY_OR_SETTINGS]) });
}

function loadPlayers(cli, steamIds) {
  var query = "select h.hashkey, p.player_id, p.nick, pe.g2_r, pe.g2_rd, pe.g2_dt, pe.g2_games "
    + " from hashkeys h"
    + " inner join players p on p.player_id=h.player_id"
    + " left outer join player_elos pe on pe.player_id=h.player_id"
    + " where pe.game_type_cd=$1";
  var params = [gametype];
  
  if (steamIds) {
    query += " and h.hashkey in ('0'";
    steamIds.forEach(function(steamId, i) {
      query += ",$" + (i + 2);
      params.push(steamId);
    });
    query += ")";
  }

  return Q.ninvoke(cli, "query", query, params)
    .then(function(result) {
      _logger.debug("loaded " + result.rows.length + " players");
      result.rows.forEach(function(row) {
        var player = getOrAddPlayer(row.player_id, row.hashkey, row.nick, resetRating ? 0 : row.g2_r, resetRating ? 0 : row.g2_rd, resetRating ? 0 : glickoPeriod(row.g2_dt));
        player.games = row.g2_games;
        player.mustSave = false;
      });
    });
}

function getMatchIds(cli) {
  var cond = resetRating ? "" : " and g2_status=" + ERR_NOTRATEDYET;
  cond += " and (" + (onlyProcessMatchesBefore ? 1 : 0) + "=0 or start_dt<$1)";

  return Q.ninvoke(cli, "query",
      "select match_id, start_dt, game_id from games"
      + " where game_type_cd='" + gametype + "' and mod in ('" + strategy.validFactories.join("','") + "')" + cond
      + " order by start_dt", [onlyProcessMatchesBefore || new Date()])
    .then(function(result) {
      return result.rows.map(function(row) { return { game_id: row.game_id, date: row.start_dt, match_id: row.match_id }; });
    });
}

function reprocessMatches(cli, matches) {
  var currentRatingPeriod = 0;

  var counter = 0;
  var printProgress = true;
  _logger.info("found " + matches.length + " matches");
  var progressLogger = setInterval(function() { printProgress = true; }, 5000);
  return matches.reduce(function(chain, match) {
      return chain.then(function() {
        if (printProgress) {
          _logger.info("processed matches: " + counter + " (" + Math.round(counter * 10000 / matches.length) / 100 + "%)");
          printProgress = false;
        }
        ++counter;
        g2.setPeriod(glickoPeriod(match.date));
        return /* ok || */ reprocessMatch(cli, match.match_id, match.date, match.game_id, currentRatingPeriod);
      });
    }, Q())
    .then(function() { return playersBySteamId; })
    .finally(function() { clearTimeout(progressLogger) });
}

function reprocessMatch(cli, matchId, date, gameId) {
  var deltas = [0, +1, -1];
  var subfolders = [];
  for (var i = 0; i < 3; i++)
    subfolders.push(getDateFolder(date, deltas[i]));
  subfolders.push("omega/");

  var file = null;
  for (var i = 0; i < subfolders.length; i++) {
    try {
      file = __dirname + "/../" + _config.feeder.jsondir + subfolders[i] + matchId + ".json.gz";
      var stat = fs.statSync(file);
      if (stat && stat.isFile())
        break;
    }
    catch (err) {
    }
    file = null;
  }
  
  _lastProcessedMatchStartDt = date;

  if (file) {
    return processFile(cli, gameId, file)
      .catch(function(err) {
        _logger.error("Failed to process " + file + ": " + err);
        return false;
      });
  }

  _logger.warn("json.gz not found: " + matchId);
  return setGameStatus(cli, gameId, ERR_DATAFILEMISSING).then(Q(false));
}

function getDateFolder(date, deltaDays) {
  // match .json.gz files are stored in YYYY-MM/DD/ folders
  var newDate;
  if (deltaDays) {
    newDate = new Date(date.getTime());
    newDate.setDate(newDate.getDate() + deltaDays);
  }
  else
    newDate = date;

  var year = newDate.getUTCFullYear();
  var month = newDate.getUTCMonth();
  var day = newDate.getUTCDate();
  return year + "-" + ("0" + (month + 1)).substr(-2) + "/" + ("0" + day).substr(-2) + "/";
}

function processFile(cli, gameId, file) {
  return Q
    .nfcall(fs.readFile, file)
    .then(function (data) { return file.indexOf(".gz") < 0 ? Q(data) : Q.nfcall(zlib.gunzip, data); })
    .then(function (json) { return processGame(cli, gameId, JSON.parse(json)) });
}

function processGame(cli, gameId, game) {
  var result = extractDataFromGameObject(game);

  // store a status code why the game was not rated
  if (typeof (result) === "number")
    return setGameStatus(cli, gameId, result).then(Q(false));

  var playerRanking = result;
  var players = [];
  for (var i = 0; i < playerRanking.length; i++) {
    var r1 = playerRanking[i];
    var p1 = playersBySteamId[r1.id];
    p1.score = Math.round(r1.score);
    p1.mustSave = true;
    players.push(p1);
    ++p1.games;
    if (r1.win)
      ++p1.wins;

    //if (gameId == 863)
    //  _logger.trace(p1.name + ": r=" + p1.rating.__oldR + ", rd=" + p1.rating.__oldRd + ", g=" + p1.games + ", s=" + p1.score);

    for (var j = i + 1; j < playerRanking.length; j++) {
      var r2 = playerRanking[j];
      var p2 = playersBySteamId[r2.id];
      var result = strategy.isDraw(r1.score, r2.score, game) ? 0.5 : r1.score > r2.score ? 1 : 0;

      g2.addResult(p1.rating, p2.rating, result);
    }
  }

  if (rateEachSingleMatch)
    g2.calculatePlayersRatings();

  return (rateEachSingleMatch ? savePlayerGameRatingChange(players) : Q())
    .then(function() { return setGameStatus(cli, gameId, ERR_OK) })
    .then(Q(true));

  function savePlayerGameRatingChange(players) {
    return players.reduce(function(chain, p) {
      return chain.then(function() {
        return getPlayerId(cli, p)
          .then(function(pid) {
            //if (gameId == 863)
            //  _logger.trace(p.name + ": r=" + p.rating.getRating() + ", rd=" + p.rating.getRd());

            if (!updateDatabase)
              return Q();

            var val = [gameId, pid, p.score, p.rating.getRating() - p.rating.__oldR, p.rating.getRd() - p.rating.__oldRd];
            return Q.ninvoke(cli, "query", { name: "pgs_upd", text: "update player_game_stats set g2_score=$3, g2_delta_r=$4, g2_delta_rd=$5 where game_id=$1 and player_id=$2", values: val })
              .then(function(result) {
                if (result.rowCount == 0)
                  _logger.warn("player_game_stats not found: gid=" + gameId + ", pid=" + pid);
                return Q();
              });
          });
      });
    }, Q());
  }
}

function extractDataFromGameObject(game) {
  if (game.matchStats.ABORTED) return ERR_ABORTED;
  if (!strategy.validateGame(game)) return ERR_ROUND_OR_TIMELIMIT;
  if (game.matchStats.INSTAGIB) return ERR_FACTORY_OR_SETTINGS;
  if (game.matchStats.INFECTED) return ERR_FACTORY_OR_SETTINGS;
  if (game.matchStats.QUADHOG) return ERR_FACTORY_OR_SETTINGS;
  if (game.matchStats.TRAINING) return ERR_FACTORY_OR_SETTINGS;
  if (strategy.validFactories.indexOf(game.matchStats.FACTORY) < 0) return ERR_FACTORY_OR_SETTINGS;

  // aggregate total time, damage and score of player during a match (could have been switching teams)
  var playerData = {}
  var botmatch = false;
  var timeRed = 0, timeBlue = 0, isTeamGame, roundsRed, roundsBlue;
  aggregateTimeAndScorePerPlayer();

  if (botmatch)
    return ERR_BOTMATCH;
  if (game.roundCount) {
    if (roundsBlue != 0 && (roundsRed / roundsBlue > strategy.maxTeamTimeDiff || roundsBlue / roundsRed > strategy.maxTeamTimeDiff))
      return ERR_TEAMTIMEDIFF;    
  }
  else if (timeBlue != 0 && (timeRed/timeBlue > strategy.maxTeamTimeDiff || timeBlue/timeRed > strategy.maxTeamTimeDiff))
    return ERR_TEAMTIMEDIFF;

  var players = calculatePlayerRanking();
  if (players.length < strategy.minPlayers)
    return ERR_MINPLAYERS;
  return players;

  function aggregateTimeAndScorePerPlayer() {
    game.playerStats.forEach(function(p) {
      botmatch |= p.STEAM_ID == "0";
      if (p.WARMUP || botmatch) // p.ABORTED must be counted for team switchers
        return;

      var pd = playerData[p.STEAM_ID];
      if (!pd) {
        pd = { id: p.STEAM_ID, name: p.NAME, timeRed: 0, timeBlue: 0, roundsRed: 0, roundsBlue: 0, score: 0, k: 0, d: 0, dg: 0, dt: 0, a: 0, win: false };
        playerData[p.STEAM_ID] = pd;
      }

      var time = Math.min(p.PLAY_TIME, game.matchStats.GAME_LENGTH); // pauses and whatever QL bugs can cause excessive PLAY_TIME
      if (p.TEAM == 2) {
        timeBlue += time;
        pd.timeBlue += time;
        if (game.roundCount && game.roundCount.players[p.STEAM_ID])
          roundsRed += pd.roundsBlue = game.roundCount.players[p.STEAM_ID].b; // don't aggregate round counts, they already are totals
      }
      else {
        timeRed += time;
        pd.timeRed += time;
        if (game.roundCount && game.roundCount.players[p.STEAM_ID])
          roundsBlue += pd.roundsRed = game.roundCount.players[p.STEAM_ID].r; // don't aggregate round counts, they already are totals
      }
      pd.score += p.SCORE;
      pd.dg += p.DAMAGE.DEALT;
      pd.dt += p.DAMAGE.TAKEN;
      pd.k += p.KILLS;
      pd.d += p.DEATHS;
      pd.a += p.MEDALS.ASSISTS;
      if (p.RANK == 1)
        pd.win = true;
      isTeamGame |= p.hasOwnProperty("TEAM");
    });
  }

  function calculatePlayerRanking() {
    var players = [];
    for (var steamId in playerData) {
      if (!playerData.hasOwnProperty(steamId)) continue;
      var pd = playerData[steamId];

      if (game.roundCount) {
        // min. 50% round participation
        if (pd.roundsRed + pd.roundsBlue < game.roundCount.total / 2)
          continue;
      }
      else if (pd.timeRed + pd.timeBlue < game.matchStats.GAME_LENGTH / 2) {
        // minumum 50% time participation
        continue;
      }
      if (pd.dg < 500 || pd.dt / pd.dg >= 10.0) // skip AFK players
        continue;

      getOrAddPlayer(null, pd.id, pd.name).played = true;

      if (isTeamGame) {
        var winningTeam = game.matchStats.TSCORE0 > game.matchStats.TSCORE1 ? -1 : game.matchStats.TSCORE0 == game.matchStats.TSCORE1 ? 0 : +1;
        var playerTeam = pd.timeRed >= pd.timeBlue ? -1 : +1;
        pd.win = playerTeam == winningTeam;
      }

      var rankingScore = calcPlayerPerformance(pd, game);
      if (rankingScore != NaN)
        players.push({ id: pd.id, score: rankingScore, win: pd.win });
    }
    return players;
  }
}

function getOrAddPlayer(playerId, steamId, name, rating, rd, period) {
  var player = playersBySteamId[steamId];
  if (!player)
    playersBySteamId[steamId] = player = { pid: playerId, id: steamId, name: name, games: 0, wins: 0, rating: g2.makePlayer(rating, rd, period) };
  return player;
}

function getPlayerId(cli, player) {
  if (player.pid)
    return Q(player.pid);
  
  return Q.ninvoke(cli, "query", { name: "player_by_steamid", text: "select player_id from hashkeys where hashkey=$1", values: [player.id] })
    .then(function (result) {
    if (result.rows.length == 0) {
      _logger.warn("no player with steam-id " + player.id);
      return null;
    }
    return player.pid = result.rows[0].player_id;
  });
}

function calcPlayerPerformance(p, raw) {
  var timeFactor = raw.matchStats.GAME_LENGTH / (p.timeRed + p.timeBlue);
  if (raw.roundCount) {
    var pr = raw.roundCount.players[p.id];
    if (!pr) return NaN;
    timeFactor = raw.roundCount.total / (pr.r + pr.b);
  }

  // CTF score formula inspired by http://bot.xurv.org/rating.pdf
  if (gametype == "ctf")
    return (p.dt == 0 ? 2 : Math.min(2, Math.max(0.5, p.dg / p.dt))) * (p.score + p.dg / 20) * timeFactor; // + (p.win ? 300 : 0);

  // TDM performance formula inspired by http://qlstats.info/about-ql-statistics.html
  if (gametype == "tdm")
    return ((p.k - p.d) * 5 + (p.dg - p.dt) / 100 * 4 + p.dg / 100 * 3) * timeFactor;

  if (gametype == "duel")
    return p.score;

  // then use score/rounds for CA
  if (gametype == "ca")
    return (p.dg / 100 * + 0.25*p.k) * timeFactor;

  if (gametype == "ft")
    return (p.dg / 100 + 0.5*(p.k - p.d) + 2*p.a) * timeFactor;

  // FFA, FT: score/time
  return p.score * timeFactor;
}

function glickoPeriod(date) {
  if (!date) return 0;
  if (typeof (date) == "number") return date;
  return Math.floor(date.getTime() / 1000 / 60 / 60 / 24);
}

function glickoDate(period) {
  //return period ? new Date(period * 24 * 60 * 60 * 1000) : null;
  return period;
}

function setGameStatus(cli, gameId, status) {
  if (!updateDatabase)
    return Q();
  return Q.ninvoke(cli, "query", { name: "game_upd", text: "update games set g2_status=$2 where game_id=$1", values: [gameId, status] });
}

function savePlayerRatings(cli) {
  var players = playersBySteamId;
  if (!updateDatabase)
    return Q(players);

  var list = [];
  for (var steamId in players) {
    if (!players.hasOwnProperty(steamId)) continue;
    var player = players[steamId];
    if (player.mustSave)
      list.push(player);
  }

  return list.reduce(function(chain, player) {
      return chain.then(function() {
        var val = [player.pid, gametype, player.games, player.rating.getRating(), player.rating.getRd(), glickoDate(player.rating.getPeriod())];
        // try update and if rowcount is 0, execute an insert
        return Q.ninvoke(cli, "query", { name: "elo_upd", text: "update player_elos set g2_games=$3, g2_r=$4, g2_rd=$5, g2_dt=$6 where player_id=$1 and game_type_cd=$2", values: val })
          .then(function(result) {
            if (result.rowCount == 1) return Q();
            return Q.ninvoke(cli, "query", { name: "elo_ins", text: "insert into player_elos (player_id, game_type_cd, g2_games, g2_r, g2_rd, g2_dt, elo) values ($1,$2,$3,$4,$5,$6, 100)", values: val })
              .catch(function(err) { _logger.error("Failed to insert/update player_elo: steam-id=" + player.id + ", data=" + JSON.stringify(val) + ", name=" + player.name + ":\n" + err); });
          });
      });
    }, Q())
    .then(function() {
      // reprocessing could have taken quite a long time and new matches could have been added with incorrect ratings, so mark the new games as NOT_RATED_YET
      if (resetRating) {
        var val = [gametype, ERR_NOTRATEDYET, ERR_DATAFILEMISSING, _lastProcessedMatchStartDt];
        return Q.ninvoke(cli, "query", "update games set g2_status=$2 where game_type_cd=$1 and g2_status<>$3 and start_dt>$4", val);
      }
      return Q();
    })
    .then(Q(players));
}

function printResults() {
  if (!printResult)
    return playersBySteamId;

  // bring all RD values to today's date  
  //var allRatings = Object.keys(playersBySteamId).map(function (key) { return playersBySteamId[key].rating; });
  //g2.setPeriod(glickoPeriod(new Date()), allRatings);

  var players = [];
  for (var key in playersBySteamId) {
    if (!playersBySteamId.hasOwnProperty(key)) continue;
    var p = playersBySteamId[key];
    if (!p.played) continue;
    p.r1 = p.rating.getRating() - p.rating.getRd();
    players.push(p);
  }
  players.sort(function(a, b) { return -(a.r1 - b.r1); });
  players.forEach(function(p) {
    if (!p || p.games < 5) return;
    console.log(p.name
      + ": r-rd=" + Math.round(p.r1)
      + ", (r=" + Math.round(p.rating.getRating())
      + ", rd=" + Math.round(p.rating.getRd())
      + "), games: " + p.games
      + ", wins: " + Math.round(p.wins * 1000 / p.games) / 10 + "%");
  });

  return playersBySteamId;
}

init();
