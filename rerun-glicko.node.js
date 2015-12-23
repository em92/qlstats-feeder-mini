var
  fs = require("graceful-fs"),
  pg = require("pg"),
  zlib = require("zlib"),
  glicko = require("./glicko1"),
  Q = require("q");


// TODO: turn this into command line args
var gametype = "ffa";
var rateEachSingleMatch = true;

var mode = "incremental";

if (mode == "full") {
  var resetRating = true;
  var updateDatabase = true;
  var printResult = false;
  var onlyProcessMatchesBefore = null;
} else if (mode == "part1") {
  // calculate data up to a certain date to test incremental updates
  var resetRating = true;
  var updateDatabase = true;
  var printResult = true;
  var onlyProcessMatchesBefore = new Date(Date.UTC(2015, 12 - 1, 1)); 
} else if (mode == "part2") {
  // incrementally calculate updates for unrated matches
  var resetRating = false;
  var updateDatabase = false;
  var printResult = true;
  var onlyProcessMatchesBefore = null;
} else if (mode == "incremental") {
  // incrementally calculate updates for unrated matches
  var resetRating = false;
  var updateDatabase = true;
  var printResult = false;
  var onlyProcessMatchesBefore = null;
}


// calculate a value for "c" so that an average RD value of 85 changes back to 350 when a player is inactive for 180 rating periods (=days)
var g2 = new glicko.Glicko({ rating: 1500, rd: 350, c: Math.sqrt((Math.pow(350, 2) - Math.pow(82, 2)) / 180) });

var _config;
var strategy;
var playersBySteamId = {};

// values for DB column games.g2_status
const
  ERR_NOTRATED = 0,
  ERR_OK = 1,
  ERR_ABORTED = 2,
  ERR_ROUND_OR_TIMELIMIT = 3,
  ERR_BOTMATCH = 4,
  ERR_TEAMTIMEDIFF = 5,
  ERR_MINPLAYERS = 6;

function main() {
  _config = JSON.parse(fs.readFileSync(__dirname + "/cfg.json"));

  //Q.longStackSupport = true;

  strategy = createGameTypeStrategy(gametype);

  dbConnect()
    .then(function(cli) {
      return resetRatingsInDb(cli)
        .then(function() { return loadPlayers(cli) })
        .then(function() { return getMatchIds(cli); })
        .then(function(matches) { return processMatches(cli, matches); })
        .then(function(results) { return saveResults(cli, results) })
        .then(printResults)
        .finally(function() { cli.release(); });
    })
    .catch(function(err) {
      console.log(err);
      throw err;
    })
    .finally(function() { pg.end(); })
    .done(function() { console.log("-- finished --") });
}

function createGameTypeStrategy(gametype) {
  var ValidFactoriesForGametype = {
    "duel": ["duel", "qcon_duel"],
    "ffa": ["ffa", "mg_ffa_classic"],
    "ca": ["ca", "capickup"],
    "tdm": ["ctdm", "qcon_tdm"],
    "ctf": ["ctf", "ctf2", "qcon_ctf"],
    "ft": ["freeze", "cftag", "ft", "ftclassic", "mg_ft_fullclassic", "vft"]
  }
  var MinRequiredPlayersForGametype = {
    "duel": 2,
    "ffa": 4,
    "ca": 8,
    "tdm": 8,
    "ctf": 8,
    "ft": 8
  }
  var ValidateMatchForGametype = {
    "duel": function (json) { return json.matchStats.GAME_LENGTH >= 10 * 60 },
    "ffa": function (json) { return json.matchStats.FRAG_LIMIT >= 50 },
    "ca": function (json) { return Math.max(json.matchStats.TSCORE0, json.matchStats.TSCORE1) >= 10 /* old JSONS have no ROUND_LIMIT */ },
    "tdm": function (json) { return Math.max(json.matchStats.TSCORE0, json.matchStats.TSCORE1) >= 100 || json.matchStats.GAME_LENGTH >= 15 * 10 },
    "ctf": function (json) { return Math.max(json.matchStats.TSCORE0, json.matchStats.TSCORE1) >= 8 || json.matchStats.GAME_LENGTH >= 15 * 10 },
    "ft": function (json) { return Math.max(json.matchStats.TSCORE0, json.matchStats.TSCORE1) >= 8 /* old JSONS have no ROUND_LIMIT */ }
  }
  var IsDrawForGametype = {
    "duel": function (a, b) { return false; },
    "ffa": function (a, b) { return Math.abs(a - b) <= 5; },
    "ca": function (a, b) { return Math.abs(a - b) <= 2 },
    "tdm": function (a, b) { return a / b <= 1.1 && b / a <= 1.1 },
    "ctf": function (a, b) { return a / b <= 1.1 && b / a <= 1.1 },
    "ft": function (a, b) { return Math.abs(a - b) <= 5; }
  }

  return {
    validFactories: ValidFactoriesForGametype[gametype],
    minPlayers: MinRequiredPlayersForGametype[gametype],
    validateGame: ValidateMatchForGametype[gametype],
    maxTeamTimeDiff: 10 * 60,
    isDraw: IsDrawForGametype[gametype]
  }
}

function dbConnect() {
  var defConnect = Q.defer();
  pg.connect(_config.webapi.database, function (err, cli, release) {
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

  return Q
    .ninvoke(cli, "query", "update player_elos set g2_games=0, g2_r=0, g2_rd=0, g2_dt=null where game_type_cd=$1", [gametype])
    .then(function () { return Q.ninvoke(cli, "query", "update player_game_stats pgs set g2_score=null, g2_delta_r=null, g2_delta_rd=null from games g where pgs.game_id=g.game_id and g.game_type_cd=$1", [gametype]) })
    .then(function() { return Q.ninvoke(cli, "query", "update games set g2_status=0 where game_type_cd=$1", [gametype]) });
}

function loadPlayers(cli) {
  if (resetRating)
    return Q();

  return Q.ninvoke(cli, "query",
      "select h.hashkey, p.player_id, p.nick, pe.g2_r, pe.g2_rd, pe.g2_dt, pe.g2_games "
      + " from hashkeys h"
      + " inner join players p on p.player_id=h.player_id"
      + " left outer join player_elos pe on pe.player_id=h.player_id"
      + " where pe.game_type_cd=$1", [gametype])
    .then(function(result) {
      console.log("loaded " + result.rows.length + " players");
      result.rows.forEach(function(row) {
        var player = getOrAddPlayer(row.player_id, row.hashkey, row.nick, row.g2_r, row.g2_rd, glickoPeriod(row.g2_dt));
        player.games = row.g2_games;
      });
    });
}

function getMatchIds(cli) {
  var cond = resetRating ? "" : " and g2_status=0";
  cond += " and (" + (onlyProcessMatchesBefore ? 1 : 0) + "=0 or start_dt<$1)";
  
  return Q.ninvoke(cli, "query", 
      "select match_id, start_dt, game_id from games"
      + " where game_type_cd='" + gametype + "' and mod in ('" + strategy.validFactories.join("','") + "')" + cond 
      + " order by start_dt", [onlyProcessMatchesBefore || new Date()])
    .then(function(result) {
      return result.rows.map(function(row) { return { game_id: row.game_id, date: row.start_dt, match_id: row.match_id }; });
    });
}

function processMatches(cli, matches) {
  var currentRatingPeriod = 0;

  var counter = 0;
  var printProgress = true;
  console.log("found " + matches.length + " matches");
  var progressLogger = setInterval(function() { printProgress = true; }, 5000);
  return matches.reduce(function(chain, match) {
      return chain.then(function() {
        if (printProgress) {
          console.log("processed matches: " + counter + " (" + Math.round(counter * 10000 / matches.length) / 100 + "%)");
          printProgress = false;
        }
        ++counter;
        g2.setPeriod(glickoPeriod(match.date));
        return /* ok || */ processMatch(cli, match.match_id, match.date, match.game_id, currentRatingPeriod);
      });
    }, Q())
    .then(function() { return playersBySteamId; })
    .finally(function() { clearTimeout(progressLogger) });
}

function processMatch(cli, matchId, date, gameId, ratingPeriod) {
  var deltas = [0, +1, -1];
  var subfolders = [];
  for (var i = 0; i < 3; i++)
    subfolders.push(getDateFolder(date, deltas[i]));
  subfolders.push("omega/");

  var file = null;
  for (var i = 0; i < subfolders.length; i++) {
    try {
      file = __dirname + "/" + _config.feeder.jsondir + subfolders[i] + matchId + ".json.gz";
      var stat = fs.statSync(file);
      if (stat && stat.isFile())
        break;
      file = null;
    } 
    catch (err) {
    }
  }

  if (file) {
    return processFile(cli, gameId, file, ratingPeriod)
      .catch(function (err) {
        console.log("Failed to process " + file + ": " + err);
        return false;
      });
  }

  console.log("json.gz not found: " + matchId);
  return false;
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

function processFile(cli, gameId, file, ratingPeriod) {
  return extractDataFromJson(file)
    .then(function (result) {    
      // store a status code why the game was not rated
      if (typeof (result) === "number")
        return setGameStatus(result).then(Q(false));
             
      var playerRanking = result;
      var players = [];
      for (var i = 0; i < playerRanking.length; i++) {
        var r1 = playerRanking[i];
        var p1 = playersBySteamId[r1.id];
        p1.score = Math.round(r1.score);
        players.push(p1);
        ++p1.games;
        if (r1.win)
          ++p1.wins;
        p1.rating.oldR = p1.rating.getRating();
        p1.rating.oldRd = p1.rating.getRd();

        if (gameId == 863)
          console.log(p1.name + ": r=" + p1.rating.oldR + ", rd=" + p1.rating.oldRd + ", g=" + p1.games + ", s=" + p1.score);

        for (var j = i + 1; j < playerRanking.length; j++) {
          var r2 = playerRanking[j];
          var p2 = playersBySteamId[r2.id];
          var result = strategy.isDraw(r1.score, r2.score) ? 0.5 : r1.score > r2.score ? 1 : 0;

          g2.addResult(p1.rating, p2.rating, result);
        }
      }
    
      if (rateEachSingleMatch)
        g2.calculatePlayersRatings(ratingPeriod);

      return (rateEachSingleMatch ? savePlayerGameRatingChange(players) : Q())
        .then(function() { return setGameStatus(ERR_OK) })
        .then(Q(true));
    });

  function savePlayerGameRatingChange(players) {
    return players.reduce(function (chain, p) {
      return chain.then(function () {
        return getPlayerId(cli, p)
          .then(function (pid) {
            if (gameId == 863)
              console.log(p.name + ": r=" + p.rating.getRating() + ", rd=" + p.rating.getRd());

            if (!updateDatabase)
              return Q();

            var val = [gameId, pid, p.score, p.rating.getRating() - p.rating.oldR, p.rating.getRd() - p.rating.oldRd];
            return Q.ninvoke(cli, "query", { name: "pgs_upd", text: "update player_game_stats set g2_score=$3, g2_delta_r=$4, g2_delta_rd=$5 where game_id=$1 and player_id=$2", values: val })
              .then(function (result) {
                if (result.rowCount == 0)
                  console.log("player_game_stats not found: gid=" + gameId + ", pid=" + pid);
                return Q();
              });
          });
      });
    }, Q());
  }

  function setGameStatus(status) {
    if (!updateDatabase)
      return Q();
    return Q.ninvoke(cli, "query", { name: "game_upd", text: "update games set g2_status=$2 where game_id=$1", values: [gameId, status] });
  }

}

function extractDataFromJson(path) {
  return Q
    .nfcall(fs.readFile, path)
    .then(function(data) { return Q.nfcall(zlib.gunzip, data); })
    .then(function(json) {
      var raw = JSON.parse(json);

      if (raw.matchStats.ABORTED) return ERR_ABORTED;
      if (!strategy.validateGame(raw)) return ERR_ROUND_OR_TIMELIMIT;

      // aggregate total time, damage and score of player during a match (could have been switching teams)
      var playerData = {}
      var botmatch = false;
      var timeRed = 0, timeBlue = 0, isTeamGame;
      aggregateTimeAndScorePerPlayer();

      if (botmatch)
        return ERR_BOTMATCH;
      if (timeBlue != 0 && Math.abs(timeRed - timeBlue) > strategy.maxTeamTimeDiff)
        return ERR_TEAMTIMEDIFF;

      var players = calculatePlayerRanking();
      if (players.length < strategy.minPlayers)
        return ERR_MINPLAYERS;
      return players;

      function aggregateTimeAndScorePerPlayer() {
        raw.playerStats.forEach(function(p) {
          botmatch |= p.STEAM_ID == "0";
          if (p.WARMUP || botmatch) // p.ABORTED must be counted for team switchers
            return;

          var pd = playerData[p.STEAM_ID];
          if (!pd) {
            pd = { id: p.STEAM_ID, name: p.NAME, timeRed: 0, timeBlue: 0, score: 0, k: 0, d: 0, dg: 0, dt: 0, win: false };
            playerData[p.STEAM_ID] = pd;
          }

          var time = Math.min(p.PLAY_TIME, raw.matchStats.GAME_LENGTH); // pauses and whatever QL bugs can cause excessive PLAY_TIME
          if (p.TEAM == 2) {
            timeBlue += time;
            pd.timeBlue += time;
          } else {
            timeRed += time;
            pd.timeRed += time;
          }
          pd.score += p.SCORE;
          pd.dg += p.DAMAGE.DEALT;
          pd.dt += p.DAMAGE.TAKEN;
          pd.k += p.KILLS;
          pd.d += p.DEATHS;
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
          if (pd.timeRed + pd.timeBlue < raw.matchStats.GAME_LENGTH / 2) // minumum 50% participation
            continue;
          if (pd.dg < 500 || pd.dt / pd.dg >= 10.0) // skip AFK players
            continue;

          getOrAddPlayer(null, pd.id, pd.name).played = true;

          if (isTeamGame) {
            var winningTeam = raw.matchStats.TSCORE0 > raw.matchStats.TSCORE1 ? -1 : raw.matchStats.TSCORE0 == raw.matchStats.TSCORE1 ? 0 : +1;
            var playerTeam = pd.timeRed >= pd.timeBlue ? -1 : +1;
            pd.win = playerTeam == winningTeam;
          }

          var rankingScore = calcPlayerPerformance(pd, raw);
          players.push({ id: pd.id, score: rankingScore, win: pd.win });
        }
        return players;
      }
    });
}

function getOrAddPlayer(playerId, steamId, name, rating, rd, period) {
  var player = playersBySteamId[steamId];
  if (!player)
    playersBySteamId[steamId] = player = { pid: playerId, id: steamId, name: name, games: 0, wins: 0, rating: g2.makePlayer(rating, rd, period) };
  return player;
}

function calcPlayerPerformance(p, raw) {
  var timeFactor = raw.matchStats.GAME_LENGTH / (p.timeRed + p.timeBlue);

  // CTF score formula inspired by http://bot.xurv.org/rating.pdf
  if (gametype == "ctf")
    return (p.dt == 0 ? 2 : Math.min(2, Math.max(0.5, p.dg / p.dt))) * (p.score + p.dg / 20) * timeFactor + (p.win ? 300 : 0);

  // TDM performance formula inspired by http://qlstats.info/about-ql-statistics.html
  if (gametype == "tdm") 
    return ((p.k - p.d) * 5 + (p.dg - p.dt) / 100 * 4 + p.dg / 100 * 3) * timeFactor;

  if (gametype == "duel")
    return p.score;

  // TODO: derive number of rounds a player played from the ZMQ events and add it to the player results
  // then use score/rounds for CA


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

function saveResults(cli, players) {
  if (!updateDatabase)
    return Q(players);

  var list = [];
  for (var steamId in players) {
    if (!players.hasOwnProperty(steamId)) continue;
    var player = players[steamId];
    if (player.games)
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
              .catch(function(err) { console.log("Failed to insert/update player_elo: steam-id=" + player.id + ", data=" + JSON.stringify(val) + ", name=" + player.name + ":\n" + err); });
          });
      });
    }, Q())
    .then(Q(players));
}

function getPlayerId(cli, player) {
  if (player.pid)
    return Q(player.pid);

  return Q.ninvoke(cli, "query", { name: "player_by_steamid", text: "select player_id from hashkeys where hashkey=$1", values: [player.id] })
    .then(function (result) {
      if (result.rows.length == 0) {
        console.log("no player with steam-id " + player.id);
        return null;
      }
      return player.pid = result.rows[0].player_id;
    });
}

function printResults() {
  if (!printResult)
    return playersBySteamId;
  
  // bring all RD values to today's date  
  var allRatings = Object.keys(playersBySteamId).map(function (key) { return playersBySteamId[key].rating; });
  g2.setPeriod(glickoPeriod(new Date()), allRatings);

  var players = [];
  for (var key in playersBySteamId) {
    if (!playersBySteamId.hasOwnProperty(key)) continue;
    var p = playersBySteamId[key];
    if (!p.played) continue;
    p.r1 = p.rating.getRating() - p.rating.getRd();
    players.push(p);
  }
  players.sort(function (a, b) { return -(a.r1 - b.r1); });
  players.forEach(function (p) {
    if (p.games < 5) return;
    console.log(p.name
      + ": r-rd=" + Math.round(p.r1)
      + ", (r=" + Math.round(p.rating.getRating())
      + ", rd=" + Math.round(p.rating.getRd())
      + "), games: " + p.games
      + ", wins: " + Math.round(p.wins * 1000 / p.games) / 10 + "%");
  });

  return playersBySteamId;
}

main();
