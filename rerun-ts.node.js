var
  fs = require("graceful-fs"),
  pg = require("pg"),
  zlib = require("zlib"),
  ts = require("trueskill"),
  //ts = require("com.izaakschroeder.trueskill").create(),
  Q = require("q");

var _config;

function main() {
  _config = JSON.parse(fs.readFileSync(__dirname + "/cfg.json"));

  //Q.longStackSupport = true;

  //ts.SetParameters(8.3333333333/2, null, null, 0);

  dbConnect()
    .then(function (cli) {
      return getMatchIds(cli)
        .then(function (matches) { return processMatches(cli, matches); })
        .then(printResults)
        .finally(function () { cli.release(); });
    })
    .catch(function (err) { console.log(err);
      throw err;
    })
    .finally(function () { pg.end(); })
    .done();
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

function getMatchIds(cli) {
  return Q.ninvoke(cli, "query", "select match_id, start_dt, game_id from games where game_type_cd='ctf' and mod in ('ctf','ctf2','qcon_ctf') order by start_dt")
    .then(function (result) {
      return result.rows.map(function(row) { return { game_id: row["game_id"], date: row["start_dt"], match_id: row["match_id"] }; });
    });
}

function processMatches(cli, matches) {
  return matches.reduce(function (chain, match) {
    return chain.then(function (ok) { return ok || processMatch(cli, match.match_id, match.date, match.game_id); });
  }, Q());
}

function processMatch(cli, matchId, date, gameId) {
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
    return processFile(cli, gameId, file);
      //.catch(function (err) {
      //  console.log("Failed to process " + file + ": " + err);
      //  return false;
      //});
  }

  console.log("json.gz not found: " + matchId);
  return false;
}

function getDateFolder(date, deltaDays) {
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


var playersBySteamId = {};

function processFile(cli, gameId, file) {
  return extractDataFromJsonFile(file)
    .then(function (playerRanking) {
      if (!playerRanking || playerRanking.length == 0)
        return false;

      // FIXME
      console.log("file: " + file);
      playerRanking = playerRanking.slice(0, 8);

      var players = [];
      var ratings = [];
      var ranking = [];
      playerRanking.forEach(function (pr) {
        var player = playersBySteamId[pr.id];
        ++player.matches;
        if (pr.win)
          ++player.wins;

        player.rank = pr.rank;
        players.push(player);

        ratings.push([player.rating]);
        ranking.push(player.rank);
      });

      var sranking = ranking.slice();
      sranking.sort();
      ranking = ranking.map(function(r) { return sranking.indexOf(r) });
      var newRatings = ts.update(ratings, ranking);
      playerRanking.forEach(function(pr, i) {
        var player = playersBySteamId[pr.id];
        player.rating = newRatings[i][0];
      });

      //ts.AdjustPlayers(players);

      return true;
    });
}


function extractDataFromJsonFile(path) {
  return Q
    .nfcall(fs.readFile, path)
    .then(function(data) { return Q.nfcall(zlib.gunzip, data); })
    .then(function(json) {
      var raw = JSON.parse(json);

      if (raw.matchStats.ABORTED
        || raw.matchStats.GAME_TYPE != "CTF"
        //|| ["ctf", "ctf2", "qcon_ctf"].indexOf(raw.matchStats.FACTORY) < 0
        || [ "ctf2" ].indexOf(raw.matchStats.FACTORY) < 0
        || (raw.matchStats.GAME_LENGTH < 15 * 60 && raw.matchStats.TSCORE0 < 8 && raw.matchStats.TSCORE1 < 8))
        return null;

      // aggregate total time, damage and score of player during a match (could have been switching teams)
      var playerData = {}
      var botmatch = false;
      raw.playerStats.forEach(function (p) {
        botmatch |= p.STEAM_ID == "0";
        if (p.ABORTED || p.WARMUP || botmatch)
          return;

        var pd = playerData[p.STEAM_ID];
        if (!pd) {
          pd = { id: p.STEAM_ID, name: p.NAME, timeRed: 0, timeBlue: 0, score: 0, dg: 0, dt: 0 };
          playerData[p.STEAM_ID] = pd;
        }

        var time = Math.max(p.PLAY_TIME, raw.matchStats.GAME_LENGTH);
        if (p.TEAM == 2)
          pd.timeBlue += time;
        else
          pd.timeRed += time;
        pd.score += p.SCORE;
        pd.dg += p.DAMAGE.DEALT;
        pd.dt += p.DAMAGE.TAKEN;
      });

      if (botmatch)
        return null;

      // calculate a rankingScore for each player
      var players = [];
      for (var key in playerData) {
        if (!playerData.hasOwnProperty(key)) continue;
        var p = playerData[key];
        if (p.timeRed + p.timeBlue < raw.matchStats.GAME_LENGTH / 2)
          continue;

        if (!playersBySteamId[p.id])
          playersBySteamId[p.id] = { id: p.id, name: p.name, matches: 0, wins: 0, skill: [25.0, 8.333], rating: ts.createRating() };

        var winningTeam = raw.matchStats.TSCORE0 > raw.matchStats.TSCORE1 ? -1 : raw.matchStats.TSCORE0 == raw.matchStats.TSCORE1 ? 0 : +1;
        var playerTeam = p.timeRed >= p.timeBlue ? -1 : +1;
        var isWinner = playerTeam == winningTeam;
        var rankingScore = (p.dt == 0 ? 2 : Math.min(2, Math.max(0.5, p.dg / p.dt))) * (p.score + p.dg / 20) * raw.matchStats.GAME_LENGTH / (p.timeRed + p.timeBlue) + (isWinner ? 300 : 0);
        players.push({ id: p.id, rank: -rankingScore, win: isWinner }); // lower value in rank means better (doesn't have to be 1(st), 2(nd), 3(rd) ... just the order matters)
      }

      return players.length < 8 ? null : players;
    });
}

function printResults() {
  var players = [];
  for (var key in playersBySteamId) {
    if (!playersBySteamId.hasOwnProperty(key)) continue;
    var p = playersBySteamId[key];
    p.skill[0] = p.rating.mu;
    p.skill[1] = p.rating.sigma;
    p.ts = p.skill[0] - 3 * p.skill[1];
    players.push(p);
  }
  players.sort(function (a, b) { return -(a.ts < b.ts ? -1 : a.ts == b.ts ? 0 : +1); });
  players.forEach(function (p) {
    //if (p.matches >= 10)
      console.log(p.name + ", ts=" + round3(p.ts) + " (mu=" + round3(p.skill[0]) + ", sig=" + round3(p.skill[1]) + "), matches: " + p.matches + ", wins: " + Math.round(p.wins * 1000 / p.matches) / 10 + "%");
  });

  function round3(n) { return Math.round(n * 1000) / 1000; }
}

main();
