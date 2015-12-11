var
  fs = require("graceful-fs"),
  pg = require("pg"),
  zlib = require("zlib"),
  ts = require("trueskill"),
  Q = require("q");

var _config;

/*
function dbConnect() {
  var defConnect = Q.defer();
  pg.connect(_config.webadmin.database, function (err, cli, release) {
    if (err)
      defConnect.reject(new Error(err));
    else {
      cli.release = release;
      defConnect.resolve(cli);
    }
  });
  return defConnect.promise;
}

function getCtfMatchIds(cli) {
  return Q
    .ninvoke(cli, "query", "select game_id from games g inner join servers s on s.server_id=g.server_id where s.name like '#omega%' and game_type_cd='ctf' order by game_id")
    .then(function(result) {
      return result.rows.map(function(row) { return row.game_id; });
    });
}

function processMatches(cli, matches) {
  var chain = Q();
  for (var matchid in matches) {
    if (!matches.hasOwnProperty(matchid)) continue;
    var match = matches[matchid];
    chain = (function(chain, a, b, c) { return chain.then(function() { return processMatch(cli, a, b, c); }); })(chain, matchid, match[0], match[1]);
  }
  return chain;
}

function processMatch(cli, matchId, date, gameId) {
  var deltas = [0, +1, -1];
  var data = null;
  for (var i = 0; i < 3; i++) {
    try {
      var file = __dirname + "/" + _config.feeder.jsondir + getDateFolder(date, deltas[i]) + matchId + ".json.gz";
      data = fs.readFileSync(file);
      break;
    } 
    catch (err) {
    }
  }

  if (data)
     return Q(processFile(cli, matchId, gameId, data));

  console.log("json.gz not found: " + matchId);
  return Q(false);
}

function processFile(cli, matchId, gameId, data) {
  return Q
    .nfcall(zlib.gunzip, data)
    .then(function (json) {
      var stats = JSON.parse(json);
      console.log("found: " + matchId + ", game_id=" + gameId);
      
      return stats.playerStats.reduce(function (chain, p) {
        var values = [gameId, p.STEAM_ID, p.MEDALS.DEFENDS, p.DAMAGE.DEALT, p.DAMAGE.TAKEN, p.PLAY_TIME];
        return chain.then(function () {
          return Q.ninvoke(cli, "query", {
            name: "updplayer",
            text: "update player_game_stats pg set drops=$3 from hashkeys h where pg.game_id=$1 and pg.player_id=h.player_id and h.hashkey=$2 and pushes=$4 and destroys=$5 and alivetime=$6;",
            values: values
          });
        });
      }, Q());      
    });
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
*/


var playersBySteamId = {};

function extractJsonData() {
  var basedir = __dirname + "/" + _config.feeder.jsondir + "omega/";
  return Q
    .nfcall(fs.readdir, basedir)
    .then(function(files) {
      var proms = files.map(function(file) { return extractDataFromJson(basedir + file); });
      return Q.all(proms);
    })
    .then(function(matchRankings) {
      // sort matches by time and extract the player results
      matchRankings.sort(function (a, b) { return a.date < b.date ? -1 : a.date == b.date ? 0 : +1 });
      matchRankings = matchRankings.map(function(m) { return m.players });

      matchRankings.forEach(function (playerRankings) {
        var players = [];
        playerRankings.forEach(function (pr) {
          var player = playersBySteamId[pr.id];
          player.rank = pr.rank;
          players.push(player);
        });
        ts.AdjustPlayers(players);
      });


      var players = [];
      for (var key in playersBySteamId) {
        if (!playersBySteamId.hasOwnProperty(key)) continue;
        var p = playersBySteamId[key];
        p.ts = p.skill[0] - 3 * p.skill[1];
        players.push(p);
      }
      players.sort(function (a, b) { return -(a.ts < b.ts ? -1 : a.ts == b.ts ? 0 : +1); });
      players.forEach(function (p) {
        if (p.matches >= 10)
          console.log(p.name + ", ts=" + Math.round(p.ts, 3) + " (mu=" + Math.round(p.skill[0], 3) + ", sig=" + Math.round(p.skill[1], 3) + "), matches: " + p.matches + ", wins: " + Math.round(p.wins*1000/p.matches)/10 + "%");
      });
      return playersBySteamId;
    });
}


function extractDataFromJson(path) {
  return Q
    .nfcall(fs.readFile, path)
    .then(function(data) { return Q.nfcall(zlib.gunzip, data); })
    .then(function(json) {
      var raw = JSON.parse(json);
      if (raw.matchStats.ABORTED || raw.matchStats.GAME_TYPE != "CTF")
        return { date: 0, players: [] };

      // aggregate total time, damage and score of player during a match (could have been switching teams)
      var playerData = {}
      raw.playerStats.forEach(function (p) {
        if (p.ABORTED || p.WARMUP)
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

      // calculate a rankingScore for each player and order the list by it
      var players = [];
      for (var key in playerData) {
        if (!playerData.hasOwnProperty(key)) continue;
        var p = playerData[key];
        if (p.timeRed + p.timeBlue < raw.matchStats.GAME_LENGTH / 2)
          continue;

        if (playersBySteamId[p.id])
          playersBySteamId[p.id].matches++;
        else
          playersBySteamId[p.id] = { id: p.id, name: p.name, matches: 1, wins: 0, skill: [25.0, 25.0 / 3.0] };

        var winningTeam = raw.matchStats.TSCORE0 > raw.matchStats.TSCORE1 ? -1 : raw.matchStats.TSCORE0 == raw.matchStats.TSCORE1 ? 0 : +1;
        var playerTeam = p.timeRed >= p.timeBlue ? -1 : +1;
        var isWinner = playerTeam == winningTeam;
        if (isWinner)
          playersBySteamId[p.id].wins++;
        var rankingScore = (p.dt == 0 ? 2 : Math.min(2, Math.max(0.5, p.dg / p.dt))) * (p.score + p.dg / 20) * raw.matchStats.GAME_LENGTH / (p.timeRed + p.timeBlue) + (isWinner ? 300 : 0);
        players.push({ id: p.id, rank: -rankingScore }); // lower value in rank means better (doesn't have to be 1(st), 2(nd), 3(rd) ... just the order matters)
      }
      return { date: raw.gameEndTimestamp, players: players };
    });
}

function main() {
  var oldRound = Math.round;
  Math.round = function (n, d) { return d ? oldRound(n * 1000) / 1000 : oldRound(n); }

  _config = JSON.parse(fs.readFileSync(__dirname + "/cfg.json"));

  //Q.longStackSupport = true;
  extractJsonData()
    .done();

  /*
  dbConnect()
  .then(function (cli) {
    return getCtfMatchIds(cli)
      .then(function (matches) { return processMatches(cli, matches); })
      .finally(function () { cli.release(); });
  })
  .catch(function (err) { console.log(err); })
  .done();

  pg.end();
  */
}

main();
