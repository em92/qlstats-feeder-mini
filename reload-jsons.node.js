var
  fs = require("graceful-fs"),
  pg = require("pg"),
  zlib = require("zlib"),
  Q = require("q");

var _config;


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
  return Q.ninvoke(cli, "query", "select match_id, start_dt, game_id from games where game_type_cd<>'ctf' ")
    .then(function(result) {
      var matches = {};
      result.rows.forEach(function(row) {
        matches[row["match_id"]] = [row["start_dt"], parseInt(row["game_id"])];
      });
      return matches;
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
     
      var values = [gameId, stats.matchStats.FACTORY];
      return Q.ninvoke(cli, "query", {
        name: "updmatch",
        text: "update games set mod=$2 where game_id=$1;",
        values: values
      });
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


function main() {
  _config = JSON.parse(fs.readFileSync(__dirname + "/cfg.json"));

  dbConnect()
  .then(function (cli) {
    return getMatchIds(cli)
      .then(function (matches) { return processMatches(cli, matches); })
      .finally(function () { cli.release(); });
  })
  .catch(function (err) { console.log(err); })
  .done();

  pg.end();
}

main();
