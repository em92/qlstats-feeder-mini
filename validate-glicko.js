const
  pg = require("pg"),
  fs = require("graceful-fs"),
  log4js = require("log4js"),
  Q = require("q");

var gametype = "ctf";
var _config = JSON.parse(fs.readFileSync(__dirname + "/cfg.json"));

function main() {
  return dbConnect()
    .then(function(cli) {
      return queryPlayers(cli)
        .then(function(rows) { runStats(rows); })
        .finally(function() { cli.release(); });
    })
    .catch(function(err) {
      _logger.error(err);
      throw err;
    })
    .done(function() { process.exit(0); });
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

function queryPlayers(cli) {
  return Q
    .ninvoke(cli, "query", "select g2_r, g2_rd from player_elos where game_type_cd=$1 order by g2_r", [gametype])
    .then(function(result) { return Q(result.rows); });
}

function runStats(rows) {
  var counts = [];
  rows.forEach(function(row) {
    var i = Math.max(0, Math.min(3000, Math.floor(row.g2_r / 10)));
    counts[i] = (counts[i] || 0) + 1;
  });

  for (var i = 0; i < 300; i++) {
    var c = counts[i] || 0;
    console.log(i + "\t" + c);
  }
}

main();