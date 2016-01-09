const
  pg = require("pg"),
  fs = require("graceful-fs"),
  log4js = require("log4js"),
  Q = require("q");

var gametypes = [];
var region = 1;
var minGames = 20;

var _config = JSON.parse(fs.readFileSync(__dirname + "/cfg.json"));
var _logger = log4js.getLogger("glicko");

function main() {
  gametypes = parseCommandLine();
  return dbConnect()
    .then(function(cli) {
      // funky way of a for loop to sequentialize promises
      return gametypes.reduce(function(chain, gt) {
        return chain.then(function() { return queryPlayers(cli, gt); })
          .then(function(rows) { runStats(rows, gt); })
          .finally(function() { cli.release(); });
      }, Q());
    })
    .catch(function(err) {
      _logger.error(err);
      throw err;
    })
    .done(function() { process.exit(0); });
}

function parseCommandLine() {
  var args = process.argv.slice(2);

  while (args[0] && args[0][0] == "-") {
    if (args[0] == "-r" && args.length >= 2) {
      region = args[1];
      args = args.slice(1);
    }
    else if (args[0] == "-m" && args.length >= 2) {
      minGames = parseInt(args[1]) || 20;
      args = args.slice(1);
    }
    else {
      _logger.error("Invalid command line option: " + args[0]);
      process.exit(1);
    }
    args = args.slice(1);
  }
  return args;
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

function queryPlayers(cli, gametype) {
  return Q
    .ninvoke(cli, "query", "select g2_r, g2_rd from player_elos pe inner join players p on p.player_id=pe.player_id where game_type_cd=$1 and region=$2 and g2_games>=$3 order by g2_r", [gametype, region, minGames])
    .then(function(result) { return Q(result.rows); });
}

function runStats(rows, gametype) {
  var counts = [];
  var sum = 0, count = 0;
  rows.forEach(function(row) {
    var i = Math.max(0, Math.min(3000, Math.floor(row.g2_r / 100)));
    counts[i] = (counts[i] || 0) + 1;
    sum += row.g2_r;
    ++count;
  });
  var avg = sum / count;

  sum = 0;
  rows.forEach(function(row) {
    var err = row.g2_r - avg;
    sum += err * err;
  });
  var dev = Math.sqrt(1 / (count - 1) * sum);


  console.log(gametype + ">=" + minGames + " games: avg=" + avg + ", dev=" + dev + "\n\n");

  for (var i = 0; i < 30; i++) {
    var c = counts[i] || 0;
    console.log(i + "\t" + c);
  }
}

main();