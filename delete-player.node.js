var
  fs = require("graceful-fs"),
  pg = require("pg"),
  Q = require("q"),
  utils = require("./modules/utils"),
  webui = require("./modules/webui");

var _config;

function main() {
  _config = JSON.parse(fs.readFileSync(__dirname + "/cfg.json"));

  if (process.argv.length < 2) {
    console.log("usage: delete-player <player-id>");
    process.exit(1);
  }

  //var steamId = parseInt(process.argv[2]);
  var steamId = "76561198063793800";
  

  utils.dbConnect(_config.webapi.database)
    .then(function(cli) {
      return webui.deletePlayerBySteamId(cli, steamId)
        .finally(function() { cli.release(); });
    })
    .catch(function(err) {
      console.log(err);
      throw err;
    })
    .finally(function () { pg.end(); })
    .done(function () { process.exit(0) });
}

main();