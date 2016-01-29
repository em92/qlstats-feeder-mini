var 
  fs = require("graceful-fs"), 
  pg = require("pg"),
  Q = require("q"),
  util = require("./modules/utils")

var _config;

function main() {
  _config = JSON.parse(fs.readFileSync(__dirname + "/cfg.json"));

  var maxId = 0;
  var ids = process.argv.slice(2).map(function(id) {
    id = parseInt(id);
    maxId = Math.max(id, maxId);
    return id;
  });
  
  if (ids.length < 2) {
    console.log("usage: merge-servers <server-id>, <server-id>, ...");
    process.exit(1);
  }

  utils.dbConnect(_config.webapi.database)
    .then(function(cli) {
      return updateServers(cli, ids, maxId)
        .finally(function() { cli.release(); });
    })
    .catch(function(err) {
      console.log(err);
      throw err;
    })
    .finally(function() { pg.end(); })
    .done();
}

function updateServers(cli, ids, maxId) {
  return ids.reduce(function(chain, id) {
      if (id == maxId)
        return chain;
      return chain
        .then(function() { return Q.ninvoke(cli, "query", "update games set server_id=$1 where server_id=$2", [maxId, id]); })
        .then(function() { return Q.ninvoke(cli, "query", "update servers set active_ind=false, hashkey='' where server_id=$1", [id]); });
    }, Q())
}

main();