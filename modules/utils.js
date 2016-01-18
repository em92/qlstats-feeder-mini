var 
  pg = require("pg"),
  Q = require("q");

exports.dbConnect = dbConnect;

var initialized = false;

// connect to the DB
function dbConnect(conn) {
  // this hack prevents node-postgres from applying the local time offset to timestaps that are already in UTC 
  // see: https://github.com/brianc/node-postgres/issues/429#issuecomment-24870258
  if (!initialized) {
    pg.types.setTypeParser(1114, function (stringValue) {
      return new Date(Date.parse(stringValue + "+0000"));
    });
    initialized = true;
  }

  var defConnect = Q.defer();
  pg.connect(conn, function (err, cli, release) {
    if (err)
      defConnect.reject(new Error(err));
    else {
      cli.release = release;
      defConnect.resolve(cli);
    }
  });
  return defConnect.promise;
}