var 
  pg = require("pg"),
  Q = require("q");

exports.dbConnect = dbConnect;

// connect to the DB
function dbConnect(conn) {
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