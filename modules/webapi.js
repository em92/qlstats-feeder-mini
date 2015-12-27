var
  fs = require("graceful-fs"),
  pg = require("pg"),
  log4js = require("log4js"),
  Q = require("q");

exports.init = init;

var _config;
var _logger = log4js.getLogger("webapi");

function init(config, app) {
  _config = config;
  _logger.setLevel(config.webapi.logLevel || "INFO");

  app.get("/api/jsons/:date", function(req, res) {
    Q(listJsons(req))
      .then(function (result) { res.json(result); })
      .catch(function (err) { res.json({ ok: false, msg: "internal error: " + err }); })
      .finally(function () { res.end(); });
  });

  app.get("/api/jsons/:date/:file.json(.gz)?", function (req, res) {
    Q(getJson(req, res))
      .catch(function (err) { res.json({ ok: false, msg: "internal error: " + err }); })
      .finally(function () { res.end(); });
  });

  app.get("/api/jsons", function (req, res) {
    Q(queryJson(req))
      .then(function (obj) { res.json(obj); })
      .catch(function (err) { res.json({ ok: false, msg: "internal error: " + err }); })
      .finally(function () { res.end(); });
  });
}

// to be removed, deprecated by queryJson
function listJsons(req) {
  var ts = Date.parse(req.params.date);
  if (ts == NaN || !ts)
    return { ok: false, msg: "Date must be provided in YYYY-MM-DD format" };
  var date = new Date(ts);
  var dir = __dirname + "/" + _config.feeder.jsondir + "/" + date.getUTCFullYear() + "-" + ("0" + (date.getUTCMonth() + 1)).substr(-2) + "/" + ("0" + date.getUTCDate()).substr(-2);
  return Q
    .nfcall(fs.readdir, dir)
    .then(function (files) { return { ok: true, files: files.map(function (name) { return name.substr(0, name.indexOf(".json")) }) }; })
    .catch(function () { return { ok: false, msg: "File not found" } });
}

function getJson(req, res) {
  var ts = Date.parse(req.params.date);
  if (ts == NaN || !ts)
    return Q(res.json({ ok: false, msg: "Date must be provided in YYYY-MM-DD format" }));

  var date = new Date(ts);
  var dir = __dirname + "/" + _config.feeder.jsondir + "/" + date.getUTCFullYear() + "-" + ("0" + (date.getUTCMonth() + 1)).substr(-2) + "/" + ("0" + date.getUTCDate()).substr(-2) + "/";
  var asGzip = req.path.substr(-3) == ".gz";
  var options = {
    root: dir,
    dotfiles: "deny",
    headers: asGzip ? {} : { "Content-Type": "application/json", "Content-Encoding": "gzip" }
  };
  return Q.ninvoke(res, "sendFile", req.params.file + ".json.gz", options).catch(function () { return res.json({ ok: false, msg: "File not found" })});
}

function queryJson(req) {
  var defConnect = Q.defer();
  pg.connect(_config.webapi.database, function (err, cli, release) {
    if (err)
      defConnect.reject(new Error(err));
    else {
      cli.release = release;
      defConnect.resolve(cli);
    }
  });

  return Q(defConnect.promise).then(function (cli) {

    if (!req.query.server)
      return { ok: false, msg: "'server' query parameter must be specified for the numeric server id" };
    if (!req.query.date)
      return { ok: false, msg: "'date' query parameter must be specified as a UTC timestamp (YYYY-MM-ddTHH:mm:ssZ)" };

    var cond = "";
    var values = [req.query.server, req.query.date];
    // TODO: add optional query criteria

    return Q.ninvoke(cli, "query", { name: "json_query", text: "select start_dt as end, match_id as id from games where server_id=$1 and start_dt>=$2 " + cond + " order by start_dt", values: values })
      .then(function(result) { return result.rows; })
      .finally(function () { cli.release(); });
  });
}