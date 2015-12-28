var
  fs = require("graceful-fs"),
  pg = require("pg"),
    log4js = require("log4js"),
  request = require("request"),
  Q = require("q");

exports.init = init;

var _config;
var _logger = log4js.getLogger("webapi");

// interface for communication with the feeder.node.js module
var _feeder = {
  getStatsConnections: function () { }
};

function init(config, app, feeder) {
  _config = config;
  _feeder = feeder;
  _logger.setLevel(config.webapi.logLevel || "INFO");
  
  app.get("/api/server/statusdump", function (req, res) {
    Q(getServerStatusdump(req))
      .then(function (obj) { res.json(obj); })
      .catch(function (err) { res.json({ ok: false, msg: "internal error: " + err }); })
      .finally(function () { res.end(); });
  });

  if (!_config.webapi.enabled)
    return;

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
    
  app.get("/api/server/skillrating", function (req, res) {
    Q(getServerSkillrating(req))
      .then(function (obj) { res.json(obj); })
      .catch(function (err) { res.json({ ok: false, msg: "internal error: " + err }); })
      .finally(function () { res.end(); });
  });

  app.get("/api/server/:addr/players", function (req, res) {
    Q(getServerPlayers(req))
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
  return dbConnect()
    .then(function(cli) {
      if (!req.query.server)
        return { ok: false, msg: "'server' query parameter must be specified for the numeric server id" };
      if (!req.query.date)
        return { ok: false, msg: "'date' query parameter must be specified as a UTC timestamp (YYYY-MM-ddTHH:mm:ssZ)" };

      var cond = "";
      var values = [req.query.server, req.query.date];
      // TODO: add optional query criteria

      return Q.ninvoke(cli, "query", { name: "json_query", text: "select start_dt as end, match_id as id from games where server_id=$1 and start_dt>=$2 " + cond + " order by start_dt", values: values })
        .then(function(result) { return result.rows; })
        .finally(function() { cli.release(); });
    });
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

function getServerPlayers(req) {
  var addr = req.params.addr;
  if (!addr) return { ok: false, msg: "No server address specified" };
  var conns = _feeder.getStatsConnections();
  var conn = conns[addr];
  if (!conn) return { ok: false, msg: "Server is not being tracked (by this panel instance)" };
  
  var keys = conn.players ? Object.keys(conn.players) : [];
  var players = keys.map(function(steamid) { return { steamid: steamid, team: conn.players[steamid].team }; });
  return { ok: true, players: players };
}

// internal API used to aggregate live server information from multiple feeder instances
function getServerStatusdump(req) {
  if (req && req.connection.remoteAddress.indexOf("127.0.0.") < 0 && req.connection.remoteAddress != "::1")
    return { ok: false, msg: "only internal connections allowed" };

  var info = {};
  var conns = _feeder.getStatsConnections();
  if (!conns) return {};
  var addrs = Object.keys(conns);
  addrs.forEach(function (addr) {
    var conn = conns[addr];
    if (!conn.connected) return;
    info[addr] = { gt: conn.gameType, p: conn.players };
  });
  return info;
}


var _getServerSkillratingCache = { timestamp: 0, data: null };

// get a complete list of all servers from all feeder instances with current game type and min/max/avg player rating
function getServerSkillrating() {
  var now = new Date().getTime();
  if (_getServerSkillratingCache.timestamp + 15000 > now)
    return _getServerSkillratingCache.data;

  var aggregateInfo = {}; //getServerStatusdump();}
  var tasks = [];
  _config.webapi.aggregatePanelPorts.forEach(function(port) {
    //if (port != _config.httpd.port)
      tasks.push(getStatusdumpFromPort(port).catch(function (err) { _logger.error("aggregate from port " + port + ": " + err); }));
  });

  return dbConnect().then(function(cli) {
    return Q.allSettled(tasks)
      .then(function() {
        return Q.ninvoke(cli, "query", { name: "serverskill", text: "select hashkey, game_type_cd, g2_r, g2_rd from hashkeys h inner join player_elos e on e.player_id=h.player_id where e.g2_games>=5" })
          .then(function(result) { return mapSkillInfo(result.rows); })
          .then(function (skillInfo) { return rateServers(skillInfo); })
          .catch(function (err) { _logger.error(err); throw err; })
          .finally(function() { cli.release(); });
      });
  });
  
  // load status dump from a different admin panel port and aggregate the information
  function getStatusdumpFromPort(port) {
    var defer = Q.defer();
    var ok = true;
    var buffer = "";
    request.get("http://127.0.0.1:" + port + "/api/server/statusdump", { timeout: 1000 })
      .on("error", function(err) { defer.reject(err); })
      .on("response", function(response) {
        if (response.statusCode != 200) {
          ok = false;
          defer.reject(new Error("HTTP status code " + response.statusCode));
        }
      })
      .on("data", function(data) { buffer += data; })
      .on("end", function() {
        if (!ok) return;
        var info = JSON.parse(buffer);
        for (var key in info) {
          if (!info.hasOwnProperty(key)) continue;
          aggregateInfo[key] = info[key];
        }
        defer.resolve(info);
      }).end();

    return defer.promise;
  }

  // convert database rows to a dictionary player[steamid][game_type_cd] => number
  function mapSkillInfo(rows) {
    var info = {};
    rows.forEach(function(row) {
      var player = info[row.hashkey];
      if (!player) info[row.hashkey] = player = {};
      player[row.game_type_cd] = row.g2_r - row.g2_rd;
    });
    return info;
  }
  
  // combine information from players on servers with player ratings
  function rateServers(skillInfo) {
    var info = [];
    var addrs = Object.keys(aggregateInfo);
    addrs.forEach(function(addr) {
      var conn = aggregateInfo[addr];
      var gt = conn.gt;
      if (typeof (conn.p) == "undefined" || !gt)
        return;

      var totalRating = 0;
      var maxRating = 0;
      var minRating = 9999;
      var count = 0;
      Object.keys(conn.p).reduce(function(prev, steamid) {
        var player = conn.p[steamid];
        if (player.team < 0 || player.team >= 3) return;
        var playerRating = skillInfo[steamid];
        var rating = playerRating ? playerRating[gt] : null;
        if (!rating) return;
        ++count;
        totalRating += rating;
        maxRating = Math.max(maxRating, rating);
        minRating = Math.min(minRating, rating);
      }, []);
      if (count > 0)
        info.push({ server: addr, gt: gt, min: Math.round(minRating), avg: Math.round(totalRating / count), max: Math.round(maxRating) });
    });

    _getServerSkillratingCache.timestamp = now;
    _getServerSkillratingCache.data = info;
    return info;
  }
}