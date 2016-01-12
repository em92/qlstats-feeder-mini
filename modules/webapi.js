const
  fs = require("graceful-fs"),
  pg = require("pg"),
  log4js = require("log4js"),
  request = require("request"),
  gsq = require("game-server-query"),
  Q = require("q");

exports.init = init;

var DEBUG = false;
var _config;
var _logger = log4js.getLogger("webapi");

const GameTypes = { 0: "FFA", 1: "Duel", 2: "Race", 3: "TDM", 4: "CA", 5: "CTF", 6: "1Flag", 8: "Harv", 9: "FT", 10: "Dom", 11: "A&D", 12: "RR" };

// interface for communication with the feeder.node.js module
var _feeder = {
  getStatsConnections: function() {}
};

function init(config, app, feeder) {
  _config = config;
  _feeder = feeder;
  _logger.setLevel(config.webapi.logLevel || "INFO");

  // Internal API methods

  app.get("/api/server/statusdump", function(req, res) {
    Q(getServerStatusdump(req))
      .then(function(obj) { res.json(obj); })
      .catch(function(err) { res.json({ ok: false, msg: "internal error: " + err }); })
      .finally(function() { res.end(); });
  });

  if (!_config.webapi.enabled)
    return;

  // Public API methods

  app.get("/api/jsons/:date", function(req, res) {
    Q(listJsons(req))
      .then(function(result) { res.json(result); })
      .catch(function(err) { res.json({ ok: false, msg: "internal error: " + err }); })
      .finally(function() { res.end(); });
  });

  app.get("/api/jsons/:date/:file.json(.gz)?", function(req, res) {
    Q(getJson(req, res))
      .catch(function(err) { res.json({ ok: false, msg: "internal error: " + err }); })
      .finally(function() { res.end(); });
  });

  app.get("/api/jsons", function(req, res) {
    Q(queryJson(req))
      .then(function(obj) { res.json(obj); })
      .catch(function(err) { res.json({ ok: false, msg: "internal error: " + err }); })
      .finally(function() { res.end(); });
  });

  app.get("/api/server/skillrating", function(req, res) {
    Q(getServerSkillrating(req))
      .then(function(obj) { res.json(obj); })
      .catch(function(err) { res.json({ ok: false, msg: "internal error: " + err }); })
      .finally(function() { res.end(); });
  });

  app.get("/api/server/:addr/query", function(req, res) {
    Q(runServerBrowserQuery(req))
      .then(function(obj) { res.json(obj); })
      .catch(function(err) { res.json({ ok: false, msg: "internal error: " + err }); })
      .finally(function() { res.end(); });
  });

  app.get("/api/server/:addr/players", function(req, res) {
    Q(getServerPlayers(req, res))
      .then(function(obj) { res.json(obj); })
      .catch(function(err) { res.json({ ok: false, msg: "internal error: " + err }); })
      .finally(function() { res.end(); });
  });

}

// to be removed, deprecated by queryJson
function listJsons(req) {
  var ts = Date.parse(req.params.date);
  if (ts == NaN || !ts)
    return { ok: false, msg: "Date must be provided in YYYY-MM-DD format" };
  var date = new Date(ts);
  var dir = __dirname + "/../" + _config.feeder.jsondir + "/" + date.getUTCFullYear() + "-" + ("0" + (date.getUTCMonth() + 1)).substr(-2) + "/" + ("0" + date.getUTCDate()).substr(-2);
  return Q
    .nfcall(fs.readdir, dir)
    .then(function(files) { return { ok: true, files: files.map(function(name) { return name.substr(0, name.indexOf(".json")) }) }; })
    .catch(function() { return { ok: false, msg: "File not found" } });
}

function getJson(req, res) {
  var ts = Date.parse(req.params.date);
  if (ts == NaN || !ts)
    return Q(res.json({ ok: false, msg: "Date must be provided in YYYY-MM-DD format" }));

  var date = new Date(ts);
  var dir = __dirname + "/../" + _config.feeder.jsondir + "/" + date.getUTCFullYear() + "-" + ("0" + (date.getUTCMonth() + 1)).substr(-2) + "/" + ("0" + date.getUTCDate()).substr(-2) + "/";
  var asGzip = req.path.substr(-3) == ".gz";
  var options = {
    root: dir,
    dotfiles: "deny",
    headers: asGzip ? {} : { "Content-Type": "application/json", "Content-Encoding": "gzip" }
  };
  return Q.ninvoke(res, "sendFile", req.params.file + ".json.gz", options).catch(function() { return res.json({ ok: false, msg: "File not found" }) });
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

function getServerPlayers(req, res) {
  res.set("Access-Control-Allow-Origin", "*");
  var addr = req.params.addr;
  if (!addr) return { ok: false, msg: "No server address specified" };

  return Q.all([getAggregatedServerStatusData(), getSkillRatings()])
    .then(function (info) {
      var serverStatus = info[0];
      var ratings = info[1];
      var status = serverStatus[addr];
      if (!status) return { ok: false, msg: "Server is not being tracked" };

      var gt = status.gt || _getServerGametypeCache[addr];
      var keys = status.p ? Object.keys(status.p) : [];
      var players = keys.reduce(function(result, steamid) {
        var player = status.p[steamid];
        if (!player.quit) {
          var rating = gt && ratings && ratings[steamid] ? ratings[steamid][gt] : undefined;
          result.push({ steamid: steamid, name: player.name, team: player.team, rating: rating, time: player.time });
        }
        return result;
      }, []);
      return { ok: true, players: players, serverinfo: getServerInfo(addr, status, gt, ratings) };
    });
}


// internal API used to aggregate live server information from multiple feeder instances
function getServerStatusdump(req) {
  if (!isInternalRequest(req))
    return { ok: false, msg: "only internal connections allowed" };

  var info = {};
  var conns = _feeder.getStatsConnections();
  var addrs = Object.keys(conns);
  addrs.forEach(function(addr) {
    var conn = conns[addr];
    if (!conn.connected) return;
    info[addr] = { gt: conn.gameType, f: conn.factory, p: conn.players };
  });
  return info;
}


// get a complete list of all servers from all feeder instances with current game type and min/max/avg player rating
var _getServerSkillratingCache = { timestamp: 0, data: null, updatePromise: null };
var _getServerGametypeCache = {};

function getServerSkillrating() {
  var now = new Date().getTime();
  if (_getServerSkillratingCache.timestamp + 15000 > now)
    return _getServerSkillratingCache.data;
  if (_getServerSkillratingCache.updatePromise != null)
    return _getServerSkillratingCache.updatePromise;

  return _getServerSkillratingCache.updatePromise = Q
    .all([getAggregatedServerStatusData(), getSkillRatings()])
    .then(function(results) { return rateServers(results[0], results[1]); })
    .catch(function(err) {
      _logger.error(err);
      throw err;
    })
    .finally(function() { _getServerSkillratingCache.updatePromise = null; });

  // combine information from players on servers with player ratings
  function rateServers(serverStatus, skillInfo) {
    var info = [];
    var addrs = Object.keys(serverStatus);
    var delay = 0;
    addrs.forEach(function(addr) {
      var conn = serverStatus[addr];
      var gt = conn.gt || _getServerGametypeCache[addr];
      if (!gt) {
        // execute browser query in the background and put the result in the cache for the next call to this API
        Q.delay(delay += 100).then(function() {
          runServerBrowserQueryInternal(addr).then(function(result) {
            if (!result.state.error && result.state.raw && result.state.raw.rules) {
              gt = (GameTypes[parseInt(result.state.raw.rules.g_gametype)] || "").toLowerCase();
              _getServerGametypeCache[addr] = gt;
            }
          });
        }).catch();
        return;
      }

      info.push(getServerInfo(addr, conn, gt, skillInfo));
    });

    _getServerSkillratingCache.timestamp = now;
    _getServerSkillratingCache.data = info;
    return info;
  }
}

function getServerInfo(addr, serverStatus, gt, skillInfo) {
  var totalRating = 0;
  var maxRating = 0;
  var minRating = 9999;
  var count = 0;
  var playerCount = 0, botCount = 0, specCount = 0;
  Object.keys(serverStatus.p).reduce(function (prev, steamid) {
    var player = serverStatus.p[steamid];
    if (player.quit)
      return;
    if (steamid == 0)
      ++botCount;
    else if (player.team == -1 || player.team >= 3) {
      ++specCount;
      return;
    }
    else
      ++playerCount;
    var playerRating = skillInfo[steamid];
    var rating = playerRating ? playerRating[gt] : null;
    if (!rating) return;
    ++count;
    totalRating += rating;
    maxRating = Math.max(maxRating, rating);
    minRating = Math.min(minRating, rating);
  }, []);
  return { server: addr, gt: gt, min: count == 0 ? 0 : Math.round(minRating), avg: count == 0 ? 0 : Math.round(totalRating / count), max: Math.round(maxRating), pc: playerCount, sc: specCount, bc: botCount };  
}

function runServerBrowserQuery(req) {
  if (!isInternalRequest(req))
    return { ok: false, msg: "only internal connections allowed" };

  var addr = req.params.addr;
  if (!addr) return { ok: false, msg: "No server address specified" };
  return runServerBrowserQueryInternal(addr);
}


// helper functions

var _getSkillRatingsCache = { timestamp: 0, data: {}, updatePromise: null }
var _getAggregatedStatusDataCache = { timestamp: 0, data: {}, updatePromise: null }

function isInternalRequest(req) {
  return !(req && req.connection.remoteAddress.indexOf("127.0.0.") < 0 && req.connection.remoteAddress != "::1");
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

function runServerBrowserQueryInternal(addr) {
  var parts = addr.split(":");
  var host = parts[0];
  var port = (parts[1] ? parseInt(parts[1]) : 0) || 27960;

  var def = Q.defer();
  gsq({ type: "synergy", host: host, port: port }, function(state) { def.resolve(state); });
  return def.promise
    .then(function(state) {
      return { ok: true, state: state };
    });
}

function getSkillRatings() {
  if (_getSkillRatingsCache.timestamp + 60 * 1000 > new Date().getTime())
    return Q(_getSkillRatingsCache.data);
  if (_getSkillRatingsCache.updatePromise !== null)
    return _getSkillRatingsCache.updatePromise;

  return _getSkillRatingsCache.updatePromise = dbConnect()
    .then(function(cli) {
      return Q
        .ninvoke(cli, "query", { name: "serverskill", text: "select hashkey, game_type_cd, g2_r, g2_rd from hashkeys h inner join player_elos e on e.player_id=h.player_id where e.g2_games>=5" })
        .then(function(result) {
          _getSkillRatingsCache.data = mapSkillInfo(result.rows);
          _getSkillRatingsCache.timestamp = new Date().getTime();
          return _getSkillRatingsCache.data;
        })
        .finally(function() {
          cli.release();
          _getSkillRatingsCache.updatePromise = null;
        });
    });

  function mapSkillInfo(rows) {
    var info = {};
    rows.forEach(function(row) {
      var player = info[row.hashkey];
      if (!player)
        info[row.hashkey] = player = {};
      player[row.game_type_cd] = Math.round(row.g2_r);
    });
    return info;
  }
}

function getAggregatedServerStatusData() {
  // bypass cache for single instance feeder/webadmin/webapi process
  if (_config.webapi.aggregatePanelPorts.length == 0 || _config.webapi.aggregatePanelPorts.length == 1 && _config.webapi.aggregatePanelPorts[0] == _config.httpd.port)
    return getServerStatusdump();

  if (_getAggregatedStatusDataCache.timestamp + 5 * 1000 > new Date().getTime())
    return Q(_getAggregatedStatusDataCache.data);
  if (_getAggregatedStatusDataCache.updatePromise !== null)
    return _getAggregatedStatusDataCache.updatePromise;

  _getAggregatedStatusDataCache.updatePending = true;
  var aggregateInfo = DEBUG ? {} : getServerStatusdump();
  var tasks = [];
  _config.webapi.aggregatePanelPorts.forEach(function(port) {
    if (port != _config.httpd.port || DEBUG)
      tasks.push(getStatusdumpFromPort(port).catch(function(err) { _logger.error("aggregate from port " + port + ": " + err); }));
  });

  return _getAggregatedStatusDataCache.updatePromise = Q
    .all(tasks)
    .then(function() {
      _getAggregatedStatusDataCache.data = aggregateInfo;
      _getAggregatedStatusDataCache.timestamp = new Date().getTime();
      return aggregateInfo;
    })
    .finally(function() { _getAggregatedStatusDataCache.updatePromise = null; });

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
}