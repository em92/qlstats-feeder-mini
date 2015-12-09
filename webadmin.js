var
  express = require("express"),
  app = express(),
  http = require("http"),
  server = http.createServer(app),
  bodyParser = require("body-parser"),
  fs = require("graceful-fs"),
  Q = require("q");

exports.startHttpd = startHttpd;
exports.setFeeder = setFeeder;

var _config;
var _feeder = {
  getStatsConnections: function() {},
  writeConfig: function () { },
  addServer: function (owner, ip, port, pass) { }
};

function setFeeder(feeder) {
  _feeder = feeder;
}

function startHttpd(config) {
  if (!config.webadmin.enabled)
    return;

  _config = config;

  app.use(express.static(__dirname + "/htdocs"));
  app.use(bodyParser.json());

  app.get("/", function(req, res) {
    res.redirect("/servers.html");
  });

  app.get("/api/servers", function (req, res) {
    res.jsonp(getServerList());
    res.end();
  });

  app.post("/api/addserver", function (req, res) {
    res.json(addServer(req.body));
    res.end();
  });

  app.post("/api/editserver", function (req, res) {
    res.json(updateServers(req.body));
    res.end();
  });

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


  app.listen(_config.webadmin.port);
}

function getServerList() {
  var statsConn = _feeder.getStatsConnections();
  var addrList = [];
  for (var key in statsConn) {
    if (!statsConn.hasOwnProperty(key)) continue;
    var conn = statsConn[key];
    addrList.push(conn);
  }

  addrList.sort(function (a, b) { return a.compareTo(b); });
  return addrList.map(function(item) {
    return {
      owner: item.owner,
      ip: item.ip,
      port: parseInt(item.port),
      status: item.connected ? "connected" : item.connecting ? "connecting" : item.badPassword ? "badPassword" : "disconnected",
      lastMessageUtc: item.lastMessageUtc
    };
  });
}

function addServer(req) {
  if (req.owner && ! /^[\w\d_\-\[\]{}^|]{3,20}$/.test(req.owner))
    return { ok: false, msg: "Invalid owner name (length or characters)" };

  if (!req.newPwd1)
    return { ok: false, msg: "ZMQ password must not be blank" };

  var serverMatch = /^((?:\d+\.){3}\d+):(\d+)$/.exec(req.newAddr);
  if (!serverMatch)
    return { ok: false, msg: "Invalid server address (IPv4:port required)" };
  var serverIp = serverMatch[1];
  var serverPort = serverMatch[2];

  var statsConn = _feeder.getStatsConnections();

  if (statsConn[req.newAddr])
    return { ok: false, msg: "This IP:port is already registered" };

  var ownerByIp = {};
  var passByOwner = {};
  for (var key in statsConn) {
    if (!statsConn.hasOwnProperty(key)) continue;
    var conn = statsConn[key];
    if (!ownerByIp[conn.ip])
      ownerByIp[conn.ip] = conn.owner;
    if (!passByOwner[conn.owner])
      passByOwner[conn.owner] = conn.pass;
  }

  if (!req.owner && !(req.owner = ownerByIp[serverIp]))
    return { ok: false, msg: "Owner required for new IPs" };

  if (ownerByIp[serverIp] && ownerByIp[serverIp] != req.owner)
    return { ok: false, msg: "This IP is owned by " + ownerByIp[serverIp] };

  if (passByOwner[req.owner] && passByOwner[req.owner] != req.newPwd1)
    return { ok: false, msg: "Wrong password (must match the one of your first listed server)" };
  else if (!passByOwner[req.owner] && req.newPwd1 != req.newPwd2)
    return { ok: false, msg: "Passwords don't match" };

  var conn = _feeder.connectServer(req.owner, serverIp, serverPort, req.newPwd1);
  statsConn[req.newAddr] = conn;
  _feeder.writeConfig();
  return { ok: true, msg: "Added " + req.newAddr };
}

function updateServers(req) {
  // req = {"action":"update","owner":null,"server":"45.79.100.154:27962","oldPwd":"","newPwd1":"","newPwd2":"","newAddr":"45.79.100.154:27962"}

  if (!req.owner && !req.server)
    return { ok: false, msg: "No owner or server specified" };

  if ((req.newPwd1 || req.newPwd2) && req.newPwd1 != req.newPwd2)
    return { ok: false, msg: "New passwords don't match" };

  var serverIp = null, serverPort = null;
  if (req.server) {
    var serverMatch = /^((?:\d+\.){3}\d+)(?::(\d+))?$/.exec(req.server);
    if (!serverMatch)
      return { ok: false, msg: "Server must be an IPv4 address" };
    serverIp = serverMatch[1];
    serverPort = serverMatch[2];
  }

  var newIp = null, newPort = null;
  if (req.newAddr) {
    var addrMatch = /^((?:\d+\.){3}\d+)(?::(\d+))?$/.exec(req.newAddr);
    if (!addrMatch)
      return { ok: false, msg: "New address must be an IPv4 address" };
    newIp = addrMatch[1];
    newPort = addrMatch[2];

    if (!serverIp)
      return { ok: false, msg: "Address change is only allowed for single IP or port" };
  }

  if (req.newAddr) {
    // TODO: check if someone else already owns the IP or IP:port
  }


  var statsConn = _feeder.getStatsConnections();
  var result = { ok: true, msg: "" };
  for (var key in statsConn) {
    if (!statsConn.hasOwnProperty(key)) continue;
    var conn = statsConn[key];
    if (req.owner && conn.owner != req.owner) continue;
    if (serverIp && conn.ip != serverIp) continue;
    if (serverPort && conn.port != serverPort) continue;

    if (conn.pass != req.oldPwd) {
      result.ok = false;
      result.msg += "Wrong ZMQ stats password for " + conn.addr + "\n";
      continue;
    }

    if (req.action == "delete") {
      conn.disconnect();
      delete statsConn[key];
      result.msg += conn.addr + " deleted\n";
      continue;
    }

    var newAddr = (newIp || conn.ip) + ":" + (newPort || conn.port);
    if ((newIp || newPort) && newAddr != conn.addr && statsConn[newAddr]) {
      result.ok = false;
      result.msg += newAddr + " is already in the list";
      continue;
    }

    if (req.newPwd1 || newIp || newPort) {
      if (req.newPwd1)
        conn.pass = req.newPwd1;
      if (newIp)
        conn.ip = newIp;
      if (newPort)
        conn.port = newPort;
      conn.addr = newAddr;
      conn.disconnect();
      conn.connect();
      result.msg += conn.addr + " updated\n";
    }
  }

  _feeder.writeConfig();

  return result;
}

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