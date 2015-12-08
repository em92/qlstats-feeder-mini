var
  express = require("express"),
  app = express(),
  http = require("http"),
  server = http.createServer(app),
  bodyParser = require("body-parser");

exports.startHttpd = startHttpd;
exports.setFeeder = setFeeder;

var _config;
var _feeder = {
  getStatsConnections: function() {},
  writeConfig: function() {}
};

function setFeeder(feeder) {
  _feeder = feeder;
}

function startHttpd(config) {
  if (!config.enabled)
    return;

  _config = config;

  app.use(express.static(__dirname + '/htdocs'));
  app.use(bodyParser.json());

  app.get('/api/servers', function (req, res) {
    res.jsonp(getServerList());
    res.end();
  });

  app.post('/api/update', function (req, res) {
    res.json(updateServers(req.body));
    res.end();
  });

  app.listen(_config.port);
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

  //console.log("action=" + req.action + ", owner=" + req.owner + ", server=" + req.server + ", serverIp=" + serverIp + ", serverPort=" + serverPort);

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

    if (req.newPwd1 || newIp || newPort) {
      if (req.newPwd1)
        conn.pass = req.newPwd1;
      if (newIp)
        conn.ip = newIp;
      if (newPort)
        conn.port = newPort;
      conn.addr = conn.ip + ":" + conn.port;
      conn.disconnect();
      conn.connect();
      result.msg += conn.addr + " updated\n";
    }
  }
  
  return result;
}