var
  bodyParser = require("body-parser"),
  fs = require("graceful-fs"),
  log4js = require("log4js"),
  Q = require("q");

exports.init = init;

var MaxServersLowLimit = 150;
var MaxServersHighLimit = 200;

var _logger = log4js.getLogger("webadmin");

// interface for communication with the feeder.node.js module
var _feeder = {
  getStatsConnections: function() {},
  addServer: function (owner, ip, port, pass) { },
  removeServer: function(statsConn) { },
  writeConfig: function () { }
};

function init(config, app, feeder) {
  _feeder = feeder;
  _logger.setLevel(config.webadmin.logLevel || "INFO");

  var express = require("express");
  app.use(express.static(__dirname + "/../htdocs"));
  app.use(bodyParser.json());

  app.get("/", function(req, res) {
    res.redirect("/servers.html");
  });

  app.get("/api/servers", function (req, res) {
    _logger.info(req.connection.remoteAddress + ": /api/servers ");
    res.jsonp(getServerList());
    res.end();
  });

  app.post("/api/addserver", function (req, res) {
    _logger.info(req.connection.remoteAddress + ": /api/addserver " + JSON.stringify(req.body));
    res.json(addServer(req.body));
    res.end();
  });

  app.post("/api/editserver", function (req, res) {
    _logger.info(req.connection.remoteAddress + ": /api/editserver " + JSON.stringify(req.body));
    res.json(updateServers(req.body));
    res.end();
  });
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
  var serverCount = 0;
  for (var key in statsConn) {
    if (!statsConn.hasOwnProperty(key)) continue;
    ++serverCount;
    var conn = statsConn[key];
    if (!ownerByIp[conn.ip])
      ownerByIp[conn.ip] = conn.owner;
    if (!passByOwner[conn.owner])
      passByOwner[conn.owner] = conn.pass;
  }

  if (serverCount >= MaxServersHighLimit || (!passByOwner[req.owner] && serverCount >= MaxServersLowLimit))
    return { ok: false, msg: "Maximum number of servers reached. Please use a different admin panel." }

  if (!req.owner && !(req.owner = ownerByIp[serverIp]))
    return { ok: false, msg: "Owner required for new IPs" };

  if (ownerByIp[serverIp] && ownerByIp[serverIp] != req.owner)
    return { ok: false, msg: "This IP is owned by " + ownerByIp[serverIp] };

  if (passByOwner[req.owner] && passByOwner[req.owner] != req.newPwd1)
    return { ok: false, msg: "Wrong password (must match the one of your first listed server)" };
  if (!passByOwner[req.owner] && req.newPwd1 != req.newPwd2)
    return { ok: false, msg: "Passwords don't match" };

  _feeder.addServer(req.owner, serverIp, serverPort, req.newPwd1);
  _feeder.writeConfig();
  return { ok: true, msg: "Added " + req.newAddr };
}

function updateServers(req) {
  // req = {"action":"update","owner":null,"server":"45.79.100.154:27962","oldPwd":"","newPwd1":"","newPwd2":"","newAddr":"45.79.100.154:27962"}

  if (!req.owner && !req.server)
    return { ok: false, msg: "No owner or server specified" };

  if (!req.oldPwd)
    return { ok: false, msg: "Current password must not be blank" };

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

  var statsConn = _feeder.getStatsConnections();

  // check if someone else already owns the new IP
  if (newIp && newIp != serverIp) {
    var ipOwner = null;
    var validPass = false;
    for (var addr in statsConn) {
      if (!statsConn.hasOwnProperty(addr)) continue;
      var conn = statsConn[addr];
      if (conn.ip == newIp) {
        ipOwner = conn.owner;
        if (conn.pass && conn.pass == req.oldPwd) {
          validPass = true;
          break;
        }
      }
    }
    if (ipOwner && !validPass)
      return { ok: false, msg: "The new IP is owned by " + ipOwner };
  }


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
      _feeder.removeServer(conn);
      result.msg += conn.addr + " deleted\n";
      continue;
    }

    var newAddr = (newIp || conn.ip) + ":" + (newPort || conn.port);
    if ((newIp || newPort) && newAddr != conn.addr && statsConn[newAddr]) {
      result.ok = false;
      result.msg += newAddr + " is already in the list\n";
      continue;
    }

    if (req.newPwd1 || newIp || newPort) {
      _feeder.removeServer(conn);
      _feeder.addServer(req.owner || conn.owner, newIp || conn.ip, newPort || conn.port, req.newPwd1 || conn.pass, newAddr);
      result.msg += conn.addr + " updated\n";
    }
  }

  _feeder.writeConfig();

  return result;
}
