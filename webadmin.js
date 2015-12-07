var
  express = require("express"),
  app = express(),
  http = require("http"),
  server = http.createServer(app);

exports.startHttpd = startHttpd;
exports.setStatsConnectionProvider = function(callback) { _statsConnectionProvider = callback; };

var _config;
var _statsConnectionProvider = function() {};

function startHttpd(config) {
  if (!config.enabled)
    return;

  _config = config;

  app.use(express.static(__dirname + '/htdocs'));

  app.get('/api/servers', function (req, res) {
    res.jsonp(getServerList());
    res.end();
  });

  app.listen(_config.port);
}

function getServerList() {
  var statsConn = _statsConnectionProvider();
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
