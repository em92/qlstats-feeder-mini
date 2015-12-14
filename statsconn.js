zmq = require("zmq");

exports.create = create;
exports.setLogger = setLogger;


var MonitorInterval = 1000; // interval for checking connection status
var ConnectAttemptInterval = 60 * 1000; // try for 60 sec to establish a connection, then consider the server to be offline
var OfflineServerRetryInterval = 5 * 60 * 1000; // try to reconnect to offline servers after 5min
var IdleReconnectInvterval = 15 * 60 * 1000; // reconnect to idle servers after 15min (QL stops sending data at some point)
var WrongPasswordInterval = 5 * 1000; // when connection is closed within this interval after connecting, it's probably due to wrong password

var _logger = {};
_logger.trace = _logger.debug = _logger.info = _logger.warn = _logger.error = function (msg) { log(msg); }

function create(owner, ip, port, pass, onZmqMessageCallback) {
  return new StatsConnection(owner, ip, port, pass, onZmqMessageCallback);
}

function setLogger(logger) {
  _logger = logger;
}

function StatsConnection(owner, ip, port, pass, onZmqMessageCallback) {
  this.owner = owner;
  this.ip = ip;
  this.port = port;
  this.pass = pass;
  this.onZmqMessageCallback = onZmqMessageCallback;

  this.addr = ip + ":" + port;
  this.matchStarted = false;
  this.playerStats = [];
  this.reconnectTimer = null;

  this.connecting = false;
  this.connected = false;
  this.disconnected = false;
  this.badPassword = false;
  this.lastMessageUtc = 0;
  this.connectUtc = 0;
}

StatsConnection.prototype.connect = function (isReconnect) {
  var self = this;
  var failAttempt = 0;

  this.badPassword = false;
  this.disconnected = false;
  this.connected = false;
  this.connecting = true;
  this.connectUtc = Date.now();
  if (this.reconnectTimer)
    clearTimeout(this.reconnectTimer);
  this.reconnectTimer = null;

  this.sub = zmq.socket("sub");
  if (this.pass) {
    this.sub.sap_domain = "stats";
    this.sub.plain_username = "stats";
    this.sub.plain_password = this.pass;
  }

  //_logger.debug(self.addr + ": trying to connect");

  this.sub.on("connect", function () {
    self.connected = true;
    self.connectUtc = Date.now();
    if (isReconnect)
      _logger.debug(self.addr + ": reconnected successfully");
    else
      _logger.info(self.addr + ": connected successfully");
    self.resetIdleTimeout();
  });

  this.sub.on("connect_delay", function () {
    if (failAttempt++ == 3)
      _logger.warn(self.addr + ": failed to connect, but will keep trying...");
    if (Date.now() - self.connectUtc >= ConnectAttemptInterval) {
      self.disconnect();
      self.startReconnectTimer();
    }
  });

  this.sub.on("connect_retry", function () {
    self.connecting = true;
    if (Date.now() - self.connectUtc >= ConnectAttemptInterval) {
      self.disconnect();
      self.startReconnectTimer();
    }
    else if (failAttempt % 40 == 0)
      _logger.debug(self.addr + ": retrying to connect");
  });

  this.sub.on("message", function (data) {
    self.lastMessageUtc = Date.now();
    self.onZmqMessageCallback(self, data);
    self.resetIdleTimeout();
  });

  this.sub.on("disconnect", function () {
    if (Date.now() - self.connectUtc <= WrongPasswordInterval) {
      _logger.warn(self.addr + ": disconnected (probably wrong password)");
      self.disconnect();
      self.badPassword = true;
      self.startReconnectTimer();
    }
    else {
      _logger.warn(self.addr + ": disconnected");
      failAttempt = 0;
      self.connectUtc = Date.now();
      self.connecting = true;
    }
    self.connected = false;
  });

  this.sub.on("monitor_error", function () {
    if (!self.disconnected) {
      _logger.error(self.addr + ": error monitoring network status");
      setTimeout(function() { self.sub.monitor(MonitorInterval, 0); });
    }
  });

  this.sub.monitor(MonitorInterval, 0);
  this.sub.connect("tcp://" + this.addr);
  this.sub.subscribe("");
}

StatsConnection.prototype.startReconnectTimer = function() {
  var self = this;
  this.reconnectTimer = setTimeout(function() { self.connect(); }, OfflineServerRetryInterval);
}

StatsConnection.prototype.resetIdleTimeout = function () {
  var self = this;
  clearTimeout(this.idleTimeout);
  this.idleTimeout = setTimeout(function () { self.onIdleTimeout(); }, IdleReconnectInvterval);
}

StatsConnection.prototype.onIdleTimeout = function () {
  _logger.debug(this.addr + ": reconnecting to idle server");
  this.disconnect();
  this.connect(true);
}

StatsConnection.prototype.disconnect = function () {
  var err;
  if (!this.disconnected) {
    //try { this.sub.unsubscribe(""); } catch (err) { }
    try { this.sub.unmonitor(); } catch (err) { _logger.error("Can't unmonitor " + this.addr + ": " + err) }
    if (this.connected)
      try { this.sub.disconnect("tcp://" + this.addr); } catch (err) { _logger.error("Can't disconnect from " + this.addr + ": " + err) }
    try { this.sub.close(); } catch (err) { _logger.error("Can't close " + this.addr + ": " + err) }
  }
  this.connected = false;
  this.connecting = false;
  this.lastMessageUtc = 0;
  this.disconnected = true;

  if (this.idleTimeout)
    clearTimeout(this.idleTimeout);
  this.idleTimeout = null;

  if (this.reconnectTimer)
    clearTimeout(this.reconnectTimer);
  this.reconnectTimer = null;
}

StatsConnection.prototype.compareTo = function (other) {
  var c = (this.owner || "").localeCompare(other.owner || "");
  if (c != 0) return c;
  c = compareIp(this.ip, other.ip);
  if (c != 0) return c;
  if (this.port < other.port) return -1;
  if (this.port > other.port) return +1;
  return 0;

  function compareIp(a, b) {
    var x = a.split(".").map(function (n) { return parseInt(n); });
    var y = b.split(".").map(function (n) { return parseInt(n); });
    for (var i = 0; i < 4; i++) {
      if (x[i] < y[i]) return -1;
      if (x[i] > y[i]) return +1;
    }
    return 0;
  }
}