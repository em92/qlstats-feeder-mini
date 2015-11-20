/*
 Fetch Quake Live statistics from game server ZeroMQ message queues, 
 reformat it to XonStat match report format and 
 send it to sumbission.py via HTTP POST.

 Optionally save the stats to a .json.gz file or 
 process saved files specified as command line arguments.

 When the submission.py server is not responding with an "ok",
 a .json.gz is saved in the <jsondir>/errors/ folder.

 The script monitors changes to the config file and automatically 
 connects to added servers and disconnects from removed servers.

 Reconnecting after network errors is handled internally by ZeroMQ.
 When QL fails and becomes silent, this code will reconnect after an idle timeout.

*/

"use strict";

var
  fs = require("graceful-fs"),
  request = require("request"),
  log4js = require("log4js"),
  zlib = require("zlib"),
  zmq = require("zmq"),
  Q = require("q");

var MonitorInterval = 1000; // interval for checking connection status
var IpPortPassRegex = /^((?:[0-9]{1,3}\.){3}[0-9]{1,3}):([0-9]+)(?:\/(.*))?$/;  // IP:port/pass
var IdleReconnectTimeout = 15 * 60 * 1000; // reconnect to idle servers after 15min (QL stops sending data at some point)

var __dirname; // current working directory (defined by node.js)
var _logger; // log4js logger
var _config; // config data from cfg.json
var _statsConnections = {}; // dictionary with IP:port => StatsConnection

function StatsConnection(ip, port, pass, onZmqMessageCallback) { 
  this.ip = ip;
  this.port = port;
  this.pass = pass;
  this.onZmqMessageCallback = onZmqMessageCallback;

  this.addr = ip + ":" + port;
  this.matchStarted = false;
  this.playerStats = [];
}

StatsConnection.prototype.connect = function(isReconnect) {
  var self = this;

  this.sub = zmq.socket("sub");
  if (this.pass) {
    this.sub.sap_domain = "stats";
    this.sub.plain_username = "stats";
    this.sub.plain_password = this.pass;
  }

  //_logger.debug(self.addr + ": trying to connect");
  var failAttempt = 0;
  this.sub.on("connect", function () {
    if (isReconnect)
      _logger.debug(self.addr + ": reconnected successfully");
    else
      _logger.info(self.addr + ": connected successfully");
    failAttempt = 0;
    self.resetIdleTimeout();
  });
  this.sub.on("connect_delay", function () {
    if (failAttempt++ == 1)
      _logger.warn(self.addr + ": failed to connect, but will keep trying...");
  });
  this.sub.on("connect_retry", function() {
    if (failAttempt % 40 == 0)
      _logger.debug(self.addr + ": retrying to connect");
  });
  this.sub.on("message", function(data) {
    self.onZmqMessageCallback(self, data);
    self.resetIdleTimeout();
  });
  this.sub.on("disconnect", function() {
    _logger.warn(self.addr + ": disconnected");
    failAttempt = 0;
  });
  this.sub.on("monitor_error", function() {
    _logger.error(self.addr + ": error monitoring network status");
    setTimeout(function () { self.sub.monitor(MonitorInterval, 0); });
  });

  this.sub.monitor(MonitorInterval, 0);
  this.sub.connect("tcp://" + this.addr);
  this.sub.subscribe("");
}

StatsConnection.prototype.resetIdleTimeout = function () {
  var self = this;
  clearTimeout(this.idleTimeout);
  this.idleTimeout = setTimeout(function () { self.onIdleTimeout(); }, IdleReconnectTimeout);
}

StatsConnection.prototype.onIdleTimeout = function () {
  _logger.debug(this.addr + ": reconnecting to idle server");
  this.disconnect();
  this.connect(true);
}

StatsConnection.prototype.disconnect = function () {
  //try { this.sub.unsubscribe(""); } catch (err) { }
  try { this.sub.disconnect("tcp://" + addr); } catch (err) { }
  try { this.sub.unmonitor(); } catch (err) { }
  try { this.sub.close(); } catch (err) { }
}


function main() {
  _logger = log4js.getLogger("feeder");
  Q.longStackSupport = false; // enable if you have to trace a problem, but it has a HUGE performance penalty

  reloadConfig();

  // process saved .json.gz files specified on the command line ... or connect to live zmq feeds
  if (process.argv.length > 2) {
    for (var i = 2; i < process.argv.length; i++)
      feedJsonFile(process.argv[i]);
  } else {
    connectToServerList(_config.feeder.servers);
  var timer;
    fs.watch(__dirname + "/cfg.json", function () {
    // execute the reload after a delay to give an editor the chance to delete/truncate/write/flush/close/release the file
      if (timer)
        clearTimeout(timer);
    timer = setTimeout(function() {
        timer = undefined;
        if (reloadConfig())
          connectToServerList(_config.feeder.servers);
    }, 500);
    });
  }
}

function reloadConfig() {
  try {
    _config = JSON.parse(fs.readFileSync(__dirname + "/cfg.json"));
    if (!(_config.feeder.saveDownloadedJson || _config.feeder.importDownloadedJson)) {
      _logger.error("At least one of feeder.saveDownloadedJson or feeder.importDownloadedJson must be set in cfg.json");
      process.exit();
    }

    _logger.setLevel(_config.feeder.logLevel || log4js.levels.INFO);
    _logger.info("Reloaded modified of cfg.json");
    return true;
  }
  catch (err) {
    // while being saved by the editor, the config file can be locked, temporarily removed or incomplete.
    // there will be another file system watcher event when the lock is released
    if (err.code != "EBUSY" && err.code != "ENOENT")
      _logger.error("Failed to reload the server list: " + err);
    return false;
  }
}

function feedJsonFile(file) {
  if (!file.match(/.json(.gz)?$/)) {
    _logger.warn("Skipping file (not *.json[.gz]): " + file);
    return false;
  }

  _logger.info("Loading " + file);
  var data = fs.readFileSync(file);
  if (file.slice(-3) == ".gz")
    data = zlib.gunzipSync(data);
  return processGame(JSON.parse(data));
}

function connectToServerList(servers) {
  if (!servers.length) {
    _logger.error("There are no servers configured in cfg.json.");
    return;
  }

  // create a new dictionary with the zmq connections for the new server list
  // after the loop _statsConnections only contains servers which are no longer configured
  var newZmqConnections = {};
  var conn;
  for (var i = 0; i < servers.length; i++) {
    var server = servers[i];
    var match = IpPortPassRegex.exec(server);
    if (!match) {
      _logger.warn(server + ": ignoring server (not IP:port[/password])");
      continue;
    }

    var ip = match[1];
    var port = match[2];
    var pass = match[3];
    var addr = ip + ":" + port;
    conn = _statsConnections[addr];
    if (conn && pass == conn.pass)
      delete _statsConnections[addr];
    else {
      conn = new StatsConnection(ip, port, pass, onZmqMessageCallback);
      conn.connect();
    }
    newZmqConnections[addr] = conn;
  }

  // shut down connections to servers which are no longer in the config
  for (var addr in _statsConnections) {
    if (!_statsConnections.hasOwnProperty(addr)) continue;
    conn = _statsConnections[addr];
    _logger.info(addr + ": disconnected. Server was removed from config.");
    conn.disconnect();
  }

  _statsConnections = newZmqConnections;
}

function onZmqMessageCallback(conn, data) {
  var msg = data.toString();
  var obj = JSON.parse(msg);
  _logger.debug(conn.addr + ": received ZMQ message: " + obj.TYPE);
  if (obj.TYPE == "MATCH_STARTED") {
    _logger.debug(conn.addr + ": match started");
    conn.matchStarted = true;
  }
  else if (obj.TYPE == "PLAYER_STATS") {
    if (!obj.DATA.WARMUP)
      conn.playerStats.push(obj.DATA);
  }
  else if (obj.TYPE == "MATCH_REPORT") {
    _logger.debug(conn.addr + ": match finished");
    var stats = {
      serverIp: conn.ip,
      serverPort: conn.port,
      gameEndTimestamp: Math.round(new Date().getTime() / 1000),
      matchStats: obj.DATA,
      playerStats: conn.playerStats
    };
    conn.playerStats = [];
    processMatch(stats);
  }
}

function processMatch(stats) {
  var tasks = [];
  if (_config.feeder.saveDownloadedJson)
    tasks.push(saveGameJson(stats));
  if (_config.feeder.importDownloadedJson)
    tasks.push(processGame(stats));
  return Q
    .allSettled(tasks)
    .catch(function(err) { _logger.error(err.stack); });
}

function saveGameJson(game, toErrorDir) {
  var basedir = _config.feeder.jsondir;
  var date = new Date(game.gameEndTimestamp * 1000);
  var dirName1 = toErrorDir ? basedir : basedir + date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2);
  var dirName2 = toErrorDir ? basedir + "errors" : dirName1 + "/" + ("0" + date.getDate()).slice(-2);
  var filePath = dirName2 + "/" + game.matchStats.MATCH_GUID + ".json.gz";
  _logger.debug("saving JSON: " + filePath);
  return createDir(dirName1)
    .then(createDir(dirName2))
    .then(function () {
      var json = JSON.stringify(game);
      return Q.nfcall(zlib.gzip, json);
    })
    .then(function (gzip) {
      return Q.nfcall(fs.writeFile, filePath, gzip);
    })
    .fail(function(err) { _logger.error("Can't save game JSON: " + err.stack); });
}

function createDir(dir) {
  var defer = Q.defer();
  // fs.mkdir returns an error when the directory already exists
  fs.mkdir(dir, function(err) {
    if (err && err.code != "EEXIST")
      defer.reject(err);
    else
      defer.resolve(dir);
  });
  return defer.promise;
}

function processGame(game) {
  var addr = game.serverIp + ":" + game.serverPort;

  if (game.matchStats.ABORTED) {
    _logger.debug(addr + ": ignoring aborted game " + game.matchStats.MATCH_GUID);
    return false;
  }

  var gt = game.matchStats.GAME_TYPE.toLowerCase();
  if (",ffa,duel,ca,tdm,ctf,ft,".indexOf("," + gt + ",") < 0) {
    _logger.debug(addr + ": unsupported game type: " + gt);
    return false;
  }

  var report = [];
  exportMatchInformation(gt, game, report);

  var allWeapons = { gt: "GAUNTLET", mg: "MACHINEGUN", sg: "SHOTGUN", gl: "GRENADE", rl: "ROCKET", lg: "LIGHTNING", rg: "RAILGUN", pg: "PLASMA", bfg: "BFG", hmg: "HMG", cg: "CHAINGUN", ng: "NAILGUN", pm: "PROXMINE", gh: "OTHER_WEAPON" };

  var ok = false;
  if ("ffa,duel,race".indexOf(gt) >= 0)
    ok = exportScoreboard(gt, game.playerStats, 0, true, allWeapons, report);
  else if ("ca,tdm,ctf,ft".indexOf(gt) >= 0) {
    var redWon = parseInt(game.matchStats.TSCORE0) > parseInt(game.matchStats.TSCORE1);
    ok = exportTeamSummary(gt, game.matchStats, 1, report)
      && exportScoreboard(gt, game.playerStats, 1, redWon, allWeapons, report)
      && exportTeamSummary(gt, game.matchStats, 2, report)
      && exportScoreboard(gt, game.playerStats, 2, !redWon, allWeapons, report);
  }

  if (!ok) {
    _logger.info(addr + ": match doesn't meet requirements: " + game.matchStats.MATCH_GUID);
    return false;
  }

  return postMatchReportToXonstat(addr, game, report.join("\n"));
}

function exportMatchInformation(gt, game, report) {
  report.push("0 " + game.serverIp); // not XonStat standard
  report.push("S " + game.matchStats.SERVER_TITLE);
  report.push("I " + game.matchStats.MATCH_GUID);
  report.push("G " + gt);
  report.push("M " + game.matchStats.MAP);
  report.push("O baseq3");
  report.push("V 7"); // CA must be >= 6 
  report.push("R .1");
  report.push("U " + game.serverPort);
  report.push("D " + game.matchStats.GAME_LENGTH);
}

function exportScoreboard(gt, scoreboard, team, isWinnerTeam, weapons, report) {
  var playerMapping = { SCORE: "score", KILLS: "kills", DEATHS: "deaths" };
  var damageMapping = { DEALT: "pushes", TAKEN: "destroyed" };
  var medalMapping = { CAPTURES: "captured", ASSISTS: "returns" };

  if (!scoreboard || !scoreboard.length || scoreboard.length < 2) {
    _logger.debug("not enough players in team " + team);
    return false;
  }

  if (gt == "ft")
    medalMapping.ASSISTS = "revivals";

  for (var i = 0; i < scoreboard.length; i++) {
    var p = scoreboard[i];
    if ((team || p.TEAM) && p.TEAM != team)
      continue;
    report.push("P " + p.STEAM_ID);
    report.push("n " + p.NAME);
    if (team)
      report.push("t " + team);
    report.push("e matches 1");
    report.push("e scoreboardvalid 1");
    report.push("e alivetime " + p.PLAY_TIME);
    report.push("e rank " + p.RANK);
    if ((team == 0 && p.RANK == "1") || isWinnerTeam)
      report.push("e wins 1");
    report.push("e scoreboardpos " + p.RANK);

    mapFields(p, playerMapping, report);
    mapFields(p.DAMAGE, damageMapping, report);
    mapFields(p.MEDALS, medalMapping, report);

    for (var w in weapons) {
      if (!weapons.hasOwnProperty(w)) continue;
      var lname = weapons[w];
      var wstats = p.WEAPONS[lname];
      var kills = wstats && wstats.K;
      if (kills === undefined)
        continue;
      report.push("e acc-" + w + "-cnt-fired " + wstats.S);
      report.push("e acc-" + w + "-cnt-hit " + wstats.H);
      report.push("e acc-" + w + "-frags " + wstats.K);
    }
  }
  return true;
}

function exportTeamSummary(gt, matchstats, team, data) {
  var mapping = { CAPTURES: "caps", SCORE: "score", ROUNDS_WON: "rounds" };
  var score = matchstats["TSCORE" + (team - 1)];
  var info = {};
  if (gt == "ctf")
    info.CAPTURES = score;
  else if (gt == "tdm")
    info.SCORE = score;
  else if (gt == "ca" || gt == "ft")
    info.ROUNDS_WON = score;
  else
    return false;

  data.push("Q team#" + team);
  mapFields(info, mapping, data);
  // not filled:
  // scoreboard-score
  // scoreboard-caps
  // scoreboard-rounds
  return true;
}

function mapFields(info, mapping, data) {
  for (var field in mapping) {
    if (!mapping.hasOwnProperty(field)) continue;
    if (field in info)
      data.push("e scoreboard-" + mapping[field] + " " + info[field]);
  }
}

function postMatchReportToXonstat(addr, game, report) {
  var defer = Q.defer();
  request({
      uri: "http://localhost:" + _config.feeder.xonstatPort + "/stats/submit",
      timeout: 10000,
      method: "POST",
      headers: { "X-D0-Blind-Id-Detached-Signature": "dummy" },
      body: report
    },
    function(err) {
      if (err) {
        _logger.error(addr + ": upload failed: " + game.matchStats.MATCH_GUID + ": " + err);
        saveGameJson(game, true);
        defer.reject(new Error(err));
      } else {
        _logger.info(addr + ": match uploaded: " + game.matchStats.MATCH_GUID);
        defer.resolve(true);
      }
    });
  return defer.promise;
}

main();
