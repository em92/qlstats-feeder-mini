/*
 Fetch Quake Live statistics from game servers' ZeroMQ message queues, 
 reformat it to XonStat match report format and 
 send it to sumbission.py via HTTP POST.

 Optionally save the stats to a .json.gz file or 
 process saved files specified as command line arguments.

 When the submission.py server is not responding with an "ok",
 a .json.gz is saved in the <jsondir>/errors/ folder.

 The script monitors changes to the config file an automatically 
 connects to added server and disconnects from removed servers.

*/

"use strict";

var
  fs = require("graceful-fs"),
  async = require("async"),
  request = require("request"),
  log4js = require("log4js"),
  zlib = require("zlib"),
  zmq = require("zmq"),
  Q = require("q");

var MonitorInterval = 1000; // interval for checking connection status
var IpPortPassRegex = /^((?:[0-9]{1,3}\.){3}[0-9]{1,3}):([0-9]+)(?:\/(.+))?$/;  // IP:port/pass

var __dirname; // current working directory (defined by node.js)
var _logger; // log4js logger
var _config; // config data from cfg.json
var _zmqConnections = {}; // dictionary with IP:port => zqm socket

main();

function main() {
  _logger = log4js.getLogger("feeder");
  Q.longStackSupport = false; // enable if you have to trace a problem, but it has a HUGE performance penalty

  reloadConfig();

  // process saved .json.gz files specified on the command line ... or connect to live zmq feeds
  if (process.argv.length > 2) {
    for (var i = 2; i < process.argv.length; i++)
      feedJsonFile(process.argv[i]);
  } else {
    connectToServerList(_config.loader.servers);
    fs.watch(__dirname + "/cfg.json", function () {
      if (reloadConfig())
        connectToServerList(_config.loader.servers);
    });
  }
}

//==========================================================================================
// QL stats data tracker
//==========================================================================================


function feedJsonFile(file) {
  if (!file.match(/.json(.gz)?$/)) {
    _logger.warn("Skipping file (not *.json[.gz]): " + file);
    return;
  }
  var isGzip = file.slice(-3) == ".gz";
  _logger.info("Loading " + file);
  var data = fs.readFileSync(file);
  if (isGzip)
    data = zlib.gunzipSync(data);

  var game = JSON.parse(data);
  processGame(game);
}

function reloadConfig() {
  try {
    _config = JSON.parse(fs.readFileSync(__dirname + "/cfg.json"));
    if (!(_config.loader.saveDownloadedJson || _config.loader.importDownloadedJson)) {
      _logger.error("At least one of loader.saveDownloadedJson or loader.importDownloadedJson must be set in cfg.json");
      process.exit();
    }
    _logger.setLevel(_config.loader.logLevel || log4js.levels.INFO);
    _logger.info("Reloaded modified of cfg.json");
    return true;
  } catch (err) {
    // while being saved by the editor, the config file can be locked or removed temporarily
    // there will be another file system watcher event when the lock is released
    if (err.code != "EBUSY" && err.code != "ENOENT")
      _logger.error("Failed to reload the server list: " + err);
    return false;
  }
}

function connectToServerList(servers) {
  if (!servers.length) {
    _logger.error("There are no servers configured in cfg.json.");
    return;
  }

  // create a new dictionary with the zmq connections for the new server list
  var newZmqConnections = {};
  for (var i = 0; i < servers.length; i++) {
    var server = servers[i];
    var match = IpPortPassRegex.exec(server);
    if (!match) {
      _logger.warn(server + ": ignoring server (not IP:port[/password])");
      continue;
    }

    var ip = match[1];
    var port = match[2];
    var key = ip + ":" + port;
    var sub = _zmqConnections[key];
    if (sub)
      delete _zmqConnections[key];
    else
      sub = connectToZmq(ip, port, match[3]);
    newZmqConnections[key] = sub;
  }

  // shut down connections to servers which are no longer in the config
  for (var key in _zmqConnections) {
    if (!_zmqConnections.hasOwnProperty(key)) continue;
    var sub = _zmqConnections[key];
    _logger.info(key + ": disconnected. Server was removed from config.");
    try { sub.disconnect(); } catch (err) { }
    try { sub.close(); } catch (err) { }
    try { sub.unmonitor(); } catch (err) { }
  }

  _zmqConnections = newZmqConnections;
}

function connectToZmq(ip, port, pass) {
  var addr = ip + ":" + port;

  var sub = zmq.socket("sub");
  if (pass) {
    sub.sap_domain = "stats";
    sub.plain_username = "stats";
    sub.plain_password = pass;
  }

  _logger.debug(addr + ": trying to connect");
  var failAttempt = 0;
  var context = { addr: addr, ip: ip, port: port, matchStarted: false, playerStats: [] }
  sub.on("message", function(data) { onZmqMessage(context, data) });
  sub.on("connect", function () { _logger.info(addr + ": connected successfully"); failAttempt = 0; });
  sub.on("connect_delay", function () {
    if (failAttempt++ == 1)
      _logger.warn(addr + ": failed to connect, but will keep trying...");
  });
  sub.on("connect_retry", function () { if (failAttempt % 40 == 0) _logger.debug(addr + ": retrying to connect"); });
  sub.on("disconnect", function () { _logger.warn(addr + ": disconnected"); failAttempt = 0; });
  sub.on("monitor_error", function() { _logger.error(addr + ": error monitoring network status"); setTimeout(function() { sub.monitor(MonitorInterval, 0); }); });

  sub.monitor(MonitorInterval, 0);
  sub.connect("tcp://" + addr);
  sub.subscribe("");
  return sub;
}

function onZmqMessage(context, data) {
  var msg = data.toString();
  var obj = JSON.parse(msg);
  _logger.debug(context.addr + ": received ZMQ message: " + obj.TYPE);
  if (obj.TYPE == "MATCH_STARTED") {
    _logger.info(context.addr + ": match started");
    context.matchStarted = true;
  }
  else if (obj.TYPE == "PLAYER_STATS") {
    //if (context.matchStarted)
      context.playerStats.push(obj.DATA);
  }
  else if (obj.TYPE == "MATCH_REPORT") {
    _logger.info(context.addr + ": match finished");
    var stats = {
      serverIp: context.ip,
      serverPort: context.port,
      gameEndTimestamp: Math.round(new Date().getTime() / 1000),
      matchStats: obj.DATA,
      playerStats: context.playerStats
    };
    context.playerStats = [];
    //if (context.matchStarted) {
    //  context.matchStarted = false;
      processMatch(stats);
    //}
  }
}

function processMatch(stats) {
  var tasks = [];
  if (_config.loader.saveDownloadedJson)
    tasks.push(saveGameJson(stats));
  if (_config.loader.importDownloadedJson)
    tasks.push(processGame(stats));
  return Q
    .allSettled(tasks)
    .catch(function(err) { _logger.error(err.stack); });
}

function saveGameJson(game, toErrorDir) {
  var GAME_TIMESTAMP = game.gameEndTimestamp;
  var basedir = _config.loader.jsondir;
  var date = new Date(GAME_TIMESTAMP * 1000);
  var dirName1 = toErrorDir ? basedir : basedir + date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2);
  var dirName2 = toErrorDir ? basedir + "errors" : dirName1 + "/" + ("0" + date.getDate()).slice(-2);
  var filePath = dirName2 + "/" + game.matchStats.MATCH_GUID + ".json.gz";
  _logger.debug("saving JSON: " + filePath);
  return createDir(dirName1)
    .then(createDir(dirName2))
    .then(function() {
      var json = JSON.stringify(game);
      return Q.nfcall(zlib.gzip, json);
    })
    .then(function(gzip) {
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
  var defer = Q.defer();
  var gt = getGametype(game.matchStats.GAME_TYPE);
  if (",ffa,duel,ca,tdm,ctf,ft,".indexOf("," + gt + ",") < 0) {
    _logger.debug(game.serverIp + ":" + game.serverPort + ": unsupported game type: " + gt);
    return false;
  }

  var data = [];
  data.push("0 " + game.serverIp); // not XonStat standard
  data.push("S " + game.matchStats.SERVER_TITLE);
  data.push("I " + game.matchStats.MATCH_GUID);
  data.push("G " + gt);
  data.push("M " + game.matchStats.MAP);
  data.push("O baseq3");
  data.push("V 7"); // CA must be >= 6 
  data.push("R .1");
  data.push("U " + game.serverPort);
  data.push("D " + game.matchStats.GAME_LENGTH);

  var allWeapons = { gt: "GAUNTLET", mg: "MACHINEGUN", sg: "SHOTGUN", gl: "GRENADE", rl: "ROCKET", lg: "LIGHTNING", rg: "RAILGUN", pg: "PLASMA", bfg: "BFG", hmg: "HMG", cg: "CHAINGUN",  ng: "NAILGUN", pm: "PROXMINE", gh: "OTHER_WEAPON"};
 
  var ok = false;
  if ("ffa,duel".indexOf(gt) >= 0)
    ok = exportScoreboard(game.playerStats, 0, true, allWeapons, data);
  //else if (game.RACE_SCOREBOARD)
    //  ok = exportScoreboard(game.RACE_SCOREBOARD, 0, true, allWeapons, data);
  else if ("ca,tdm,ctf,ft".indexOf(gt) >= 0) {
    var redWon = parseInt(game.matchStats.TSCORE0) > parseInt(game.matchStats.TSCORE1);
    ok = exportTeamSummary(gt, game.matchStats, 1, data)
    && exportScoreboard(game.playerStats, 1, redWon, allWeapons, data)
    && exportTeamSummary(gt, game.matchStats, 2, data)
    && exportScoreboard(game.playerStats, 2, !redWon, allWeapons, data);
  }

  if (!ok) {
    _logger.info(game.serverIp + ":" + game.serverPort + ": match doesn't meet requirements for stats tracking");
    return false;
  }

  request({
      uri: "http://localhost:" + _config.loader.xonstatPort + "/stats/submit",
      timeout: 10000,
      method: "POST",
      headers: { /*"X-Forwarded-For": "0.0.0.0", */ "X-D0-Blind-Id-Detached-Signature": "dummy" },
      body: data.join("\n")
    },
    function(err) {
      if (err) {
        _logger.error("Error posting data to QLstats: " + err);
        saveGameJson(game, true);
        defer.reject(new Error(err));
      } else {
        _logger.debug("Successfully posted data to QLstats");
        defer.resolve(true);
      }
    });
  return defer.promise;
}

function getGametype(gt) {
  gt = gt.toLowerCase();
  switch (gt) {
  case "tourney":
  case "duel":
    return "duel";
  case "ffa":
  case "dm":
    return "ffa";
  case "ca":
  case "tdm":
  case "ctf":
  case "ft":
  case "race":
    return gt;
  default:
    return gt;
  }
}

function exportScoreboard(scoreboard, team, isWinnerTeam, weapons, data) {
  var playerMapping = { SCORE: "score", KILLS: "kills", DEATHS: "deaths" };
  var damageMapping = { DEALT: "pushes", TAKEN: "destroyed" };
  var medalMapping = { CAPTURES: "captured", ASSISTS: "returns", THAWS: "revivals" };

  if (!scoreboard || !scoreboard.length || scoreboard.length < 2) {
    _logger.debug("not enough players in team " + team);
    return false;
  }
  for (var i = 0; i < scoreboard.length; i++) {
    var p = scoreboard[i];
    if ((team || p.TEAM) && p.TEAM != team)
      continue;
    data.push("P " + p.STEAM_ID);
    data.push("n " + p.NAME);
    if (team)
      data.push("t " + team);
    data.push("e matches 1");
    data.push("e scoreboardvalid 1");
    data.push("e alivetime " + p.PLAY_TIME);
    data.push("e rank " + p.RANK);
    if (p.RANK == "1" && isWinnerTeam)
      data.push("e wins");
    data.push("e scoreboardpos " + p.RANK);

    mapFields(p, playerMapping, data);
    mapFields(p.DAMAGE, damageMapping, data);
    mapFields(p.MEDALS, medalMapping, data);

    for (var w in weapons) {
      if (!weapons.hasOwnProperty(w)) continue;
      var lname = weapons[w];
      var wstats = p.WEAPONS[lname];
      var kills = wstats && wstats.K;
      if (kills === undefined)
        continue;
      data.push("e acc-" + w + "-cnt-fired " + wstats.S);
      data.push("e acc-" + w + "-cnt-hit " + wstats.H);
      data.push("e acc-" + w + "-frags " + wstats.K);
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
  return true;
}

function mapFields(info, mapping, data) {
  for (var field in mapping) {
    if (!mapping.hasOwnProperty(field)) continue;
    if (field in info)
      data.push("e scoreboard-" + mapping[field] + " " + info[field]);
  }
}

