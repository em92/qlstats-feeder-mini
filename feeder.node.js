/*
 Fetch Quake Live statistics from game server ZeroMQ message queues.
 
 Optionally save the stats to .json.gz files.
 Optionally reformat the JSON to XonStat match report format and send it to sumbission.py via HTTP POST.
 Optionally submit saved .json.gz matches to XonStats (files specified as command line arguments)

 The script monitors changes to the config file and automatically connects to added servers and disconnects from removed servers.

 Reconnecting after network errors is handled internally by ZeroMQ.
 When QL fails and becomes silent, this code will reconnect after an idle timeout.

 When XonStat's submission.py server is not responding with an "ok", a .json.gz is saved in the <jsondir>/errors/ folder.

*/

"use strict";

var
  fs = require("graceful-fs"),
  request = require("request"),
  log4js = require("log4js"),
  zlib = require("zlib"),
  Q = require("q");

var
  StatsConnection = require("./statsconn"),
  webadmin = require ("./webadmin");


var IpPortPassRegex = /^(?:([^:]*):)?((?:[0-9]{1,3}\.){3}[0-9]{1,3}):([0-9]+)(?:\/(.*))?$/;  // IP:port/pass

var __dirname; // current working directory (defined by node.js)
var _logger; // log4js logger
var _config; // config data from cfg.json
var _statsConnections = {}; // dictionary with IP:port => StatsConnection
var _ignoreConfigChange = false; //
var _reloadErrorFiles = false;


function main() {
  _logger = log4js.getLogger("feeder");
  Q.longStackSupport = false; // enable if you have to trace a problem, but it has a HUGE performance penalty
  StatsConnection.setLogger(_logger);  

  reloadConfig();

  
  if (process.argv.length > 2) {
    var files;
    if (process.argv[2] == "-e") {
      _reloadErrorFiles = true;
      files = [__dirname + "/" + _config.feeder.jsondir + "errors"];
    }
    else
      files = process.argv.slice(2);

    // process saved .json[.gz] files specified on the command line (allows recursion into directories)
    loadJsonFiles(files)
      .catch(function (err) { _logger.error(err); })
      .done();
    return;
  } 

  // connect to live zmq stats data feeds from QL game servers
  connectToServerList(_config.feeder.servers);

  // setup automatic config file reloading when the file changes
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


  // start HTTP server with Server Admin Panel and API URLs
  if (_config.webadmin.enabled) {
    webadmin.setFeeder({
      getStatsConnections: function() { return _statsConnections; },
      connectServer: connectServer,
      writeConfig: writeConfig
    });

    webadmin.startHttpd(_config);
  }
}

function reloadConfig() {
  try {
    if (_ignoreConfigChange) {
      _ignoreConfigChange = false;
      return false;
    }

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

function writeConfig() {
  _config.feeder.servers = [];
  for (var addr in _statsConnections) {
    if (!_statsConnections.hasOwnProperty(addr)) continue;
    var conn = _statsConnections[addr];
    _config.feeder.servers.push(conn.owner + ":" + addr + "/" + conn.pass);
  }

  _ignoreConfigChange = true;
  fs.writeFile(__dirname + "/cfg.json", JSON.stringify(_config, null, 2));
}

function loadJsonFiles(files) {
  // serialize calls for each file
  return files.reduce(function(chain, file) {
    return chain.then(function() { return feedJsonFile(file); });
  }, Q());

  function feedJsonFile(file) {
    return Q
      .nfcall(fs.stat, file)
      .then(function(stats) {
        if (stats.isDirectory()) {
          return Q
            .nfcall(fs.readdir, file)
            .then(function(direntries) {
              return loadJsonFiles(direntries.map(function (direntry) { return file + "/" + direntry; }));
            });
        }

        if (!file.match(/.json(.gz)?$/)) {
          _logger.warn("Skipping file (not *.json[.gz]): " + file);
          return Q();
        }

        _logger.info("Loading " + file);
        return Q
          .nfcall(fs.readFile, file)
          .then(function(content) { return file.slice(-3) == ".gz" ? Q.nfcall(zlib.gunzip, content) : content; })
          .then(function (json) { return processGame(JSON.parse(json)) })
          .then(function(success) {
            if (_reloadErrorFiles && success)
              return Q.nfcall(fs.unlink, file);
            else
              return success;
          })
          .catch(function(err) { _logger.error(file + ": " + err)});
      });
  }
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

    var owner = match[1];
    var ip = match[2];
    var port = match[3];
    var pass = match[4];
    var addr = ip + ":" + port;
    conn = _statsConnections[addr];
    if (conn && pass == conn.pass) {
      conn.owner = owner;
      delete _statsConnections[addr];
    }
    else
      conn = connectServer(owner, ip, port, pass);
    
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

function connectServer(owner, ip, port, pass) {
  var conn = StatsConnection.create(owner, ip, port, pass, onZmqMessageCallback);
  conn.connect();
  return conn;
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
    ok = exportScoreboard(gt, game, 0, true, allWeapons, report);
  else if ("ca,tdm,ctf,ft".indexOf(gt) >= 0) {
    var redWon = parseInt(game.matchStats.TSCORE0) > parseInt(game.matchStats.TSCORE1);
    ok = exportTeamSummary(gt, game, 1, report)
      && exportScoreboard(gt, game, 1, redWon, allWeapons, report)
      && exportTeamSummary(gt, game, 2, report)
      && exportScoreboard(gt, game, 2, !redWon, allWeapons, report);
  }

  if (!ok) {
    _logger.info(addr + ": match doesn't meet requirements: " + game.matchStats.MATCH_GUID);
    return false;
  }

  return postMatchReportToXonstat(addr, game, report.join("\n"));
}

function exportMatchInformation(gt, game, report) {
  report.push("0 " + game.serverIp); // not XonStat standard
  report.push("1 " + game.gameEndTimestamp); // not XonStat standard
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

function exportScoreboard(gt, game, team, isWinnerTeam, weapons, report) {
  var playerMapping = { SCORE: "score", KILLS: "kills", DEATHS: "deaths" };
  var damageMapping = { DEALT: "pushes", TAKEN: "destroyed" };
  var medalMapping = { CAPTURES: "captured", ASSISTS: "returns", DEFENDS: "drops" };
  var scoreboard = game.playerStats;

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
    report.push("e alivetime " + Math.min(p.PLAY_TIME, game.matchStats.GAME_LENGTH));
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

function exportTeamSummary(gt, game, team, data) {
  var mapping = { CAPTURES: "caps", SCORE: "score", ROUNDS_WON: "rounds" };
  var matchstats = game.matchStats;
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
        if (!_reloadErrorFiles)
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
