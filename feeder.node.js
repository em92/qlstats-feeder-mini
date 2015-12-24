/*
 Fetch Quake Live statistics from game server ZeroMQ message queues.
 
 Optionally save the stats to .json.gz files.
 Optionally reformat the JSON to XonStat match report format and send it to sumbission.py via HTTP POST.
 Optionally submit saved .json.gz matches to XonStats (files specified as command line arguments)

 The script monitors changes to the config file and automatically connects to added servers and disconnects from removed servers.

 Reconnecting after network errors is handled internally by ZeroMQ.
 When QL fails and becomes silent, this code will reconnect after an idle timeout.

 When XonStat's submission.py server is not responding with an "ok", a .json.gz is saved in the <jsondir>/errors/ folder.

 The "zmq" node module uses "libzmq", which has a hardcoded limit of 1024 sockets. 3 sockets per ZMQ connection => max 341 ZMQ conns.
 You can either recompile libzmq + node.zmq, or run multiple instances of the feeder and provide different config files with "-c cfg1.json".

 Command line: feeder [options] [files/dirs...]:
 -c <configfile>:  use the provided config file
 -e:               reprocess .json.gz files from "errors" folder
 -x:               delete broken .json.gz files
 files/dirs:       list of files and directories to be processed recursively
*/

"use strict";

var
  fs = require("graceful-fs"),
  request = require("request"),
  log4js = require("log4js"),
  zlib = require("zlib"),
  Q = require("q"),
  express = require("express"),
  http = require("http");

var
  StatsConnection = require("./statsconn");


var IpPortPassRegex = /^(?:([^:]*):)?((?:[0-9]{1,3}\.){3}[0-9]{1,3}):([0-9]+)(?:\/(.*))?$/; // IP:port/pass

var __dirname; // current working directory (defined by node.js)
var _logger; // log4js logger
var _configFileName = "cfg.json";
var _config; // config data
var _statsConnections = {}; // dictionary with IP:port => StatsConnection
var _ignoreConfigChange = false; //
var _reloadErrorFiles = false;
var _deleteBrokenFiles = false;


function main() {
  _logger = log4js.getLogger("feeder");
  Q.longStackSupport = false; // enable if you have to trace a problem, but it has a HUGE performance penalty
  StatsConnection.setLogger(_logger);

  var filesToProcess = parseCommandLine();

  loadInitialConfig();

  if (_reloadErrorFiles)
    return processFilesFromCommandLine([__dirname + "/" + _config.feeder.jsondir + "errors"]);
  
  if (filesToProcess.length > 0)
    return processFilesFromCommandLine(filesToProcess);

  if (_config.feeder.enabled !== false)
    startFeeder();

  if (_config.webadmin.enabled || _config.webapi.enabled)
    startHttpd();
  return null;
}

/**
 * Parses the command line args, set global variables based on selected switches
 * @returns {Array<string>} List of files and/or directories to (re)process
 */
function parseCommandLine() {
  var args = process.argv.slice(2);
  
  while (args[0] && args[0][0] == "-") {
    if (args[0] == "-c" && args.length >= 2) {
      _configFileName = args[1];
      args = args.slice(1);
    }
    else if (args[0] == "-e")
      _reloadErrorFiles = true;
    else if (args[0] == "-x")
      _deleteBrokenFiles = true;
    else {
      _logger.error("Invalid command line option: " + args[0]);
      process.exit(1);
    }
    args = args.slice(1);
  }
  return args;
}

/**
 * Load the config file specified in _configFileName
 */
function loadInitialConfig() {
  if (!reloadConfig()) {
    _logger.error("Unable to load config file " + _configFileName);
    process.exit(1);
  }
  if (upgradeConfigVersion())
    fs.writeFileSync(__dirname + "/" + _configFileName, JSON.stringify(_config, null, "  "));
}

/**
 * Try to (re-)load the configuration JSON file and store the object in _config
 * @returns {Boolean} true if the config was (re)loaded
 */
function reloadConfig() {
  try {
    if (_ignoreConfigChange) {
      _ignoreConfigChange = false;
      return false;
    }

    var config = JSON.parse(fs.readFileSync(__dirname + "/" + _configFileName));
    if (!(config.feeder.saveDownloadedJson || config.feeder.importDownloadedJson)) {
      _logger.error("At least one of feeder.saveDownloadedJson or feeder.importDownloadedJson must be set in " + _configFileName);
      return false;
    }

    _config = config;
    _logger.setLevel(_config.feeder.logLevel || log4js.levels.INFO);
    _logger.info("Reloaded modified " + _configFileName);
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

/**
 * Upgrade older config data to current format
 * @returns {Boolean} true if there were changes to the configuration
 */
function upgradeConfigVersion() {
  var oldConfig = JSON.stringify(_config);

  if (!_config.httpd && _config.webadmin) {
    _config.httpd = { enabled: _config.webadmin.enabled, port: _config.webadmin.port };
    delete _config.webadmin.port;
  }
  else if (!_config.httpd)
    _config.httpd = { port: 8081 };

  if (_config.webadmin)
    delete _config.webadmin.database;
  else
    _config.webadmin = { enabled: false, logLevel: "INFO" };
  if (!_config.webadmin.logLevel)
    _config.webadmin.logLevel = "INFO";

  if (!_config.webapi)
    _config.webapi = { enabled: false, logLevel: "INFO", database: "postgres://xonstat:xonstat@localhost/xonstatdb" };

  if (!_config.feeder.xonstatSubmissionUrl) {
    var port = _config.feeder.xonstatPort;
    delete _config.feeder.xonstatPort;
    _config.feeder.xonstatSubmissionUrl = "http://localhost:" + port + "/stats/submit";
  }

  return JSON.stringify(_config) != oldConfig;
}

/**
 * Load saved .json[.gz] files for reprosessing
 * @param {Array[string]} args - list of files to be processed (ignored if _reloadErrorFiles is true)
 */
function processFilesFromCommandLine(files) {
  processJsonFiles(files)
    .catch(function(err) { _logger.error(err); })
    .done();
}

/**
 * Starts the thread that maintains QL server ZeroMQ connections and reloads the config file when it was modified externally
 */
function startFeeder() {
  // connect to live zmq stats data feeds from QL game servers
  if (!connectToServerList(_config.feeder.servers))
    process.exit(1);

  _logger.info("starting feeder");

  // setup automatic config file reloading when the file changes
  var timer;
  fs.watch(__dirname + "/" + _configFileName, function() {
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

/**
 * Starts the HTTP server thread which is used to handle the server admin panel and/or API requests
 */
function startHttpd() {
  var app = express();

  if (_config.webadmin.enabled) {
    _logger.info("starting webadmin");
    var webadmin = require("./webadmin");
    webadmin.init(_config, app, {
      getStatsConnections: function() { return _statsConnections; },
      addServer: addServer,
      removeServer: removeServer,
      writeConfig: writeConfig
    });
  }

  if (_config.webapi.enabled) {
    _logger.info("starting webapi");
    var webapi = require("./webapi");
    webapi.init(_config, app);
  }

  app.listen(_config.httpd.port);
}

/**
 * Write the current config settings fron _config back to the config file in _configFileName.
 * Internally used when upgrading config file formats or by the HTTP admin panel when servers were added/modified
 */
function writeConfig() {
  _config.feeder.servers = [];
  for (var addr in _statsConnections) {
    if (!_statsConnections.hasOwnProperty(addr)) continue;
    var conn = _statsConnections[addr];
    _config.feeder.servers.push(conn.owner + ":" + addr + "/" + conn.pass);
  }
  _config.feeder.servers.sort();

  _ignoreConfigChange = true;
  fs.writeFile(__dirname + "/" + _configFileName, JSON.stringify(_config, null, 2));
}

/**
 * Recursively load and process all provided .json[.gz] files and folders
 * @param {Array[string]} files - files and folders
 * @returns {Q promise[Boolean]} A promise that will be fulfilled when all files are processed. True when there were no errors.
 */
function processJsonFiles(files) {
  // serialize calls for each file
  return files.reduce(function(chain, file) {
    return chain.then(function(ok) { return ok & feedJsonFile(file); }); // single & to prevent short-circuit evaluation
  }, Q(true));

  function feedJsonFile(file) {
    return Q
      .nfcall(fs.stat, file)
      .then(function(stats) {
        if (stats.isDirectory()) {
          return Q
            .nfcall(fs.readdir, file)
            .then(function(direntries) {
              return processJsonFiles(direntries.map(function(direntry) { return file + "/" + direntry; }));
            });
        }

        if (!file.match(/.json(.gz)?$/)) {
          _logger.warn("Skipping file (not *.json[.gz]): " + file);
          return Q(true);
        }

        return Q
          .nfcall(fs.readFile, file)
          .then(function(content) { return file.slice(-3) == ".gz" ? Q.nfcall(zlib.gunzip, content) : content; })
          .then(function(json) {
            var gameData;
            try { gameData = JSON.parse(json); }
            catch (err) {
              if (_deleteBrokenFiles)
                return Q.nfcall(fs.unlink, file).then(Q());
              throw err;
            }
            return processGameData(gameData);
          })
          .then(function() {
            if (_reloadErrorFiles)
              return Q.nfcall(fs.unlink, file).then(Q(true));
            return true;
          })
          .catch(function(err) {
            _logger.error(file.replace(__dirname + "/" + _config.feeder.jsondir, "") + ": " + err);
            return false;
          });
      });
  }
}

/**
 * Synchronizes the currently active ZeroMQ connections in _statsConnections with the provided server list.
 * @param {Array[string]} servers - List of servers with the format owner:ip:port/password
 * @returns {Boolean} True if the server list was updated
 */
function connectToServerList(servers) {
  if (!servers.length) {
    _logger.error("There are no servers configured in " + _configFileName);
    return false;
  }

  // libzmq has a limit of max 1024 handles in a select() call. 1024 / 3 (sockets/connection) => 341 max
  // Linux also often has a file handle limit of 1024 (ulimit -n), which is reached even before that (~ 255).
  if (servers.length > 250) {
    _logger.error("Too many servers, maximum allowed is 250 (to stay below the hardcoded libzmq limit).");
    return false;
  }

  // copy current connection dictionary
  var oldZmqConnections = _statsConnections;
  for (var addr in _statsConnections) {
    if (_statsConnections.hasOwnProperty(addr))
      oldZmqConnections[addr] = _statsConnections[addr];
  }

  // create a new dictionary with the zmq connections for the new server list.
  // after the loop oldZmqConnections only contains servers which are no longer used
  var newZmqConnections = {};
  var deferredConnections = [];
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
    conn = oldZmqConnections[addr];
    if (conn && pass == conn.pass) {
      // unchanged, existing connection
      conn.owner = owner;
      delete oldZmqConnections[addr];
      newZmqConnections[addr] = conn;
    }
    else {
      // ZMQ as a very low hardcoded limit on how many connections it can handle.
      // Therefore we defer creating new connections until old connections have been cleaned up
      deferredConnections.push({ owner: owner, ip: ip, port: port, pass: pass });
    }
  }

  // shut down connections to servers which are no longer in the config
  for (var addr in oldZmqConnections) {
    if (!oldZmqConnections.hasOwnProperty(addr)) continue;
    _logger.info(addr + ": disconnected. Server was removed from config.");
    conn = oldZmqConnections[addr];
    removeServer(conn);
  }

  _statsConnections = newZmqConnections;

  var count = 0;
  try {
    deferredConnections.forEach(function(conn) {
      ++count;
      addServer(conn.owner, conn.ip, conn.port, conn.pass);
    });
  }
  catch (err) {
    _logger.error("Failed creating ZMQ connection #" + count + ": " + err);
    return false;
  }

  return true;
}

/**
 * Used internally and by the HTTP admin panel to add a server and create a ZeroMQ connection
 * @param {String} owner 
 * @param {String} ip 
 * @param {Number} port 
 * @param {String} pass 
 * @returns {Object StatsConnection|null} The new connection object or null if there is already another connection for this server
 */
function addServer(owner, ip, port, pass) {
  var addr = ip + ":" + port;
  if (_statsConnections[addr]) {
    _logger.error("Ignoring duplicate connection to " + addr);
    return null;
  }

  var conn = StatsConnection.create(owner, ip, port, pass, onZmqMessageCallback);
  conn.connect();
  _statsConnections[addr] = conn;
  return conn;
}

/**
 * Used internally and by the HTTP admin panel to remove a server and shut down the ZeroMQ connection
 * @param {Object StatsConnection} conn - The StatsConnection object of the server to be removed
 */
function removeServer(conn) {
  conn.disconnect();
  delete _statsConnections[conn.addr];
}

/**
 * Callback function for events on ZeroMQ connections
 */
function onZmqMessageCallback(conn, data) {
  var msg = data.toString();
  var obj = JSON.parse(msg);
  _logger.trace(conn.addr + ": received ZMQ message: " + obj.TYPE);
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

    // save .json.gz and/or process the data for uploading it to xonstatdb
    var tasks = [];
    if (_config.feeder.saveDownloadedJson)
      tasks.push(saveGameJson(stats));
    if (_config.feeder.importDownloadedJson)
      tasks.push(processGameData(stats));
    Q
      .allSettled(tasks)
      .catch(function(err) { _logger.error(err.stack); });
  }
}

/**
 * Saves the game data object to a .json.gz file. Errors are logged and handled internally 
 * @param {Object} game 
 * @param {Boolean} toErrorDir - If true, the file is saved in the "errors" folder, otherwise in a YYYY-MM/DD/ folder
 * @returns {Promise[undefined]} A promise which gets fulfilled when the operation has completed
 */
function saveGameJson(game, toErrorDir) {
  var basedir = _config.feeder.jsondir;
  var date = new Date(game.gameEndTimestamp * 1000);
  var dirName1 = toErrorDir ? basedir : basedir + date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2);
  var dirName2 = toErrorDir ? basedir + "errors" : dirName1 + "/" + ("0" + date.getDate()).slice(-2);
  var filePath = dirName2 + "/" + game.matchStats.MATCH_GUID + ".json.gz";
  _logger.debug("saving JSON: " + filePath);
  return createDir(dirName1)
    .then(createDir(dirName2))
    .then(function() { return Q.nfcall(zlib.gzip, JSON.stringify(game)); })
    .then(function(gzip) { return Q.nfcall(fs.writeFile, filePath, gzip); })
    .fail(function(err) { _logger.error("Can't save game JSON: " + err.stack); });

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
}

/**
 * (re-)process game data: validate game data, transform JSON to xonstat match report text format and post it to submission.py
 * @param {Object} game - game data from onZmqMessageCallback or from a loaded .json[.gz] file
 * @returns {Boolean|Promise<Boolean>} 
 *   false when the game doesn't qualify to be uploaded to xonstat, 
 *   Promise<Boolean>==true when the match was successfully uploaded, 
 *   Promise with exception when there was an error in the upload or server side processing.
 */
function processGameData(game) {
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

  // verify minimum number of players in each team (this is for saving stats, ranking has additional requirements)
  var playerCounts = game.playerStats.reduce(function(counts, player) {
    ++counts[player.TEAM || 0];
    return counts;
  }, [0, 0, 0]);
  const minPlayers = { ffa: [4, 0, 0], duel: [2, 0, 0], ca: [0, 2, 2], ctf: [0, 2, 2], tdm: [0, 2, 2], ft: [0, 2, 2] };
  for (var i = 0; i <= 2; i++) {
    var min = minPlayers[gt][i];
    if (playerCounts[i] < min) {
      _logger.debug("only " + playerCounts[i] + " player(s) in team " + i + ", minimum required is " + min);
      return false;
    }
  }

  var report = createXonstatMatchReport(gt, game);
  return postMatchReportToXonstat(addr, game, report)
    .then(function(success) {
      if (!success && !_reloadErrorFiles)
        saveGameJson(game, true);
      if (success)
        return true;
      throw new Error("failed to upload " + game.gameStats.MATCH_GUID);
    });
}

/**
 * Convert the internal game data to the XonStat match report text file format used as HTTP POST body for submission.py
 * @param {string} gt - game type (ffa, ca, duel, ctf, tdm, ft, ...)
 * @param {Object} game - game data object from onZmqMessageCallback or from a .json.gz
 * @returns {String} - match report text
 */
function createXonstatMatchReport(gt, game) {
  var report = [];
  exportMatchInformation(gt, game, report);
  
  var allWeapons = { gt: "GAUNTLET", mg: "MACHINEGUN", sg: "SHOTGUN", gl: "GRENADE", rl: "ROCKET", lg: "LIGHTNING", rg: "RAILGUN", pg: "PLASMA", bfg: "BFG", hmg: "HMG", cg: "CHAINGUN", ng: "NAILGUN", pm: "PROXMINE", gh: "OTHER_WEAPON" };
  
  if ("ffa,duel,race".indexOf(gt) >= 0)
    exportScoreboard(gt, game, 0, true, allWeapons, report);
  else if ("ca,tdm,ctf,ft".indexOf(gt) >= 0) {
    var redWon = parseInt(game.matchStats.TSCORE0) > parseInt(game.matchStats.TSCORE1);
    var blueWon = parseInt(game.matchStats.TSCORE0) < parseInt(game.matchStats.TSCORE1);
    exportTeamSummary(gt, game, 1, report);
    exportScoreboard(gt, game, 1, redWon, allWeapons, report);
    exportTeamSummary(gt, game, 2, report);
    exportScoreboard(gt, game, 2, blueWon, allWeapons, report);
  } 
  return report.join("\n");

  function exportMatchInformation(gt, game, report) {
    report.push("0 " + game.serverIp); // not XonStat standard
    report.push("1 " + game.gameEndTimestamp); // not XonStat standard
    report.push("S " + game.matchStats.SERVER_TITLE);
    report.push("I " + game.matchStats.MATCH_GUID);
    report.push("G " + gt);
    report.push("M " + game.matchStats.MAP);
    report.push("O " + game.matchStats.FACTORY);
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
  }
  
  function exportTeamSummary(gt, game, team, data) {
    var mapping = { CAPTURES: "caps", SCORE: "score", ROUNDS_WON: "rounds" };
    var matchstats = game.matchStats;
    var score = matchstats["TSCORE" + (team - 1)];
    var info = {};
    if (gt == "ctf")
      info.CAPTURES = score;
    else if (gt == "ca" || gt == "ft")
      info.ROUNDS_WON = score;
    else //if (gt == "tdm")
      info.SCORE = score;
    
    data.push("Q team#" + team);
    mapFields(info, mapping, data);
  }
  
  function mapFields(info, mapping, data) {
    for (var field in mapping) {
      if (!mapping.hasOwnProperty(field)) continue;
      if (field in info)
        data.push("e scoreboard-" + mapping[field] + " " + info[field]);
    }
  }
}

/**
 * Send the xonstat match report to submission.py with a HTTP POST
 * @param {string} addr - Server address as ip:port
 * @param {Object} game - Game data which in case of an error will be saved as .json.gz in the "errors" folder for later reprocessing
 * @param {string} report - The xonstat match report
 * @returns {Promise<Boolean>} - true when the data was successfully posted an processed by submission.py
 */
function postMatchReportToXonstat(addr, game, report) {
  var defer = Q.defer();
  request({
      uri: _config.feeder.xonstatSubmissionUrl,
      timeout: 10000,
      method: "POST",
      headers: { "X-D0-Blind-Id-Detached-Signature": "dummy" },
      body: report
    },
    function(err, response) {
      if (err)
        defer.reject(new Error("upload failed: " + game.matchStats.MATCH_GUID + ": " + err));
      else if (response.statusCode != 200)
        defer.reject(new Error("upload failed: " + game.matchStats.MATCH_GUID + ": HTTP " + response.statusCode + " - " + response.statusMessage + "): "));
      else {
        _logger.info("match uploaded successfully: " + game.matchStats.MATCH_GUID);
        defer.resolve(true);
      }
    });
  return defer.promise;
}

main();