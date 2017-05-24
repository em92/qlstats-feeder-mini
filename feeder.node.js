/*
 
 This is a multi-purpose script which can
 - fetch live game and player statistics from Quake Live game server ZeroMQ message queues
 - save the stats to .json.gz files
 - load saved .json.gz stats files for reprocessing
 - transform the stats JSON to XonStat match report format and HTTP POST it to sumbission.py, which will insert it in the database
 - send match data to the glicko gamerating module
 - run an HTTP server with an administration panel to monitor/add/modify the game server list
 - run an HTTP server with API URLs to query and deliver saved .json.gz files and get some live match information
 
 The script monitors changes to the config file and automatically connects to added servers and disconnects from removed servers.

 Reconnecting after network errors is handled internally by ZeroMQ.
 QL servers seem to stop sending ZMQ messages when they are idle for a while, therefore this script reconnects to periodically to idle servers.
 
 The "zmq" node module uses "libzmq", which has a hardcoded limit of 1024 sockets. 3 sockets per ZMQ connection => max 341 ZMQ connections.
 You can either recompile libzmq + node.zmq, or run multiple instances of the feeder and provide different config files with "-c cfg1.json".

 When XonStat's submission.py server is not responding with an "ok", a .json.gz is saved in the "errors" folder.
 
 Command line: feeder [options] [files/dirs...]:
 -c <configfile>:  use the provided config file. If omitted, "cfg.json" is used
 -e:               reprocess .json.gz files from the "errors" folder
 -x:               delete broken .json.gz files
 files/dirs:       list of files and directories to be processed recursively
  
 When no command line options and files are specified, the feeder connects to the configured QL game servers to load live statistics.
 Servers in the config file are specified as "ownername:ip:port/zmqpassword"
*/

"use strict";

var
  fs = require("graceful-fs"),
  request = require("request"),
  log4js = require("log4js"),
  zlib = require("zlib"),
  Q = require("q"),
  express = require("express"),
  http = require("http"),
  StatsConnection = require("./modules/statsconn"),
  semver = require("semver"),
  utils = require("./modules/utils");

var IpPortPassRegex = /^(?:([^:]*):)?((?:[0-9]{1,3}\.){3}[0-9]{1,3}):([0-9]+)(?:\/(.*))?$/; // IP:port/pass

var __dirname; // current working directory (defined by node.js)

var _logger; // log4js logger
var _configFileName = "cfg.json";
var _config; // config data object
var _ignoreConfigChange = false;
var _reloadErrorFiles = false;
var _deleteBrokenFiles = false;
var _gameId = 0;

/**
 * @type Object.<string,StatsConnection> using IP:port as key
 */
var _statsConnections = {}; // dictionary with IP:port => StatsConnection

function main() {
  _logger = log4js.getLogger("feeder");
  //Q.longStackSupport = true; // enable if you have to trace a problem, but it has a HUGE performance penalty
  StatsConnection.setLogger(_logger);

  // checking node version
  if (semver.lt(process.versions.node, "0.11.13")) {
    // "Promise" was introduced with node 0.11.13. No idea why it worked with 0.10.29 for a long time until 2016-12-28
    _logger.warn("node >= 0.11.13 required, but using node v" + process.versions.node);
  }
  
  var filesToProcess = parseCommandLine();

  loadInitialConfig();
  
  if (_reloadErrorFiles)
    return processFilesFromCommandLine([__dirname + "/" + _config.feeder.jsondir + "errors"]);

  if (filesToProcess.length > 0) {
    _logger.setLevel("DEBUG");
    return processFilesFromCommandLine(filesToProcess);
  }

  if (_config.feeder.enabled !== false)
    startFeeder();

  if (_config.webadmin.enabled || _config.webapi.enabled)
    startHttpd();
  return null;
}

/**
 * Parses the command line args, set global variables based on selected switches
 * @returns {string[]} List of files and/or directories to (re)process
 */
function parseCommandLine() {
  var args = process.argv.slice(2);
  
  while (args[0] && args[0][0] == "-") {
    if (args[0] == "-c" && args.length >= 2) {
      _configFileName = args[1];
      args = args.slice(1);
    }
    else if (args[0] == "-g") {
      _gameId = args[1];
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
 * @returns {boolean} true if the config was (re)loaded
 */
function reloadConfig() {
  try {
    if (_ignoreConfigChange) {
      _ignoreConfigChange = false;
      return false;
    }

    var json = fs.readFileSync(__dirname + "/" + _configFileName, { encoding: 'utf8' });
    if (json.substr(-2) == "}}") // hack: sometimes writing the JSON config back to the file generates a double }} at the end of the file
      json = json.substr(0, json.length - 1);
    var config = JSON.parse(json);
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
 * @returns {boolean} true if there were changes to the configuration
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

  if (typeof (_config.feeder.calculateGlicko) == "undefined")
    _config.feeder.calculateGlicko = true;

  if (typeof (_config.webapi.aggregatePanelPorts) == "undefined")
    _config.webapi.aggregatePanelPorts = [];

  return JSON.stringify(_config) != oldConfig;
}

/**
 * Load saved .json[.gz] files for reprosessing
 * @param {string[]} files - list of files/dirs to be processed recursively
 */
function processFilesFromCommandLine(files) {
  return processJsonFiles(files)
    .catch(function(err) { _logger.error(err); })
    .done();
}

/**
 * Starts the thread that maintains QL server ZeroMQ connections and reloads the config file when it was modified externally
 */
function startFeeder() {
  // connect to live zmq stats data feeds from QL game servers
  Q(connectToServerList(_config.feeder.servers))
    .then(function(ok) {
      if (!ok) process.exit(1);
    }).done();

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
        connectToServerList(_config.feeder.servers).done();
    }, 500);
  });
}

/**
 * Starts the HTTP server thread which is used to handle the server admin panel and/or API requests
 */
function startHttpd() {
  var app = express();

  if (!_config.httpd.port)
    return;

  var callbacks = {
    getStatsConnections: function() { return _statsConnections; },
    addServer: addServer,
    removeServer: removeServer,
    writeConfig: writeConfig,
    isTeamGame: isTeamGame
  };

  if (_config.webadmin.enabled) {
    _logger.info("starting webadmin");
    var webadmin = require("./modules/webadmin");
    webadmin.init(_config, app, callbacks);
  }
  
  // WebAPI is always started for internal APIs, public APIs can be disabled through the config file
  _logger.info("starting webapi");
  var webapi = require("./modules/webapi");
  webapi.init(_config, app, callbacks);

  app.listen(_config.httpd.port, _config.httpd.ip);
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
 * @param {string[]} files - files and folders
 * @returns {Promise<boolean>} A promise that will be fulfilled when all files are processed. True when there were no errors.
 */
function processJsonFiles(files) {
  // serialize calls for each file
  return files.reduce(function(chain, file) {
    return chain.then(function (prevOk) {
      return feedJsonFile(file).then(function (ok) {
         if (global.gc) global.gc();
         return ok && prevOk;
      });
    });
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
          return true;
        }

        return Q
          .nfcall(fs.readFile, file)
          .then(function(content) { return file.slice(-3) == ".gz" ? Q.nfcall(zlib.gunzip, content) : content; })
          .then(function(json) {
            var gameData;
            try {
              gameData = JSON.parse(json);
            }
            catch (err) {
              if (_deleteBrokenFiles)
                return Q.nfcall(fs.unlink, file);
              throw err;
            }
            return processGameData(gameData);
          })
          .then(function() {
            if (_reloadErrorFiles)
              return Q.nfcall(fs.unlink, file).then( function() { return true });
            return true;
          })
          .catch(function(err) {
            _logger.error(file.replace(__dirname + "/" + _config.feeder.jsondir, "") + ": " + err);
            return false;
          });
      })
      .catch(function(err) { _logger.error("failed to process " + file + ": " + err) });
  }
}

/**
 * Synchronizes the currently active ZeroMQ connections in _statsConnections with the provided server list.
 * @param {string[]} servers - List of servers with the format owner:ip:port/password
 * @returns {boolean} True if the server list was updated
 */
function connectToServerList(servers) {
  if (!servers.length) {
    _logger.error("There are no servers configured in " + _configFileName);
    return false;
  }

  // libzmq has a limit of max 1024 handles in a select() call. 1024 / 3 (sockets/connection) => 341 max
  // Linux also often has a file handle limit of 1024 (ulimit -n), which is reached even before that (~ 255).
  if (servers.length > 300) {
    _logger.error("Too many servers, maximum allowed is 300 (to stay below the hardcoded libzmq limit).");
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

  if (deferredConnections.length == 0) {
    _statsConnections = newZmqConnections;
    return true;
  }
  
  // load associated game ports from database
  var ret = Q({});
  if (_config.webapi.database) {
    ret = ret.then(function() {
      return utils.dbConnect(_config.webapi.database)
        .then(function(cli) {
          return Q
            .ninvoke(cli, "query", "select hashkey, port from servers")
            .then(function(result) {
              var gamePorts = {};
              result.rows.forEach(function(row) {
                gamePorts[row.hashkey] = row.port;
              });
              return gamePorts;
            })
            .finally(function() { cli.release(); });
        });
    });
  }

  return ret
    .then(function (gamePorts) {
      _statsConnections = newZmqConnections;
      var count = 0;
      try {
        deferredConnections.forEach(function(conn) {
          ++count;
          addServer(conn.owner, conn.ip, conn.port, conn.pass, gamePorts[conn.ip + ":" + conn.port]);
        });
        return true;
      }
      catch (err) {
        _logger.error("Failed creating ZMQ connection #" + count + ": " + err);
        return false;
      }
    });
}

/**
 * Used internally and by the HTTP admin panel to add a server and create a ZeroMQ connection
 * @param {string} owner 
 * @param {string} ip 
 * @param {number} port 
 * @param {string} pass 
 * @param {string} gamePort
 * @returns {?StatsConnection} The new connection object or null if there is already another connection for this server
 */
function addServer(owner, ip, port, pass, gamePort) {
  var addr = ip + ":" + port;
  if (_statsConnections[addr]) {
    _logger.error("Ignoring duplicate connection to " + addr);
    return null;
  }

  var conn = StatsConnection.create(owner, ip, port, pass, onZmqMessageCallback, gamePort);
  conn.connect();
  _statsConnections[addr] = conn;
  return conn;
}

/**
 * Used internally and by the HTTP admin panel to remove a server and shut down the ZeroMQ connection
 * @param {StatsConnection} conn - The StatsConnection object of the server to be removed
 */
function removeServer(conn) {
  conn.disconnect();
  delete _statsConnections[conn.addr];
}

/**
 * Callback function for events on ZeroMQ connections
 * This function keeps track of the number of rounds and the time played by each player per team.
 * PLAYER_STATS.PLAY_TIME unfortunately includes warmup and is useless.
 */
function onZmqMessageCallback(conn, data) {
  var msg = data.toString();
  var obj = JSON.parse(msg);
  var now = new Date().getTime();
  _logger.trace(conn.addr + ": received ZMQ message: " + msg);

  //fs.writeFileSync("temp/" + obj.TYPE.toLowerCase() + ".json", msg);

  if (_config.feeder.trackDuelEvents && conn.gameType == "duel")
    conn.events.push({ time: now, event: obj });

  if (obj.TYPE == "PLAYER_CONNECT")
    setPlayerTeam(conn, obj.DATA, 3).time = now;
  else if (obj.TYPE == "PLAYER_DISCONNECT") {
    var p = conn.players[obj.DATA.STEAM_ID];
    if (p) {
      checkQuitter(conn, obj.DATA.STEAM_ID);
      updatePlayerPlayTime(p);
      p.quit = true;
      p.team = 3;
    }
  }
  else if (obj.TYPE == "PLAYER_SWITCHTEAM") {
    var p = conn.players[obj.DATA.KILLER.STEAM_ID];
    updatePlayerPlayTime(p);
    if ([3,"SPECTATOR"].indexOf(obj.DATA.KILLER.TEAM) >= 0)
      checkQuitter(conn, obj.DATA.KILLER.STEAM_ID);
    setPlayerTeam(conn, obj.DATA.KILLER);
  }
  else if (obj.TYPE == "PLAYER_KILL") {
    setPlayerTeam(conn, obj.DATA.KILLER).dead = false;
    setPlayerTeam(conn, obj.DATA.VICTIM);
  }
  else if (obj.TYPE == "PLAYER_DEATH") {
    setPlayerTeam(conn, obj.DATA.VICTIM).dead = true;
  }
  else if (obj.TYPE == "MATCH_STARTED") {
    onMatchStarted();
    conn.gameType = obj.DATA.GAME_TYPE.toLowerCase();
    conn.events = [ { time: now, event: obj } ];
  }
  else if (obj.TYPE == "ROUND_OVER")
    onRoundOver(obj.DATA);
  else if (obj.TYPE == "PLAYER_STATS") {
    if (!obj.DATA.WARMUP)
      conn.playerStats.push(obj.DATA);
  } else if (obj.TYPE == "MATCH_REPORT") {
    onMatchReport();
    if (_config.feeder.trackDuelEvents && conn.gameType == "duel") {
      var file = _config.feeder.jsondir + "duel/" + obj.DATA.MATCH_GUID + ".json";
      fs.writeFile(file, JSON.stringify(conn.events), function(err) {
          _logger.error("failed to save duel stats file " + file + ": " + err);
        });
    }
  }

  try {
    conn.emitter.emit('zmq', obj);
  }
  catch (err) {
  }

  function onMatchStarted() {
    _logger.debug(conn.addr + ": match started");
    conn.matchStartTime = now;
    conn.matchDuration = 0;
    conn.gameType = (obj.DATA.GAME_TYPE || "").toLowerCase() || null;
    conn.factory = (obj.DATA.FACTORY || "").toLowerCase() || null;
    Object.keys(conn.players).forEach(function (steamid) {
      var p = conn.players[steamid];
      p.time = now;
      p.playTimes = [0, 0, 0]; // time for team free, red, blue
      p.rounds = {}; // dict with round number => team number
      p.dead = false;
    });
    conn.round = 1;
    conn.roundStartTime = now;
    conn.roundStats = [];
    conn.quitters = [];
    conn.roundTimer = setTimeout(function () { roundSnapshot(conn) }, 10 * 1000); // "prepare to fight! round begins in: 10 ... 3, 2, 1, FIGHT!"    
  }
  
  function onRoundOver(data) {
    var duration = Math.round((now - conn.roundStartTime) / 1000);
    
    var roundStats = { TEAMS: { 1: [], 2: [] } };
    if (conn.matchStartTime) {
      roundStats.teamWon = data.TEAM_WON;
      roundStats.roundLength = duration;
      conn.roundStats.push(roundStats);
    }

    conn.matchDuration += duration;
    Object.keys(conn.players).forEach(function (steamid) {
      var p = conn.players[steamid];
      var team = p.rounds[conn.round];
      if (team == 1 || team == 2) {
        p.playTimes[team] += duration; // add time to the team that the player played in this round (could be different team now)
        roundStats.TEAMS[team].push(steamid);
      }
      p.dead = false;
    });
    
    ++conn.round;
    conn.roundStartTime = now;
    conn.roundTimer = setTimeout(function () { roundSnapshot(conn) }, 14 * 1000); // "red won the round! round begins in: 10 ... 3, 2, 1, FIGHT!"    
  }
  
  function checkQuitter(conn, steamid) {
    if (conn.matchStartTime == 0) // match not started or already finished
      return;
    var p = conn.players[steamid];
    if (!p || (p.team != 1 && p.team != 2))
      return;
    var teamSize = [0, 0, 0, 0];
    Object.keys(conn.players).forEach(function (steamid) { teamSize[conn.players[steamid].team]++; });
    if (teamSize[p.team] > teamSize[3 - p.team]) // quitting from the larger team is fine
      return;
    if (teamSize[p.team] + 3 <= teamSize[3 - p.team]) // quitting is also not punished, when the other team has 3 players more (a diff of 2 players can be fixed by 1 player switching teams)
      return;
    conn.quitters.push(steamid);
  }

  function setPlayerTeam(conn, playerData, overrideTeam) {
    var steamid = playerData.STEAM_ID;
    var player = conn.players[steamid];
    if (!player)
      conn.players[steamid] = player = { team: -1, time: now, rounds: {}, quit: false, playTimes:[0,0,0], dead: false };
    var teams = [0, "FREE", 1, "RED", 2, "BLUE", 3, "SPECTATOR"];
    var team = overrideTeam !== undefined ? overrideTeam : playerData.TEAM;
    player.team = Math.floor(teams.indexOf(team) / 2);
    player.name = playerData.NAME;
    player.quit = false;
    player.lastMsg = now;
    return player;
  }
  
  function updatePlayerPlayTime(p) {
    if (!p || p.quit) return;
    
    // for round-based games types the play time is updated when the round is over
    if ("ca,ft,ad".indexOf(conn.gameType || "-") >= 0) return;
    
    // for non-round-based games update playTimes immediately
    if (p.team >= 0 && p.team <= 2)
      p.playTimes[p.team] += Math.round((now - p.time) / 1000);
    
    p.time = now;    
  }

  function roundSnapshot(conn) {
    // this funtion is timed to the "FIGHT!" announcement and take a snapshot of the players who participated in that round
    Object.keys(conn.players).forEach(function(steamid) {
      var p = conn.players[steamid];
      if ((p.team == 1 || p.team == 2) && !p.quit) {
        p.rounds[conn.round] = p.team;
        if (p.team !== p.rounds[conn.round - 1])
          p.time = conn.roundStartTime;
      }
    });
  }
  
  function onMatchReport() {
    if (obj.DATA.ABORTED) {
      _logger.debug(conn.addr + ": match aborted");
      return Q();
    }

    _logger.debug(conn.addr + ": match finished");
    Object.keys(conn.players).forEach(function (steamid) {
      var p = conn.players[steamid];
      updatePlayerPlayTime(p);
    });
    if (conn.round <= 1) // for non-round-based games
      conn.matchDuration = Math.round((now - conn.matchStartTime)/1000);
    
    clearTimeout(conn.roundTimer);
    var stats = {
      serverIp: conn.ip,
      serverPort: conn.port,
      gameEndTimestamp: Math.round(now / 1000),
      matchStats: obj.DATA,
      playerStats: conn.playerStats,
      roundCount: getRoundsInformation(conn),
      playTimes: getPlayTimeInformation(conn),
      roundStats: conn.matchStartTime && conn.round ? conn.roundStats : undefined,
      quitters: conn.quitters
    };
    conn.playerStats = [];
    Object.keys(conn.players).forEach(function (steamid) {
      if (conn.players[steamid].quit || conn.players[steamid].lastMsg + 2 * 3600 * 1000 < now)
        delete conn.players[steamid];
    });
    
    //conn.gameType = null;  // assume it will stay the same, since QL doesn't provide any timely update after map change
    conn.matchStartTime = 0;
    
    // save .json.gz and/or process the data for uploading it to xonstatdb
    var chain = Q();
    if (_config.feeder.saveDownloadedJson)
      chain = chain.then(function() { return saveGameJson(stats).catch(function(err) { _logger.error("failed saving .json.gz: " + err) }) }); 
    if (_config.feeder.importDownloadedJson)
      chain = chain.then(function() { return processGameData(stats).catch(function(err) { _logger.error("failed processing match: " + err) }) });
    return chain;
  }

  function getRoundsInformation(conn) {
    if (!(conn.matchStartTime && conn.round > 1))
      return undefined;
    var playerRounds = Object.keys(conn.players).reduce(function(aggregate, steamid) {
      var p = conn.players[steamid];
      var rounds = p.rounds;
      var count = { r: 0, b: 0 };
      for (var round in rounds) {
        if (!rounds.hasOwnProperty(round)) continue;
        if (rounds[round] == 2)
          ++count.b;
        else
          ++count.r;
      }

      if (count.r || count.b)
        aggregate[steamid] = count;

      return aggregate;
    }, {});
    return { total: conn.round - 1, players: playerRounds };
  }

  function getPlayTimeInformation(conn) {
    if (!conn.matchStartTime)
      return undefined;
    var playTimes = Object.keys(conn.players).reduce(function (aggregate, steamid) {
      var times = conn.players[steamid].playTimes;
      if (times[0] + times[1] + times[2])
        aggregate[steamid] = times.slice();
      return aggregate;
    }, {});
    return { total: conn.matchDuration, players: playTimes };
  }
}

/**
 * Saves the game data object to a .json.gz file. Errors are logged and handled internally 
 * @param {Object} game 
 * @param {boolean} toErrorDir - If true, the file is saved in the "errors" folder, otherwise in a YYYY-MM/DD/ folder
 * @returns {Promise<>} A promise which gets fulfilled when the operation has completed
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
 * @returns {boolean|Promise<Boolean>} 
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
  if (/untracked/i.test(game.matchStats.FACTORY_TITLE)) {
    _logger.debug(addr + ": ignoring untracked game " + game.matchStats.MATCH_GUID);
    return false;
  }

  var gt = game.matchStats.GAME_TYPE.toLowerCase();

  game.playerStats = aggregatePlayerStats();
  if (game.playTimes)
    game.matchStats.GAME_LENGTH = game.playTimes.total;

  // verify minimum number of players in each team (this is for saving stats, ranking has additional requirements)
  var playerCounts = game.playerStats.reduce(function(counts, player) {
    ++counts[player.TEAM || 0];
    return counts;
  }, [0, 0, 0]);
  if (gt == "rr") {
    var count = playerCounts.reduce(function(s, c) { return s + c; }, 0);
    if (count < 4) {
      _logger.debug("only " + count + " player(s) in match, minimum required is 4");
      return false;
    }
  }
  else {
    var minPlayers = { duel: [2, 0, 0], race: [1, 0, 0] };
    var minCounts = minPlayers[gt] || (isTeamGame(gt) ? [0, 2, 2] : [4, 0, 0]);
    for (var i = 0; i <= 2; i++) {
      var min = minCounts[i];
      if (playerCounts[i] < min) {
        _logger.debug("only " + playerCounts[i] + " player(s) in team " + i + ", minimum required is " + min);
        return false;
      }
    }
  }

  var report = createXonstatMatchReport(gt, game);
  if (!report) {
    _logger.error(addr + ": no match report generated for " + game.matchStats.MATCH_GUID);
    return false;
  }

  return postMatchReportToXonstat(addr, game, report)
    .then(function(result) {
      if (!_config.feeder.calculateGlicko)
        return true;
      if (!result.ok || !result.game_id) {
        if (_gameId)
          result = { game_id: _gameId } // this is for running a match data file through the debugger
        else {
          _logger.debug("game could not be rated: " + game.matchStats.MATCH_GUID);
          return false;
        }
      }
      var rating = require("./modules/gamerating");
      return rating.rateSingleGame(result.game_id, game);
    })
    .catch(function(err) {
      if (!_reloadErrorFiles)
        saveGameJson(game, true);
      throw err;
    });

  function aggregatePlayerStats() {
    var partialPlayTimes = game.playerStats.reduce(function (aggregate, data) {
      var key = data.STEAM_ID + "_" + data.TEAM;
      var p = aggregate[key];
      if (!p)
        aggregate[key] = data;
      else
        summarize(p, data);
      return aggregate;
    }, {});

    return Object.keys(partialPlayTimes).map(function(key) {
      var p = partialPlayTimes[key];
      if (game.playTimes && game.playTimes.players[p.STEAM_ID])
        p.PLAY_TIME = game.playTimes.players[p.STEAM_ID][p.TEAM || 0]; // substitute broken QL play time with accurate time
      return p;
    });

    function summarize(t, s) {
      Object.keys(s).forEach(function (key) {
        var val = s[key];
        if (key.indexOf("RANK") >= 0) {
          if ((t[key] || 0) <= 0 || val < t[key])
            t[key] = val;
        }
        else if (key == "TEAM") {
          // keep as-is
        }
        else if (typeof (val) == "number")
          t[key] = (t[key] || 0) + val;
        else if (typeof (val) == "object") {
          if (!t[key])
            t[key] = {};
          summarize(t[key], s[key]);
        }
      });
    }
  }
}

function isTeamGame(gt) {
  return ",ca,tdm,ctf,ft,ad,a&d,dom,1fctf,harvester,".indexOf("," + gt + ",") >= 0;
}

/**
 * Convert the internal game data to the XonStat match report text file format used as HTTP POST body for submission.py
 * @param {string} gt - game type (ffa, ca, duel, ctf, tdm, ft, ...)
 * @param {Object} game - game data object from onZmqMessageCallback or from a .json.gz
 * @returns {string} - match report text
 */
function createXonstatMatchReport(gt, game) {
  var report = [];
  exportMatchInformation(gt, game, report);
  
  var allWeapons = { gt: "GAUNTLET", mg: "MACHINEGUN", sg: "SHOTGUN", gl: "GRENADE", rl: "ROCKET", lg: "LIGHTNING", rg: "RAILGUN", pg: "PLASMA", bfg: "BFG", hmg: "HMG", cg: "CHAINGUN", ng: "NAILGUN", pm: "PROXMINE", gh: "OTHER_WEAPON" };
    
  if (isTeamGame(gt)) {
    var redWon = parseInt(game.matchStats.TSCORE0) > parseInt(game.matchStats.TSCORE1);
    var blueWon = parseInt(game.matchStats.TSCORE0) < parseInt(game.matchStats.TSCORE1);
    exportTeamSummary(gt, game, 1, report);
    exportScoreboard(gt, game, 1, redWon, allWeapons, report);
    exportTeamSummary(gt, game, 2, report);
    exportScoreboard(gt, game, 2, blueWon, allWeapons, report);
  }
  else
    exportScoreboard(gt, game, 0, true, allWeapons, report);
  return report.join("\n");
  
  function exportMatchInformation(gt, game, report) {
    report.push("0 " + game.serverIp); // not XonStat standard
    report.push("1 " + game.gameEndTimestamp); // not XonStat standard
    if (game.roundCount)
      report.push("2 " + game.roundCount.total);
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
      // check if player's TEAM equals to team in function parameter
      // redrover is not team-based gametype, but for some reason player is assigned to a team in match report
      if (gt != "rr" && (team || p.TEAM) && p.TEAM != team)
        continue;
      report.push("P " + p.STEAM_ID);
      report.push("n " + p.NAME);
      if (team)
        report.push("t " + team);
      report.push("e playermodel " + p.MODEL);
      report.push("e matches 1");
      report.push("e scoreboardvalid 1");
      report.push("e alivetime " + Math.min(p.PLAY_TIME, game.matchStats.GAME_LENGTH));
      report.push("e rank " + p.RANK);
      if ((team == 0 && p.RANK == "1") || isWinnerTeam)
        report.push("e wins 1");
      report.push("e scoreboardpos " + p.RANK);
      if (game.roundCount) {
        var rounds = game.roundCount.players[p.STEAM_ID];
        if (rounds)
          report.push("e scoreboard-lives " + (p.TEAM == 1 ? rounds.r : rounds.b));
      }
        
      
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
        report.push("e acc-" + w + "-fired " + wstats.DG);
        report.push("e acc-" + w + "-hit " + wstats.DR);
      }
      
      Object.keys(p.MEDALS).forEach( function(medal_name) {
        var medal_count = p.MEDALS[medal_name].toString();
        report.push("e medal-" + medal_name.toLowerCase() + " " + medal_count);
      });
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
 * @returns {Promise<Object>} - JSON object returned by submission.py
 */
function postMatchReportToXonstat(addr, game, report) {
  var defer = Q.defer();
  request({
      uri: _config.feeder.xonstatSubmissionUrl,
      timeout: 10000,
      method: "POST",
      headers: { "X-D0-Blind-Id-Detached-Signature": "dummy", "Content-Type": "text/plain" },
      body: report
    },
    function(err, response, body) {
      if (err)
        defer.reject(new Error("upload failed: " + game.matchStats.MATCH_GUID + ": " + err));
      else if (response.statusCode != 200)
        defer.reject(new Error("upload failed: " + game.matchStats.MATCH_GUID + ": HTTP " + response.statusCode + " - " + response.statusMessage + "): "));
      else {
        _logger.info("match uploaded successfully: " + game.matchStats.MATCH_GUID);
        defer.resolve(JSON.parse(body));
      }
    });
  return defer.promise;
}

main();
//process.exit(0);
