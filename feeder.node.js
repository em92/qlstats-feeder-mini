/*
 Download new game results from http://www.quakelive.com/tracker/ and add them to the database
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

var __dirname; // current working directory (defined by node.js)
var _logger; // log4js logger
var _config; // config data from cfg.json file
var _adaptivePollDelaySec = 120; // will be reduced to 60 after first (=full) batch. Values are 15,30,60,120

main();

function main() {
  _logger = log4js.getLogger("ldtracker");
  _logger.setLevel(log4js.levels.DEBUG);
  var data = fs.readFileSync(__dirname + "/cfg.json");
  _config = JSON.parse(data);
  if (!(_config.loader.saveDownloadedJson || _config.loader.importDownloadedJson)) {
    _logger.error("At least one of loader.saveDownloadedJson or loader.importDownloadedJson must be set in cfg.json");
    process.exit();
  }
  Q.longStackSupport = false;

  if (process.argv.length > 2) {
    for (var i = 2; i < process.argv.length; i++)
      feedJsonFile(process.argv[i]);
  }
  else
    connectToServerStatsZmq();
}

//==========================================================================================
// QL stats data tracker
//==========================================================================================

function feedSampleData() {
  var playerStats = [];
  playerStats.push(JSON.parse(fs.readFileSync(__dirname + "/sample-data/playerstats1.json")).DATA);
  playerStats.push(JSON.parse(fs.readFileSync(__dirname + "/sample-data/playerstats2.json")).DATA);
  var matchStats = JSON.parse(fs.readFileSync(__dirname + "/sample-data/matchreport.json")).DATA;

  var stats = {
    serverIp: "127.0.0.1",
    serverPort: 27960,
    gameEndTimestamp: new Date().getTime() / 1000,
    matchStats: matchStats,
    playerStats: playerStats
  }
  processMatch(stats);
}

function feedJsonFile(file) {
  if (!file.match(/.json(.gz)?$/))
    return;
  var isGzip = file.slice(-3) == ".gz";
  _logger.debug("Loading " + file);
  var data = fs.readFileSync(file);
  if (isGzip)
    data = zlib.gunzipSync(data);

  var game = JSON.parse(data);
  if (game.matchStats.MATCH_GUID)
    processGame(game);
  else
    _logger.warn(file + ": no MATCH_GUID in json data. File was ignored.");
}


function connectToServerStatsZmq() {
  var regex = /^((?:[0-9]{1,3}\.){3}[0-9]{1,3}):([0-9]+)(?:\/(.+))?$/;

  for (var i = 0; i < _config.loader.servers.length; i++) {
    var server = _config.loader.servers[i];
    var match = regex.exec(server);
    if (!match)
      continue;

    _logger.info("Connecting to " + server);

    var sub = zmq.socket("sub");
    if (match[3]) {
      sub.sap_domain = "stats";
      sub.plain_username = "stats";
      sub.plain_password = match[3];
    }
    sub.connect("tcp://" + server);
    sub.subscribe("");

    (function (sub, ip, port) {
      var context = { addr: ip + ":" + port, ip: ip, port: port, playerStats: [] }
      sub.on("message", function(data) { onZmqMessage(context, data) });
    }) (sub, match[1], match[2]);
  }
}

function onZmqMessage(context, data) {
  var msg = data.toString();
  var obj = JSON.parse(msg);
  _logger.debug(context.addr + ": received ZMQ message: " + obj.TYPE);
  if (obj.TYPE == "MATCH_STARTED") {
    context.matchStarted = true;
  }
  else if (obj.TYPE == "PLAYER_STATS") {
    if (context.matchStarted)
      context.playerStats.push(obj.DATA);
  }
  else if (obj.TYPE == "MATCH_REPORT") {
    var stats = {
      serverIp: context.ip,
      serverPort: context.port,
      gameEndTimestamp: new Date().getTime() / 1000,
      matchStats: obj.DATA,
      playerStats: context.playerStats
    };
    context.playerStats = [];
    if (context.matchStarted) {
      context.matchStarted = false;
      processMatch(stats);
    }
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

function saveGameJson(game) {
  var GAME_TIMESTAMP = game.gameEndTimestamp;
  var basedir = _config.loader.jsondir;
  var date = new Date(GAME_TIMESTAMP * 1000);
  var dirName1 = basedir + date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2);
  var dirName2 = dirName1 + "/" + ("0" + date.getDate()).slice(-2);
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
  if (!gt)
    return false;

  //if (gt != "ca")
  // return false;
  //saveGameJson(game);

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
  var usedWeapons = allWeapons;
  /*
  var usedWeapons = {};
  for (var w in allWeapons) {
    for (var i=0; i<game.SCOREBOARD.length; i++) {
    if (parseInt(game.SCOREBOARD[i][allWeapons[w] + "_SHOTS"]) || parseInt(game.SCOREBOARD[i][allWeapons[w] + "_KILLS"])) {
    usedWeapons[w] = allWeapons[w];
    }
  }
  }
  */
  var ok;
  if ("ffa,duel".indexOf(gt) >= 0)
    ok = exportScoreboard(game.playerStats, 0, true, usedWeapons, data);
  //else if (game.RACE_SCOREBOARD)
  //  ok = exportScoreboard(game.RACE_SCOREBOARD, 0, true, usedWeapons, data);
  else if ("ca,tdm,ctf,ft".indexOf(gt) >= 0) {
    var redWon = parseInt(game.matchStats.TSCORE0) > parseInt(game.matchStats.TSCORE1);
    ok = exportTeamSummary(gt, game.matchStats, 1, data)
    && exportScoreboard(game.playerStats, 1, redWon, allWeapons, data)
    && exportTeamSummary(gt, game.matchStats, 2, data)
    && exportScoreboard(game.playerStats, 2, !redWon, allWeapons, data);
  }

  if (!ok)
    return false;

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
        defer.reject(new Error(err));
      } else {
        _logger.debug("Successfully posted data to QLstats");
        defer.resolve();
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
    _logger.debug("unsupported game type: " + gt);
    return undefined;
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
    if (p.TEAM != team)
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
    if (field in info)
      data.push("e scoreboard-" + mapping[field] + " " + info[field]);
  }
}

