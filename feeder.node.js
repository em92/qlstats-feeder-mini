/*
 Download new game results from http://www.quakelive.com/tracker/ and add them to the database
*/
'use strict';

var
  fs = require('graceful-fs'),
  async = require('async'),
  request = require('request'),
  log4js = require('log4js'),
  zlib = require('zlib'),
  Q = require('q');

var __dirname; // current working directory (defined by node.js)
var _logger; // log4js logger
var _config; // config data from cfg.json file
var _cookieJar; // www.quakelive.com login cookies
var _adaptivePollDelaySec = 120; // will be reduced to 60 after first (=full) batch. Values are 15,30,60,120
var _lastGameTimestamp = ""; // last timestamp retrieved from live game tracker, used to get next incremental set of games

main();

function main() {
  _logger = log4js.getLogger("ldtracker");
  _logger.setLevel(log4js.levels.INFO);
  var data = fs.readFileSync(__dirname + '/cfg.json');
  _config = JSON.parse(data);
  if (!(_config.loader.saveDownloadedJson || _config.loader.importDownloadedJson)) {
    _logger.error("At least one of loader.saveDownloadedJson or loader.importDownloadedJson must be set in cfg.json");
    process.exit();
  }
  Q.longStackSupport = false;
  loginToQuakeliveWebsite()
    .then(fetchAndProcessJsonInfiniteLoop)
    .fail(function (err) { _logger.error(err.stack); })
    .done(function() { _logger.info("completed"); process.exit(); });
}

//==========================================================================================
// QL live data tracker
//==========================================================================================

function loginToQuakeliveWebsite() {
  var defer = Q.defer();
  _cookieJar = request.jar();
  request({
      uri: "https://secure.quakelive.com/user/login",
      timeout: 10000,
      method: "POST",
      form: { submit: "", email: _config.loader.ql_email, pass: _config.loader.ql_pass },
      jar: _cookieJar
    },
    function(err) {
      if (err) {
        _logger.error("Error logging in to quakelive.com: " + err);
        defer.reject(new Error(err));
      } else {
        _logger.info("Logged on to quakelive.com");
        defer.resolve(_cookieJar);
      }
    });
  return defer.promise;
}

function fetchAndProcessJsonInfiniteLoop() {
  _logger.debug("Fetching data from http://www.quakelive.com/tracker/from/");
  return requestJson()
    .then(processBatch)
    .fail(function (err) { _logger.error("Error processing batch: " + err); })
    .then(sleepBetweenBatches)
    .then(fetchAndProcessJsonInfiniteLoop);
}

function requestJson() {
  var defer = Q.defer();
  request(
    {
      uri: "http://www.quakelive.com/tracker/from/" + _lastGameTimestamp,
      timeout: 10000,
      method: "GET",
      jar: _cookieJar
    },
    function (err, resp, body) {
      if (err)
        defer.reject(new Error(err));
      else
        defer.resolve(body);
    });
  return defer.promise;
}

function processBatch(json) {
  var batch = JSON.parse(json);

  // adapt polling rate
  if (!batch)
	return undefined;
  
  var len = batch.length;
  _adaptivePollDelaySec = len < 100 ? 10 : 1;
  _logger.info("Received " + len + " games. Next fetch in " + _adaptivePollDelaySec + "sec");

  if (len == 0)
    return undefined; // value doesnt matter

  _lastGameTimestamp = batch[0].GAME_TIMESTAMP;
  var tasks = [];
  batch.forEach(function(game) {
    if (_config.loader.saveDownloadedJson)
      tasks.push(saveGameJson(game));
    if (_config.loader.importDownloadedJson)
      tasks.push(processGame(game));
   });
  return Q
    .allSettled(tasks)
    .catch(function (err) { _logger.error(err.stack); });
}

function sleepBetweenBatches() {
  var defer = Q.defer();
  setTimeout(function () { defer.resolve(); }, _adaptivePollDelaySec * 1000);
  return defer.promise;
}

function saveGameJson(game) {
  // JSONs loaded from match profiles contain "mm/dd/yyyy h:MM a" format, live tracker contains unixtime int data
  var GAME_TIMESTAMP = game.GAME_TIMESTAMP; // can be either a number, an Object-number, a string, ... 
  if (GAME_TIMESTAMP.indexOf("/") >= 0) {
    GAME_TIMESTAMP = new Date(GAME_TIMESTAMP).getTime() / 1000;
  }
  var basedir = _config.loader.jsondir;
  var date = new Date(GAME_TIMESTAMP * 1000);
  var dirName1 = basedir + date.getFullYear() + "-" + ("0" + (date.getMonth() + 1)).slice(-2);
  var dirName2 = dirName1 + "/" + ("0" + date.getDate()).slice(-2);
  var filePath = dirName2 + "/" + game.PUBLIC_ID + ".json.gz";
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
    .fail(function (err) { _logger.error("Can't save game JSON: " + err.stack); });
}

function createDir(dir) {
  var defer = Q.defer();
  // fs.mkdir returns an error when the directory already exists
  fs.mkdir(dir, function (err) {
    if (err && err.code != "EEXIST")
      defer.reject(err);
    else
      defer.resolve(dir);
  });
  return defer.promise;
}

function processGame(game) {
  var defer = Q.defer();

  var gt = getGametype(game.GAME_TYPE);
  if (!gt)
    return false;
  
  //if (gt != "ca")
  // return false;
  //saveGameJson(game);
  
  var data = [];
  var serverId = game.OWNER ? game.OWNER : "quakelive." + game.QLS;
  var serverName = game.OWNER ? game.OWNER + ": " + game.SERVER_TITLE : "Public " + game.SERVER_TITLE + " @" + game.QLS;
  data.push("0 " + serverId); // not XonStat standard
  data.push("S " + serverName);
  data.push("I " + game.PUBLIC_ID);
  data.push("G " + gt);
  data.push("M " + game.MAP_NAME_SHORT);
  data.push("O baseq3");
  data.push("V 6"); // CA must be >= 6 
  data.push("R .1");
  //data.push("U 27960"); // port
  data.push("D " + game.GAME_LENGTH);
  
  var allWeapons = { gt: "GAUNTLET", mg: "MG", sg: "SHOTGUN", gl: "GRENADE", rl: "ROCKET", lg: "LIGHTNING", rg: "RAILGUN", pg: "PLASMA", bfg: "BFG", hmg: "HMG" }; 
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
  if (game.SCOREBOARD)
    ok = exportScoreboard(game.SCOREBOARD, 0, true, usedWeapons, data);
  else if (game.RACE_SCOREBOARD)
    ok = exportScoreboard(game.RACE_SCOREBOARD, 0, true, usedWeapons, data);
  else if (game.RED_SCOREBOARD && game.BLUE_SCOREBOARD) {
	var redWon = parseInt(game.TSCORE0) > parseInt(game.TSCORE1);
    ok = exportTeamSummary(game.TEAM_SCOREBOARD[0], 1, data)
    && exportScoreboard(game.RED_SCOREBOARD, 1, redWon, allWeapons, data)
    && exportTeamSummary(game.TEAM_SCOREBOARD[1], 2, data)
    && exportScoreboard(game.BLUE_SCOREBOARD, 2, !redWon, allWeapons, data);
  }
  
  if (!ok)
    return false;

  request({
    uri: "http://localhost:6543/stats/submit",
    timeout: 10000,
    method: "POST",
    headers: { "X-Forwarded-For": "0.0.0.0",  "X-D0-Blind-Id-Detached-Signature":"dummy" },
    body: data.join("\n"),
    jar: _cookieJar
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
  switch(gt) {
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

function exportTeamSummary(info, team, data) {
  var mapping = { CAPTURES: "caps", SCORE: "score", ROUNDS_WON: "rounds" }
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

function exportScoreboard(scoreboard, team, isWinnerTeam, weapons, data) {
  var mapping = { SCORE: "score", KILLS: "kills", DEATHS: "deaths", CAPTURES: "captured", ASSISTS: "returns", THAWS: "revivals", DAMAGE_DEALT: "pushes", DAMAGE_TAKEN: "destroyed" };
  if (!scoreboard || !scoreboard.length || scoreboard.length < 2) {
	_logger.debug("not enough players in team " + team);
    return false;
  }
  for(var i=0; i<scoreboard.length; i++) {
	var p = scoreboard[i];
    data.push("P " + p.PLAYER_NICK);
	data.push("n " + p.PLAYER_NICK);
	if (team)
	  data.push("t " + team);
	data.push("e matches 1");
	data.push("e scoreboardvalid 1");
	data.push("e alivetime " + p.PLAY_TIME);
	data.push("e rank " + p.RANK);
	if (p.RANK == "1" && isWinnerTeam)
	  data.push("e wins");
	data.push("e scoreboardpos " + p.RANK);
	mapFields(p, mapping, data);
	
	for (var w in weapons) {
	  var lname = weapons[w];
	  var kills = p[lname + "_KILLS"];
	  if (kills === undefined)
		continue;
	  data.push("e acc-" + w + "-cnt-fired " + p[lname + "_SHOTS"]);
	  data.push("e acc-" + w + "-cnt-hit " + p[lname + "_HITS"]);
	  data.push("e acc-" + w + "-frags " + p[lname + "_KILLS"]);
	}
  }	
  return true;
}
