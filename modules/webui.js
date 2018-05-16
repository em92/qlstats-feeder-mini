var
  bodyParser = require("body-parser"),
  fs = require("graceful-fs"),
  log4js = require("log4js"),
  passport = require("passport"),
  SteamStrategy = require("passport-steam").Strategy,
  session = require("express-session"),
  pg = require("pg"),
  Q = require("q"),
  utils = require("./utils");

exports.init = init;
exports.deletePlayerBySteamId = deletePlayerBySteamId;

var _logger = log4js.getLogger("webui");
var _config;

// interface for communication with the feeder.node.js module
var _feeder = {
  // no callbacks needed
};


function init(config, app, feeder) {
  _config = config;
  _feeder = feeder;
  _logger.setLevel(config.webui.logLevel || "INFO");

  if (!_config.webui.steamAuth || !_config.webui.steamAuth.apiKey) {
    _logger.warn("webui not started due to missing webui.steamAuth.apiKey");
    return;
  }

  var express = require("express");
  initSteamAuthPages(express, app);
}

function initSteamAuthPages(express, app) {
  // setup Steam OpenID 2.0 authenticator
  passport.serializeUser(function (user, done) { done(null, user); });
  passport.deserializeUser(function (obj, done) { done(null, obj); });
  passport.use(new SteamStrategy(_config.webui.steamAuth,
    function (identifier, profile, done) {
      process.nextTick(function () {
        profile.identifier = identifier;
        return done(null, profile);
      });
    }
  ));

  var prefix = _config.webui.urlprefix;

  app.use(prefix, express.static(__dirname + "/../htdocs"));

  app.set("views", __dirname + "/../views");
  app.set("view engine", "ejs");
  app.use(session({
    secret: _config.webui.sessionSecret,
    name: "Steam login session",
    resave: true,
    saveUninitialized: true
  }));
  app.use(passport.initialize());
  app.use(passport.session());


  app.get(prefix + "/login", function (req, res) {
    res.render("login", { user: req.user, conf: _config.webui });
  });

  app.get(prefix + "/auth/steam",
    passport.authenticate("steam", { failureRedirect: prefix + "/login" }),
    function (req, res) {
      // will never be executed due to automatic redirect
    });

  app.get(prefix + "/auth/steam/return",
    passport.authenticate("steam", { failureRedirect: prefix + "/login" }),
    function (req, res) {
      // "/my" is a paster web page that shows the "webui" web pages in an iframe, if the user is logged in.
      res.redirect("/my");
    });

  app.get(prefix + "", ensureAuthenticated, function(req, res) { renderAccountPage(req, res, ""); });

  app.post(prefix + "", ensureAuthenticated, function (req, res) {
    // store posted user preferences
    saveUserSettings(req, res)
      .then(function(msg) { renderAccountPage(req, res, msg); })    
      .done();
  });

  app.get(prefix + "/logout", function (req, res) {
    // log out, close the iframe and send the user back to the site's start page
    if (req.user) 
      req.logout();
    res.send("<html><head><script>window.parent.location.replace('/');</script></head></html>");
  });

  app.get(prefix + "/user",
    // API function used by paster web pages to get access to the steam user information
    function (req, res) {
      if (req.user)
        req.user.strippedNick = strippedNick(req.user.displayName);
      res.json(req.user || {});
    });

  app.get(prefix + "/privacy_policy", function(req, res) { res.render("privacy_policy"); });
}

function ensureAuthenticated(req, res, next) {
  if (req.isAuthenticated()) { return next(); }
  res.redirect(_config.webui.urlprefix + '/login');
}

function renderAccountPage(req, res, msg) {
  loadUserSettings(req)
    .then(function (player) {
      res.render("account", { user: req.user, conf: _config.webui, saved: false, player: player, errorMsg: msg });
    })
    .done();
}

function loadUserSettings(req) {
  if (!req.user || !req.user.id)
    return Q({});
  return utils.dbConnect(_config.webapi.database)
    .then(function (cli) {
      return Q()
        .then(function () {
          var data = [req.user.id];
          return Q.ninvoke(cli, "query", "select h.active_ind as allow_tracking, h.delete_dt, p.* from hashkeys h left outer join players p on p.player_id=h.player_id where h.hashkey=$1", data);
        })
        .then(function (result) {
          return result.rows && result.rows.length > 0 ? result.rows[0] : {};
        })
        .finally(function () { cli.release(); });
    });
}

function saveUserSettings(req, res) {
  return utils.dbConnect(_config.webapi.database)
    .then(function (cli) {
      return Q()
        .then(function () {
          if (req.body.action === "register")
            return registerPlayer(req, res, cli);

          if (req.body.action === "anonymize")
            return anonymizePlayer(req, res, cli);

          if (req.body.action === "delete")
            return deletePlayer(req, res, cli);

          var set = "";
          set += ",privacy_match_hist=" + (["1", "2", "3"].indexOf(req.body.matchHistory) >= 0 ? req.body.matchHistory : 1);
          set += ",privacy_nowplaying=" + (req.body.locate === "1");
          set = set.substring(1);

          var data = [req.user.id];
          return Q.ninvoke(cli, "query", "update players set " + set + " where player_id=(select player_id from hashkeys where hashkey=$1 and player_id>2)", data)
            .then(function (status) { return "Your settings have been saved"; });
        })
        .finally(function () { cli.release(); });
    });
}

function registerPlayer(req, res, cli) {
  var msg = "";
  if (req.body.policy !== "1")
    msg += "<li>You did not accept the privacy policy</li>";
  if (req.body.age !== "1")
    msg += "<li>You did not confirm that you are 16 years or older</li>";
  if (msg)
    return msg;

  return Q.ninvoke(cli, "query", "insert into players (nick,stripped_nick) values ($1, $2) returning player_id", [ req.user.displayName, strippedNick(req.user.displayName) ])
    .then(function (result) { Q.ninvoke(cli, "query", "insert into hashkeys (hashkey, player_id) values ($1, $2)", [req.user.id, result.rows[0].player_id]) })
    .then(function () { return ""; });
}

function deletePlayer(req, res, cli) {
  if (!req.body.confirm)
    return "You didn't select the confirmation to delete your account";
  return deletePlayerBySteamId(cli, req.user.id, true)
    .then(function () { return "Your account has been deleted"; });
}

function anonymizePlayer(req, res, cli) {
  if (!req.body.confirm)
    return "You didn't select the confirmation to anonymize your existing data";
  return deletePlayerBySteamId(cli, req.user.id, false)
    .then(function () { return "Your existing data has been anonymized"; });
}

/**
 * Remove color coding from nicknames (^1Nic^7k => Nick)
 * @param {string} nick
 */
function strippedNick(nick) {
  var stripped = "";
  var i, c;
  for (i = 0; i < nick.length; i++) {
    if (nick[i] === "^")
      i++;
    else
      stripped += nick[i];
  }
  return stripped;
}

/**
 * Delete or anonymize the player with the given internal id, including his aliases and ranks. Ratings are kept when anonymizing.
 * Games and game stats are anonymized by replacing the deleted player with a "Deleted Player #" placeholder (negative player_id).
 * @param {any} cli - database client
 * @param {string} steamId - Steam-ID of the player to be deleted
 * @param {bool} deletePlayer - true to delete, false to anonymize
 */
function deletePlayerBySteamId(cli, steamId, deletePlayer) {
  return Q()
    .then(function () { return Q.ninvoke(cli, "query", "select player_id from hashkeys where hashkey=$1", [steamId]) })
    .then(function (result) {
      return result.rowCount === 0 ? Q() : deletePlayerByInternalId(cli, result.rows[0].player_id, deletePlayer);
    });
}

/**
 * Delete or anonymize the player with the given internal id, including his aliases and ranks. Ratings are kept when anonymizing.
 * Games and game stats are anonymized by replacing the deleted player with a "Deleted Player #" placeholder (negative player_id).
 * @param {any} cli - database client
 * @param {int} playerId - internal ID of the player to be deleted
 * @param {bool} deletePlayer - true to delete, false to anonymize
 */
function deletePlayerByInternalId(cli, playerId, deletePlayer) {
  if (!playerId || parseInt(playerId) <= 2)
    return Q(); // special player IDs must not be deleted
  return Q()
    .then(function () { return Q.ninvoke(cli, "query", "select game_id, min(pid) as min_player_id from xonstat.games g, unnest(g.players) pid where players @> ARRAY[$1::int] group by 1", [playerId]) })
    .then(function (result) {
      return result.rows.reduce(function (chain, row) {
        var newId = row.min_player_id < 0 ? row.min_player_id - 1 : -1;
        var args = [row.game_id, playerId, newId];
        var args4 = [row.game_id, playerId, newId, "Anonymous Player " + (-newId)];
        return chain
          .then(function () { return Q.ninvoke(cli, "query", { name: "anon1", text: "update games set players=array_replace(players, $2, $3) where game_id=$1", values: args }); })
          .then(function () { return Q.ninvoke(cli, "query", { name: "anon2", text: "update player_game_stats set player_id=$3, nick=$4, stripped_nick=$4 where game_id=$1 and player_id=$2", values: args4 }); })
          .then(function () { return Q.ninvoke(cli, "query", { name: "anon3", text: "update player_weapon_stats set player_id=$3 where game_id=$1 and player_id=$2", values: args }); });
      }, Q());
    })
    .then(function () { return Q.ninvoke(cli, "query", "delete from player_nicks where player_id=$1", [playerId]); })
    .then(function () { return Q.ninvoke(cli, "query", "delete from player_ranks where player_id=$1", [playerId]); })
    .then(function () { return Q.ninvoke(cli, "query", "delete from player_ranks_history where player_id=$1", [playerId]); })
    .then(function () {
      if (!deletePlayer)
        return Q();
      return Q()
        .then(function () { return Q.ninvoke(cli, "query", "delete from player_elos where player_id=$1", [playerId]); })
        .then(function () { return Q.ninvoke(cli, "query", "update hashkeys set active_ind=false, player_id=-1, delete_dt=timezone('utc',now()) where player_id=$1", [playerId]); })
        .then(function () { return Q.ninvoke(cli, "query", "delete from players where player_id=$1", [playerId]); });
      });
}
