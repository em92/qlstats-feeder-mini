(function(exports) {
  // this is a backport of https://github.com/mmai/glicko2js to Glicko-1

  var mathPow = Math.pow;
  Math.pow = function(b, e) { return e == 2 ? b * b : mathPow(b, e); }

  var q = Math.log(10) / 400; // 0.0057565


  function Race(results) {
    this.matches = this.computeMatches(results);
  }

  Race.prototype.getMatches = function() {
    return this.matches;
  };
  Race.prototype.computeMatches = function(results) {
    var players = [];
    var position = 0;

    results.forEach(function(rank) {
      position += 1;
      rank.forEach(function(player) {
        players.push({ "player": player, "position": position });
      })
    })

    function computeMatches(players) {
      if (players.length === 0) return [];

      var player1 = players.shift()
      var player1_results = players.map(function(player2) {
        return [player1.player, player2.player, (player1.position < player2.position) ? 1 : 0.5];
      });

      return player1_results.concat(computeMatches(players));
    }

    return computeMatches(players);
  }

  function Player(rating, rd) {
    this.__period = 0;

    this.setRating(rating);
    this.setRd(rd);
  }

  Player.prototype.getRating = function() {
    return this.__rating;
  };

  Player.prototype.setRating = function(rating) {
    this.__rating = rating;
  };

  Player.prototype.getRd = function() {
    return this.__rd;
  };

  Player.prototype.setRd = function(rd) {
    this.__rd = rd;
  };

  Player.prototype.getVol = function() {
    return 0;
  };

  Player.prototype.setVol = function(vol) {
  };

  Player.prototype.addResult = function(opponent, outcome) {
    this.adv_ranks.push(opponent.__rating);
    this.adv_rds.push(opponent.__rd);
    this.outcomes.push(outcome);
  };

  Player.prototype.setPeriod = function(c2, period) {
    if (c2 && this.__period && this.__period != period)
      this.__rd = Math.max(30, Math.min(Math.sqrt(Math.pow(this.__rd, 2) + c2 * (period - this.__period)), this.defaultRd));
    this.__period = period;
  }

  // Calculates the new rating and rating deviation of the player.
  // Follows the steps of the algorithm described at http://www.glicko.net/glicko/glicko.pdf
  Player.prototype.update_rank = function(c2, period) {

    //Step 1a : done by Player initialization

    //Step 1b: increase rating deviation for the time a player didn't play
    this.setPeriod(c2, period);

    //Step 2

    var d2 = this.deviation();

    var tempSum = 0;
    for (var i = 0, len = this.adv_ranks.length; i < len; i++) {
      tempSum += this._g(this.adv_rds[i]) * (this.outcomes[i] - this._E(this.adv_ranks[i], this.adv_rds[i]));
    }


    var b = 1 / (1 / Math.pow(this.__rd, 2) + 1 / d2);

    this.__rating += q * b * tempSum;

    this.__rd = Math.max(30, Math.sqrt(b));
  };


// Calculation of the estimated deviation of the player's rating based on game outcomes
  Player.prototype.deviation = function() {
    var tempSum = 0;
    for (var i = 0, len = this.adv_ranks.length; i < len; i++) {
      var tempE = this._E(this.adv_ranks[i], this.adv_rds[i]);
      tempSum += Math.pow(this._g(this.adv_rds[i]), 2) * tempE * (1 - tempE);
    }
    return 1 / (Math.pow(q, 2) * tempSum);
  };

  // The Glicko E function.
  Player.prototype._E = function(p2rating, p2RD) {
    return 1 / (1 + Math.pow(10, -1 * this._g(p2RD) * (this.__rating - p2rating) / 400));
  };

  // The Glicko g(RD) function.
  Player.prototype._g = function(RD) {
    return 1 / Math.sqrt(1 + 3 * Math.pow(q * RD / Math.PI, 2));
  };


  //=========================  Glicko class =============================================
  function Glicko(settings) {
    settings = settings || {};

    // Default rating
    this._default_rating = settings.rating || 1500;

    // Default rating deviation (small number = good confidence on the
    // rating accuracy)
    this._default_rd = settings.rd || 350;

    this._period = 0;
    this._c2 = Math.pow(settings.c || 0, 2);

    this.players = [];
    this.players_index = 0;
    this.activePlayers = {};
  }

  Glicko.prototype.makeRace = function(results) {
    return new Race(results);
  };

  Glicko.prototype.removePlayers = function() {
    this.players = [];
    this.players_index = 0;
  };

  Glicko.prototype.getPlayers = function() {
    return this.players;
  };

  Glicko.prototype.cleanPreviousMatches = function() {
    for (var i = 0, len = this.players.length; i < len; i++) {
      this.players[i].adv_ranks = [];
      this.players[i].adv_rds = [];
      this.players[i].outcomes = [];
      this.activePlayers = {};
    }
  };

  Glicko.prototype.calculatePlayersRatings = function(period) {
    this._period = period || this._period + 1;
    var keys = Object.keys(this.activePlayers);
    for (var i = 0, len = keys.length; i < len; i++) {
      this.players[keys[i]].update_rank(this._c2, this._period);
    }
  };

  /** 
     * Add players and match result to be taken in account for the new rankings calculation
     * players must have ids, they are not created if it has been done already.
     * @param {Object litteral} pl1 The first player
     * @param {Object litteral} pl2 The second player
     * @param {number} outcom The outcome : 0 = defeat, 1 = victory, 0.5 = draw
     */
  Glicko.prototype.addMatch = function(player1, player2, outcome) {
    var pl1 = this._createInternalPlayer(player1.rating, player1.rd, player1.id);
    var pl2 = this._createInternalPlayer(player2.rating, player2.rd, player2.id);
    this.addResult(pl1, pl2, outcome);
    return { pl1: pl1, pl2: pl2 };
  };

  Glicko.prototype.makePlayer = function(rating, rd) {
    //We do not expose directly createInternalPlayer in order to prevent the assignation of a custom player id whose uniqueness could not be guaranteed
    return this._createInternalPlayer(rating, rd);
  };

  Glicko.prototype._createInternalPlayer = function(rating, rd, id) {
    if (id === undefined) {
      id = this.players_index;
      this.players_index = this.players_index + 1;
    } else {
      //We check if the player has already been created
      var candidate = this.players[id];
      if (candidate !== undefined) {
        return candidate;
      }
    }
    var player = new Player(rating || this._default_rating, rd || this._default_rd);
    var playerProto = Object.getPrototypeOf(player);

    // Set this specific Player's `defaultRating`. This _has_ to be done
    // here in order to ensure that new `Glicko` instances do not change
    // the `defaultRating` of `Player` instances created under previous
    // `Glicko` instances.
    playerProto.defaultRating = this._default_rating;
    playerProto.defaultRd = this._default_rd;

    // Since this `Player`'s rating was calculated upon instantiation,
    // before the `defaultRating` was defined above, we much re-calculate
    // the rating manually.
    player.setRating(rating || this._default_rating);

    player.id = id;
    player.adv_ranks = [];
    player.adv_rds = [];
    player.outcomes = [];
    this.players[id] = player;
    return player;
  };

  /** 
       * Add a match result to be taken in account for the new rankings calculation
       * @param {Player} player1 The first player
       * @param {Player} player2 The second player
       * @param {number} outcome The outcome : 0 = defeat, 1 = victory, 0.5 = draw
       */
  Glicko.prototype.addResult = function(player1, player2, outcome) {
    player1.addResult(player2, outcome);
    player2.addResult(player1, 1 - outcome);
    this.activePlayers[player1.id] = player1;
    this.activePlayers[player2.id] = player2;
  };

  Glicko.prototype.updateRatings = function(matches, period) {
    if (matches instanceof Race) {
      matches = matches.getMatches();
    }
    if (typeof (matches) !== 'undefined') {
      this.cleanPreviousMatches();
      for (var i = 0, len = matches.length; i < len; i++) {
        var match = matches[i];
        this.addResult(match[0], match[1], match[2]);
      }
    }
    this.calculatePlayersRatings(period);
  };

  Glicko.prototype.setPeriod = function(period) {
    var self = this;
    this.players.forEach(function(player) {
      player.setPeriod(self._c2, period);
    });
  }


  exports.Glicko = Glicko;

})(typeof exports === 'undefined' ? this['glicko'] = {} : exports);