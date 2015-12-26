const
  rating = require("./gamerating"),
  log4js = require("log4js"),
  Q = require("q");

var gametype = "ctf";
var mode = "full";
var printResult = true;

var options = {
  full: { resetRating: true, updateDatabase: true, onlyProcessMatchesBefore: null, printResult: printResult },
  part1: { resetRating: true, updateDatabase: true, onlyProcessMatchesBefore: new Date(Date.UTC(2015, 12 - 1, 1)), printResult: printResult  },
  part2: { resetRating: false, updateDatabase: false, onlyProcessMatchesBefore: null, printResult: printResult  },
  incremental: { resetRating: false, updateDatabase: true, onlyProcessMatchesBefore: null, printResult: printResult  }
};

function main() {
  
  //Q.longStackSupport = true;

  rating.rateAllGames(gametype, options[mode])
    .then(printResults)
    .done(function() {
      console.log("-- finished --");
      //process.exit(0);
    });
}

main();
