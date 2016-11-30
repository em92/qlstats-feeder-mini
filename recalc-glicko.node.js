var
  log4js = require("log4js"),
  Q = require("q"),
  rating = require("./modules/gamerating");

var _logger = log4js.getLogger("rating");

var options = {
  full: { resetRating: true, updateDatabase: true, onlyProcessMatchesBefore: null, printResult: false },
  incremental: { resetRating: false, updateDatabase: true, onlyProcessMatchesBefore: null, printResult: false },
  // part1 and part2 are for testing
  part1: { resetRating: true, updateDatabase: true, onlyProcessMatchesBefore: new Date(Date.UTC(2015, 12 - 1, 1)), printResult: false },
  part2: { resetRating: false, updateDatabase: false, onlyProcessMatchesBefore: null, printResult: true }
};

function main() {

  //Q.longStackSupport = true;

  var cmd = parseCommandLine();
  cmd.gametypes.reduce(function (chain, gt) {
      var opt = options[cmd.mode];
      if (typeof (opt.printResult) != "undefined")
        opt.printResult = cmd.printResult;
      opt.funMods = cmd.funMods;
      return chain
        .then(function() { _logger.info("-- starting " + gt + "--"); })
        .then(function() { return rating.rateAllGames(gt, opt); })
        .then(function() { _logger.info("-- finished " + gt + "--"); });
    }, Q())
    .done(function() {
      process.exit(0);
    });
}

function parseCommandLine() {
  var gametypes = [];
  var mode = "incremental";
  var print = undefined;
  var funMods = false;

  var args = process.argv.slice(2);
  while (args.length > 0) {
    if (args[0] == "-f")
      mode = "full";
    else if (args[0] == "-i")
      mode = "incremental";
    else if (args[0] == "-a")
      funMods = false;
    else if (args[0] == "-b" || args[0] == "-u")
      funMods = true;
    else if (args[0] == "-p1")
      mode = "part1";
    else if (args[0] == "-p2")
      mode = "part2";
    else if (args[0] == "-all")
      gametypes = ["ctf", "tdm", "ft", "ffa", "ca", "duel", "ad"];
    else if (args[0] == "-r")
      print = true;
    else if (args[0] == "-c")
      args = args.slice(1);
    else if (args[0][0] == "-") {
      console.log("Unsupported option: " + args[0]);
      process.exit(1);
    }
    else
      gametypes.push(args[0].toLowerCase());
    
    args = args.slice(1);
  }
  
  if (gametypes.length == 0) {
    console.log("no game types specified");
    process.exit(1);
  }

  return { gametypes: gametypes, mode: mode, printResult: print, funMods: funMods };
}

main();
