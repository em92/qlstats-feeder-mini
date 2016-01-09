var
  fs = require("graceful-fs"),
  zlib = require("zlib"),
  log4js = require("log4js"),
  Q = require("q");

var _config;
var _logger = log4js.getLogger("select");

/**
 * Recursively load and process all provided .json[.gz] files and folders
 * @param {string[]} files - files and folders
 * @returns {Promise<boolean>} A promise that will be fulfilled when all files are processed. True when there were no errors.
 */
function processJsonFiles(files) {
  // serialize calls for each file
  return files.reduce(function(chain, file) {
    return chain.then(function() { return feedJsonFile(file); }); // single & to prevent short-circuit evaluation
  }, Q());

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
            var gameData = JSON.parse(json);
            return processGameData(file, gameData);
          })
          .catch(function(err) {
            _logger.error(file.replace(__dirname + "/" + _config.feeder.jsondir, "") + ": " + err);
            return false;
          });
      })
      .catch(function(err) { _logger.error("failed to process " + file + ": " + err) });
  }
}

function processGameData(file, data) {
  var gt = (data.matchStats.GAME_TYPE || "").toLowerCase();
  if (",ffa,ca,duel,tdm,ctf,ft,".indexOf(gt) < 0) {
    console.log(file + "\t" + gt);
  }
  return true;
}


function main() {
  _config = JSON.parse(fs.readFileSync(__dirname + "/cfg.json"));
  processJsonFiles([_config.feeder.jsondir])   
  .catch(function (err) { console.log(err); })
  .done();
}

main();
