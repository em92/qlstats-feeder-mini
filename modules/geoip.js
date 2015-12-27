const
  pg = require("pg"),
  log4js = require("log4js"),
  request = require("request"),
  Q = require("q");

exports.fillAllServers = fillAllServers;
exports.fillServer = fillServer;

const _logger = log4js.getLogger("geoip");

function fillAllServers(cli) {
  return queryServers(cli)
    .then(function(rows) { return processServers(cli, rows); });
}

function queryServers(cli) {
  return Q
    .ninvoke(cli, "query", "select distinct ip_addr from servers where location is null or location=''")
    .then(function(result) { return Q(result.rows); });
}

function processServers(cli, rows) {
  var ips = rows.map(function(row) { return row.ip_addr; });
  return ips.reduce(function(chain, ip) {
    return chain
      .then(function() { return fillServer(cli, ip); })
      .catch(function(err) { _logger.error("Failed to set location info for " + ip + ": " + err); });
  }, Q());
}

function fillServer(cli, ip) {
  return lookupGeoIp(ip).then(function(geoInfo) { return updateDatabase(cli, ip, geoInfo); });
}

function lookupGeoIp(ip) {
  var defer = Q.defer();
  var ok = true;
  request.get("http://freegeoip.net/json/" + ip, { timeout: 7000 })
    .on("error", function(err) { defer.reject(err); })
    .on("response", function(response) {
      if (response.statusCode != 200) {
        ok = false;
        defer.reject(new Error("HTTP status code " + response.statusCode));
      }
    })
    .on("data", function(data) {
      if (ok)
        defer.resolve(JSON.parse(data));
    });
  return defer.promise;
}

function updateDatabase(cli, ip, geoInfo) {
  // {"ip":"212.241.101.170","country_code":"AT","country_name":"Austria","region_code":"","region_name":"","city":"","zip_code":"","time_zone":"Europe/Vienna","latitude":48.2,"longitude":16.3667,"metro_code":0}
  var region = getRegion(geoInfo.latitude, geoInfo.longitude);
  var values = [geoInfo.country_name, region, geoInfo.country_code, geoInfo.region_code, geoInfo.latitude, geoInfo.longitude, ip];
  return Q
    .ninvoke(cli, "query", { name: "servers_upd", text: "update servers set location=$1, region=$2, country=$3, state=$4, latitude=$5, longitude=$6 where ip_addr=$7", values: values })
    .then(function() { _logger.debug("updated location for server " + ip) });
}

function getRegion(lat, lon) {
  const regions = {
    germany: [53.55, 10.48],
    congo: [-2.12, 23.79],
    china: [44.175189, 93.097220],
    australia: [-27.151813, 142.956882],
    usa: [39.277695, -103.059941],
    brazil: [-12.944029, -58.121122]
  };

  var bestDist = 100000000000;
  var bestRegion = 0;
  var keys = Object.keys(regions);
  for (var i = 0; i < keys.length; i++) {
    var region = regions[keys[i]];
    var dist = calcDistance(region[0], region[1], lat, lon);
    if (dist < bestDist) {
      bestDist = dist;
      bestRegion = i;
    }
  }
  return bestRegion + 1;
}

function calcDistance(lat1, lon1, lat2, lon2) {
  var R = 6371000; // metres
  var phi1 = lat1 * Math.PI / 180;
  var phi2 = lat2 * Math.PI / 180;
  var delta_phi = (lat2 - lat1) * Math.PI / 180;
  var delta_lambda = (lon2 - lon1) * Math.PI / 180;

  var a = Math.sin(delta_phi / 2) * Math.sin(delta_phi / 2) +
    Math.cos(phi1) * Math.cos(phi2) *
    Math.sin(delta_lambda / 2) * Math.sin(delta_lambda / 2);
  var c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

  return R * c;
}