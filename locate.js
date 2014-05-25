var WGS84Util = require('wgs84-util');
var AreaStream = require('./area_stream');

var INTERESTING = ["amenity", "emergency", "historic", "leisure", "public_transport", "shop", "sport", "tourism", "craft", "office"];


// TODO: filterByHasKey(INTERESTING), stream response as json
exports.locateProximity = function(opts, cb) {
    var start = Date.now();
    var address, addressDistance, interesting = [];
    function onClose() {
        if (cb) {
            interesting = interesting.sort(function(a, b) {
                if (a._distance < b._distance) {
                    return -1;
                } else if (a._distance > b._distance) {
                    return 1;
                } else {
                    return 0;
                }
            });
            cb(null, address, interesting);
            cb = null;
        }
    }
    var as = new AreaStream(opts);
    function onData(data) {
        var distance = WGS84Util.distanceBetween({
            coordinates: [opts.lat, opts.lon]
        }, {
            coordinates: [data.lat, data.lon]
        });
        data._distance = distance;
        // console.log("data", data);

        if (data.lat && data.lon &&
            (!address || distance < addressDistance) &&
            data['addr:housenumber']) {

            address = {
                _distance: distance
            };
            addressDistance = distance;
            Object.keys(data).forEach(function(k) {
                var m;
                if ((m = k.match(/^addr:(.+)/))) {
                    address[m[1]] = data[k];
                }
            });
        }

        var isInterested = INTERESTING.some(function(f) {
            return data.hasOwnProperty(f);
        });
        if (isInterested) {
            interesting.push(data);
        }
        var runtime = Date.now() - start;
        if (runtime >= opts.maxRuntime ||
            interesting.length == (opts.max || 100)) {

            as.destroy();
        }
    }
    as.on('data', onData).on('close', onClose);
};

var opts = {
    lon: 13.8072735,
    lat: 51.0519905,
    max: 1000,
    maxRuntime: 100
};
function run() {
    var t1 = Date.now();
    exports.locateProximity(opts, function(err, addr, nodes) {
        var t2 = Date.now();
        var a = addr.street + " " + addr.housenumber + ", " + (addr.postcode || "") + " " + addr.city + " (" + Math.round(addr._distance) + "m)";
        var xs = nodes.filter(function(node) {
            return !!node.name;
        }).slice(0, 3).map(function(node) {
            return node.name + " (" + node.id + ", " + Math.round(node._distance) + "m)";
        }).join(", ");
        console.log("cb", nodes.length, "[" + (t2 - t1) + "ms]", a, ":", xs, "..", Math.round(nodes[nodes.length - 1]._distance) + "m");

        opts.lon -= 0.0001;
        opts.lat -= 0.00005;
        run();
    });
}
run()
