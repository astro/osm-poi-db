var async = require('async');
var expat = require('node-expat');
var GeoHash = require('geohash').GeoHash;
var levelup = require('levelup');
var db = levelup('./osm', {
    keyEncoding: 'utf8',
    valueEncoding: 'json'
});

var INTERESTING = [
    "amenity", "emergency", "historic",
    "leisure", "public_transport", "shop",
    "sport", "tourism", "craft",
    "office", "addr:street", "addr:housenumber"
];

function nub(a) {
    var elems = {};
    a.forEach(function(e) {
        elems[e] = true;
    });
    return Object.keys(elems);
}

function onLocation(lat, lon, ptr, cb) {
    var geohash = GeoHash.encodeGeoHash(lat, lon);
    var geoKey = "geo:" + geohash;
    db.get(geoKey, function(err, data) {
        if (err && !err.notFound) {
            return cb(err);
        }

        data = data || [];
        if (data.indexOf(ptr) < 0) {
            data.push(ptr);
            db.put(geoKey, data, function(err) {
                cb(err);
            });
        } else {
            cb();
        }
    });
}

function onElement(type, body, cb) {
    function go(cb) {
        var ptr = type + ":" + body.id;
        db.put(ptr, body, function(err) {
            if (!err && body.lat && body.lon) {
                onLocation(body.lat, body.lon, ptr, cb);
            } else {
                cb(err);
            }
        });
    }

    if (body && body.lat && body.lon) {
        go(cb);
    } else if (body.nd && body.nd.length > 0) {
        var lon = 0, lat = 0, len = 0;
        async.each(body.nd, function(id, cb) {
            db.get("node:" + id, function(err, data) {
                if (err && err.notFound) {
                    return cb();
                }
                if (err) {
                    return cb(err);
                }
                lon += Number(data.lon);
                lat += Number(data.lat);
                len++;
                cb();
            });
        }, function(err) {
            if (err) {
                return cb(err);
            }

            if (len > 0) {
                body.lon = lon / len;
                body.lat = lat / len;
                // console.log("looked up", body.lon, "/", body.lat, "from", len);
                go(cb);
            } else {
                cb();
            }
        });
    } else {
        if (type == 'node' || type == 'way') {
            console.log("No location for " + type + ":", body);
        }
        cb();
    }
}

var parser = new expat.Parser();
var state, current;
parser.on('startElement', function(name, attrs) {
    if (!state && 
        (name == 'node' ||
         name == 'way' ||
         name == 'relation')) {
        current = attrs;
        state = name;

    } else if (state && name == 'tag' && !current.hasOwnProperty(attrs.k)) {
        current[attrs.k] = attrs.v;
    } else if (state && name == 'nd') {
        if (!current.nd) {
            current.nd = [];
        }
        current.nd.push(attrs.ref);
    } else if (state && name == 'member') {
        if (!current.members) {
            current.members = [];
        }
        current.members.push(attrs);
    } else {
        console.log('in', state, 'unhandled startElement', name, attrs);
    }
});
var pending = 0;
parser.on('endElement', function(name) {
    if (state && name == state) {
        var interested = true; /*INTERESTING.some(function(field) {
            return current.hasOwnProperty(field);
        });*/

        if (interested) {
            onElement(state, current, function(err) {
                pending--;
                if (err) {
                    throw err;
                }
                if (pending < 4) {
                    process.stdin.resume();
                }
            });
            pending++;
            if (pending >= 4) {
                process.stdin.pause();
            }
        }

        state = null;
        current = null;
    }
});

process.stdin.resume();
process.stdin.pipe(parser);

parser.on('end', function() {
    console.log("endDocument");
});
process.stdin.on('end', function() {
    console.log("Fin.");
});
