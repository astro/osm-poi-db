var async = require('async');
var expat = require('node-expat');
var GeoHash = require('geohash').GeoHash;
var levelup = require('levelup');
var db = levelup('./osm');

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

        var ptrs = data ? data.split(/;/g) : [];
        ptrs.push(ptr);
        db.put(geoKey, nub(ptrs).join(";"), function(err) {
            cb(err);
        });
    });
}

function onElement(type, body, cb) {
    var ptr = type + ":" + body.id;
    db.put(ptr, JSON.stringify(body), function(err) {
        if (body && body.lat && body.lon) {
            onLocation(body.lat, body.lon, ptr, cb);
        } else if (body.nd && body.nd.length > 0) {
            var lon = 0, lat = 0, len = body.nd.length;
            async.each(body.nd, function(id, cb) {
                db.get("node:" + id, function(err, data) {
                    if (err) {
                        return cb(err);
                    }
                    var body = JSON.parse(data);
                    lon += Number(body.lon);
                    lat += Number(body.lat);
                    cb();
                });
            }, function(err) {
                if (err) {
                    return cb(err);
                }

                body.lon = lon / len;
                body.lat = lat / len;
                console.log("looked up", body.lon, "/", body.lat, "from", len);
                onLocation(body.lat, body.lon, ptr, cb);
            });
        } else {
            if (type == 'node' || type == 'way') {
                console.log("No location for " + type + ":", body);
            }
            cb(err);
        }
    });
    process.stdin.pause();
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
        // TODO: lookup tags
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
