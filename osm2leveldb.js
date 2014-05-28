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

function onLocation(lat, lon, body, cb) {
    if (!INTERESTING.some(function(f) {
        return body.hasOwnProperty(f);
    })) {
        return cb();
    }

    var geohash = GeoHash.encodeGeoHash(lat, lon);
    var key = "geo:" + geohash + ":" + body.id;
    db.put(key, body, cb);
}

function onElement(type, body, cb) {
    body._element = type;

    if (body && body.lat && body.lon) {
        db.put("p:" + body.id, {
            lat: body.lat,
            lon: body.lon
        }, function(err) {
            if (err) {
                cb(err)
            } else {
                onLocation(body.lat, body.lon, body, cb);
            }
        });
    } else if (body.nd && body.nd.length > 0) {
        var lon = 0, lat = 0, len = 0;
        // TODO: when using a writeStream, wait for nextTick to have
        // all preceding p:* flushed out
        async.each(body.nd, function(id, cb) {
            db.get("p:" + id, function(err, data) {
                if (data) {
                    lon += Number(data.lon);
                    lat += Number(data.lat);
                    len++;
                } else {
                    console.log("get p:" + body.id, err, data);
                }
                cb();
            });
        }, function(err) {
            if (len > 0) {
                body.lon = lon / len;
                body.lat = lat / len;
                // console.log("looked up", body.lon, "/", body.lat, "from", len);
                onLocation(body.lat, body.lon, body, cb);
            } else {
                console.log("No location for", body);
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

    } else if (state && name == 'tag') {
        if (!current.hasOwnProperty(attrs.k)) {
            current[attrs.k] = attrs.v;
        } else {
            console.log("ignore duplicate tag", attrs.k + "=" + attrs.v, "current:", current[attrs.k]);
        }
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
var CONCURRENCY = 1024;
var pending = 0;
parser.on('endElement', function(name) {
    if (state && name == state) {
        var interested = true;

        if (interested) {
            onElement(state, current, function(err) {
                pending--;
                if (err) {
                    throw err;
                }
                if (pending < CONCURRENCY) {
                    process.stdin.resume();
                }
            });
            pending++;
            if (pending >= CONCURRENCY) {
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

    // delete p:*
    var deleted = 0;
    var pStream = db.createReadStream({ start: "p:" });
    pStream.on('data', function(data) {
        if (/^p:/.test(data.key)) {
            pStream.pause();
            db.del(data.key, function(err) {
                if (err) {
                    console.error(err);
                    throw err;
                }
                deleted++;
                pStream.resume();
            });
        } else {
            pStream.destroy();
        }
    });
    pStream.on('close', function() {
        console.log("Deleted", deleted, "intermediate items");
    });
});
