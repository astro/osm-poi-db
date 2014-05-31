var async = require('async');
var parseOSM = require('osm-pbf-parser');
var GeoHash = require('geohash').GeoHash;
var levelup = require('levelup');
var db = levelup('./osm', {
    keyEncoding: 'utf8',
    valueEncoding: 'json'
});
var pDb = levelup('./osm.points', {
    keyEncoding: 'utf8',
    valueEncoding: 'json'
});
var Transform = require('stream').Transform;
var util = require('util');

var INTERESTING = [
    "amenity", "emergency", "historic",
    "leisure", "public_transport", "shop",
    "sport", "tourism", "craft",
    "office", "addr:street", "addr:housenumber"
];


util.inherits(DBSink, Transform);
function DBSink() {
    Transform.call(this, { objectMode: true });
}

DBSink.prototype._transform = function(data, encoding, callback) {
    if (data.type === 'nodes') {
        async.each(data.nodes, function(node, cb) {
            node._type = 'node';
            this.onElement(node, cb);
        }.bind(this), callback);
    } else if (data.type === 'way') {
        data._type = 'way';
        this.onElement(data.way, callback);
    }
};

DBSink.prototype.onElement = function(value, cb) {
    if (value.lat && value.lon) {
        pDb.put("p:" + value.id, {
            lat: value.lat,
            lon: value.lon
        }, function(err) {
            this.pushElement(value);
            cb(err);
        }.bind(this));
    } else if (value.ref && value.ref.length > 0) {
        var lon = 0, lat = 0, len = 0;
        async.each(value.nd, function(id, cb) {
            pDb.get("p:" + id, function(err, data) {
                if (data) {
                    lon += data.lon;
                    lat += data.lat;
                    len++;
                } else {
                    console.log("get p:" + id, err, data);
                }
                cb();
            });
        }, function(err) {
            if (len > 0) {
                value.lon = lon / len;
                value.lat = lat / len;
                // console.log("looked up", value.lon, "/", value.lat, "from", len);
                this.pushElement(value);
            } else {
                console.log("No location for", value);
                cb();
            }
        }.bind(this), cb);
    } else {
        console.log("No location for", value);
        cb();
    }
};

DBSink.prototype.pushElement = function(value) {
    var isInteresting = INTERESTING.some(function(field) {
        return value.tags.hasOwnProperty(field);
    });
    if (isInteresting) {
        var geohash = GeoHash.encodeGeoHash(value.lat, value.lon);
        var key = "geo:" + geohash + ":" + value.id;
        this.push({ key: key, value: value });
    }
};


var dbSink = new DBSink();
var osm = parseOSM();

process.stdin
    .pipe(osm)
    .pipe(dbSink)
    .pipe(db.createWriteStream());

osm.on('end', function() {
    console.log("osm end");

});
process.stdin.on('end', function() {
    console.log("Fin stdin.");
});
