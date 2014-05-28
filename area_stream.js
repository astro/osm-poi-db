var stream = require('stream');
var util = require('util');
var GeoHash = require('geohash').GeoHash;
var WGS84Util = require('wgs84-util');
var levelup = require('levelup');
var db = levelup('./osm', {
    keyEncoding: 'utf8',
    valueEncoding: 'json'
});

util.inherits(PrefixStream, stream.Readable);
function PrefixStream(prefix) {
    stream.Readable.call(this, {
        objectMode: true
    });

    var rs = db.createReadStream({
        start: prefix
    });
    this._read = function() {
        rs.resume();
    };
    rs.on('data', function(data) {
        rs.pause();
        if (data.key.indexOf(prefix) !== 0) {
            rs.destroy();
        } else {
            this.push(data);
        }
    }.bind(this));
    rs.on('close', function() {
        this.push(null);
    }.bind(this));
}

var EXTENT = 1000;  // 1km

util.inherits(GeoStream, stream.Readable);
function GeoStream(options) {
    stream.Readable.call(this, {
        objectMode: true
    });

    var extent = options.extent || EXTENT;
    var getAway = function(brng) {
        return WGS84Util.destinationPoint({
            coordinates: [options.lat, options.lon]
        }, brng, extent).coordinates;
    };
    var bounds = {
        n: Number(getAway(0)[0]),
        e: Number(getAway(90)[1]),
        s: Number(getAway(180)[0]),
        w: Number(getAway(270)[1])
    };

    this.prefixes = [];
    for(var lon = bounds.w; lon <= bounds.e; ) {
        var nextLon;
        for(var lat = bounds.s; lat <= bounds.n; ) {
            var geoHash = GeoHash.encodeGeoHash(lat, lon).slice(0, 6);
            this.prefixes.push("geo:" + geoHash);

            var decoded = GeoHash.decodeGeoHash(geoHash);
            lat = decoded.latitude[1] + 0.00000001;
            nextLon = decoded.longitude[1] + 0.0000001;
        }
        lon = nextLon;
    }
}

GeoStream.prototype._read = function(amount) {
    var that = this;

    if (!this.stream) {
        var nextPrefix = this.prefixes.pop();
        if (nextPrefix) {
            // console.log("new PrefixStream", nextPrefix, "of", this.prefixes.length + 1);
            var ps = new PrefixStream(nextPrefix);
            ps.on('data', function(data) {
                ps.pause();  // Wait for next read
                that.push(data.value);
            });
            ps.on('end', function() {
                that.stream = null;
                that._read();
            });
            this.stream = ps;
        } else {
            this.push(null);
            return;
        }
    }

    this.stream.resume();
};


module.exports = GeoStream;
