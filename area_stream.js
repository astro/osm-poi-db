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
    
    this.streams = [];
    var that = this;
    var addStream = function(prefix) {
        var ps = new PrefixStream(prefix);
        ps.on('data', function(data) {
            // console.log("ps data", data.key);
            ps.pause();  // Wait for next read
            that.push(data.value);
        });
        ps.on('end', function() {
            var i = that.streams.indexOf(ps);
            that.streams.splice(i, 1);
            // console.log("spliced", i, "from", that.streams.length+1);
            if (that.streams.length < 1) {
                that.push(null);
            } else {
                that.read(0);
            }
        });
        that.streams.push(ps);
    }
    for(var lon = bounds.w; lon <= bounds.e; ) {
        var nextLon;
        for(var lat = bounds.s; lat <= bounds.n; ) {
            var geoHash = GeoHash.encodeGeoHash(lat, lon).slice(0, 6);
            // TODO: maybe not start all at once
            addStream("geo:" + geoHash);

            var decoded = GeoHash.decodeGeoHash(geoHash);
            // console.log("stream", geoHash, "decoded", decoded);
            lat = decoded.latitude[1] + 0.00000001;
            nextLon = decoded.longitude[1] + 0.0000001;
        }
        lon = nextLon;
    }
    // console.log("bounds", bounds, this.streams.length + " prefix streams");
}

GeoStream.prototype._read = function(amount) {
    this.streams.forEach(function(stream) {
        stream.resume();
    });
};

util.inherits(DedupStream, stream.Transform);
function DedupStream() {
    stream.Transform.call(this, {
        objectMode: true
    });
    this.seen = {};
}

DedupStream.prototype._transform = function(data, encoding, cb) {
    if (data.constructor !== Array) {
        data = [data];
    }

    data.forEach(function(d) {
        if (!this.seen.hasOwnProperty(d.id)) {
            this.seen[d.id] = true;
            this.push(d);
        }
    }.bind(this));

    cb();
};


util.inherits(AreaStream, stream.Readable);
function AreaStream(opts) {
    stream.Readable.call(this, {
        objectMode: true
    });

    var gs = new GeoStream(opts);
    var ds = new DedupStream();
    gs.pipe(ds);

    ds.on('data', function(data) {
        ds.pause();
        this.push(data);
    }.bind(this));
    this._read = function() {
        ds.resume();
    };
    ds.on('end', function() {
        this.push(null);
    }.bind(this));
}

module.exports = AreaStream;
