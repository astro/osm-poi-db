var stream = require('stream');
var util = require('util');
var GeoHash = require('geohash').GeoHash;
var WGS84Util = require('wgs84-util');
var levelup = require('levelup');
var db = levelup('./osm', {
    keyEncoding: 'utf8',
    valueEncoding: 'json'
});

var PREFIX_LENGTH = 7;

util.inherits(GeoHashPrefixGenerator, stream.Readable);
function GeoHashPrefixGenerator(lon, lat) {
    stream.Readable.call(this, {
        objectMode: true,
        highWaterMark: 0
    });

    this.iteration = 0;
    var start = GeoHash.encodeGeoHash(lat, lon).slice(0, PREFIX_LENGTH);
    // push center tile
    this.push(start);
    var prefixLocation = GeoHash.decodeGeoHash(start);
    this.lon = prefixLocation.longitude[2];
    this.lat = prefixLocation.latitude[2];
    this.step = prefixLocation.longitude[1] - prefixLocation.longitude[0];
}

GeoHashPrefixGenerator.prototype._read = function() {
    this.iteration++;

    var distance = (this.iteration - 0.5) * this.step;
    var minDistance = WGS84Util.distanceBetween({
        coordinates: [this.lat + (this.iteration - 2) * this.step, this.lon]
    }, {
        coordinates: [this.lat - this.step, this.lon]
    });
    // console.log("distance", distance, minDistance);
    // Signal that all nodes up to this distance should have occured
    this.emit('distance', minDistance);
    // West:
    this.readStroke(
        this.lon - distance,
        this.lat - distance,
        0,
        this.step
    );
    // East:
    this.readStroke(
        this.lon + distance,
        this.lat + distance,
        0,
        -this.step
    );
    // North:
    this.readStroke(
        this.lon - distance,
        this.lat + distance,
        this.step,
        0
    );
    // South:
    this.readStroke(
        this.lon + distance,
        this.lat - distance,
        -this.step,
        0
    );
};

GeoHashPrefixGenerator.prototype.readStroke = function(lon, lat, lonDelta, latDelta) {
    var amount = this.iteration * 2;
    for(var i = 0; i < amount; i++) {
        var geohash = GeoHash.encodeGeoHash(lat, lon).slice(0, PREFIX_LENGTH);
        // console.log("it", this.iteration, "i", i, "lon/lat:", lon, lat, "geohash", geohash);
        this.push(geohash);
        lon += lonDelta;
        lat += latDelta;
    }
};

GeoHashPrefixGenerator.prototype.destroy = function() {
    this._read = function() {
        this.push(null);
    };
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
        if (!this.seen.hasOwnProperty(d)) {
            this.seen[d] = true;
            this.push(d);
        }
    }.bind(this));

    cb();
};


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

util.inherits(GeoStream, stream.Transform);
function GeoStream() {
    stream.Transform.call(this, {
        objectMode: true,
        highWaterMark: 1
    });
}

GeoStream.prototype._transform = function(prefix, encoding, callback) {
    var that = this;
    var ps = new PrefixStream("geo:" + prefix);
    ps.on('data', function(data) {
        that.push(data.value);
    });
    ps.on('end', callback);
};


util.inherits(AreaStream, stream.Transform);
function AreaStream(options) {
    stream.Transform.call(this, {
        objectMode: true,
        highWaterMark: 1
    });

    var pg = new GeoHashPrefixGenerator(options.lon, options.lat);
    pg.on('distance', function(minDistance) {
        this.emit('distance', minDistance);
        if (minDistance >= options.extent) {
            pg.destroy();
        }
    }.bind(this));
    var dedup = new DedupStream();
    var gs = new GeoStream();
    pg.pipe(dedup).pipe(gs).pipe(this);
}

AreaStream.prototype._transform = function(data, encoding, callback) {
    this.push(data);
    callback();
};

module.exports = AreaStream;
