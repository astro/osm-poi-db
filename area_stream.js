var stream = require('stream');
var util = require('util');
var GeoHash = require('geohash').GeoHash;
var WGS84Util = require('wgs84-util');
var levelup = require('levelup');
var db = levelup('./osm', {
    keyEncoding: 'utf8',
    valueEncoding: 'json'
});

var PREFIX_LENGTH = 6;

util.inherits(GeoHashPrefixGenerator, stream.Readable);
function GeoHashPrefixGenerator(lon, lat) {
    stream.Readable.call(this, {
        objectMode: true,
        highWaterMark: 0
    });

    this.iteration = 0;
    var start = GeoHash.encodeGeoHash(lat, lon).slice(0, PREFIX_LENGTH);
    this.push(start);
    var prefixLocation = GeoHash.decodeGeoHash(start);
    this.lon = prefixLocation.longitude[2];
    this.lat = prefixLocation.latitude[2];
    this.step = prefixLocation.longitude[1] - prefixLocation.longitude[0];
}

GeoHashPrefixGenerator.prototype._read = function() {
    this.iteration++;

    var distance = (this.iteration - 0.5) * this.step;
    this.emit('distance', distance);
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
        // console.log("x", i, "lon/lat:", lon, lat, "geohash", geohash);
        this.push(geohash);
        lon += lonDelta;
        lat += latDelta;
    }
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
    var ps = new PrefixStream(prefix);
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
    pg.on('distance', this.emit.bind(this, 'distance'));
    var gs = new GeoStream();
    pg.pipe(gs).pipe(this);
}

AreaStream.prototype._transform = function(data, encoding, callback) {
    this.push(data);
    callback();
};

module.exports = AreaStream;
