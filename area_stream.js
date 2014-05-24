var stream = require('stream');
var util = require('util');
var GeoHash = require('geohash').GeoHash;
var WGS84Util = require('wgs84-util');
var levelup = require('levelup');
var db = levelup('./osm', {
    keyEncoding: 'utf8',
    valueEncoding: 'json'
});


util.inherits(GeoStream, stream.Readable);
function GeoStream(options) {
    stream.Readable.call(this, {
        objectMode: true
    });

    var geoKey = "geo:" + GeoHash.encodeGeoHash(options.lat, options.lon);
    this.distanceTo = function(geohash) {
        var coords = GeoHash.decodeGeoHash(geohash);
        return WGS84Util.distanceBetween({
            coordinates: [options.lat, options.lon]
        }, {
            coordinates: [coords.latitude[2], coords.longitude[2]]
        });
    };

    this.minMaxDistanceIndex = 0;
    this.streamsMaxDistances = [0, 0];

    var that = this;
    function makeStream(opts) {
        var stream = db.createReadStream(opts);
        stream.on('data', function(data) {
            // console.log("db data", data);
            var shouldProceed = that.handleData(stream, data.key, data.value);
            if (shouldProceed) {
                stream.pause();  // Wait for next read
                that.push(data.value);
            } else {
                stream.destroy();
                var i = that.streams.indexOf(stream);
                that.streams.splice(i, 1);
                that.streamsMaxDistances.splice(i, 1);
                // console.log("spliced", i, "from", that.streams.length+1);
                if (that.streams.length < 1) {
                    that.push(null);
                } else {
                    that.minMaxDistanceIndex = 0;
                    that.read(0);
                }
            }
        });
        return stream;
    }

    this.streams = [
        makeStream({
            start: geoKey,
            reverse: true
        }),
        makeStream({
            start: geoKey
        })
    ];
}

GeoStream.prototype._read = function(amount) {
    // console.log("resume", this.minMaxDistanceIndex);
    var stream = this.streams[this.minMaxDistanceIndex] ||
        this.streams[0];
    if (stream) {
        stream.resume();
    }
};

GeoStream.prototype.handleData = function(stream, key, value) {
    if ((m = key.match(/^geo:(.+)/))) {

        var streamIndex = this.streams.indexOf(stream);
        var newMaxDistance = false;
        var d = this.distanceTo(m[1]);
        value._distance = d;
        if (d > this.streamsMaxDistances[streamIndex]) {
            // console.log("newMaxDistance", d);
            this.streamsMaxDistances[streamIndex] = d;
            newMaxDistance = true;
        }

        if (newMaxDistance) {
            /* Find minimum in streamsMaxDistance */
            this.minMaxDistanceIndex = 0;
            var minMaxDistance = this.streamsMaxDistances[0];
            for(var i = 1; i < this.streams.length; i++) {
                if (this.streamsMaxDistances[i] < minMaxDistance) {
                    minMaxDistance = this.streamsMaxDistances[i];
                    this.minMaxDistanceIndex = i;
                }
            }
        }

        return true;
    } else {
        console.log("Stop at key", key);
        return false;
    }
};

GeoStream.prototype.destroy = function() {
    var pending = 1;
    var onClose = function() {
        pending--;
        if (pending < 1) {
            this.emit('close');
        }
    }.bind(this);
    this.streams.forEach(function(stream) {
        stream.on('close', onClose);
        pending++;
        stream.destroy();
    });
    this.streams = [];
    onClose();
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
        if (!this.seen.hasOwnProperty(data)) {
            this.seen[data] = true;
            this.push(d);
        }
    }.bind(this));

    cb();
};


util.inherits(LookupStream, stream.Transform);
function LookupStream() {
    stream.Transform.call(this, {
        objectMode: true
    });
}

LookupStream.prototype._transform = function(data, encoding, cb) {
    db.get(data, function(err, data) {
        if (data) {
            this.push(data);
        }
        cb(err);
    }.bind(this));
};


util.inherits(AreaStream, stream.Readable);
function AreaStream(opts) {
    stream.Readable.call(this, {
        objectMode: true
    });

    var gs = new GeoStream(opts);
    var ds = new DedupStream();
    var ls = new LookupStream();
    gs.pipe(ds).pipe(ls);

    ls.on('data', function(data) {
        ls.pause();
        this.push(data);
    }.bind(this));
    gs.on('close', this.emit.bind(this, 'close'));
    this._read = function() {
        ls.resume();
    };
    this.destroy = gs.destroy.bind(gs);
}

module.exports = AreaStream;
