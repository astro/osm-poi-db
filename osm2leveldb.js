var async = require('async');
var parseOSM = require('osm-pbf-parser');
var GeoHash = require('geohash').GeoHash;
var leveldown = require('leveldown');
var db = leveldown('./osm', {
    writeBufferSize: 64 * 1024 * 1024
});
var Transform = require('stream').Transform;
var Writable = require('stream').Writable;
var util = require('util');

var INTERESTING = [
    "amenity", "emergency", "historic",
    "leisure", "public_transport", "shop",
    "sport", "tourism", "craft",
    "office", "addr:street", "addr:housenumber"
];

var CONCURRENCY = 32;

util.inherits(Expander, Transform);
function Expander() {
    Transform.call(this, { objectMode: true });
    this._readableState.highWaterMark = CONCURRENCY;
    this._writableState.highWaterMark = 1;
}

Expander.prototype._transform = function(values, encoding, callback) {
    async.eachSeries(values, function(value, cb) {
        if (value.type === 'way') {
            this.expandWay(value, function(err) {
                this.push(value);
                cb(err);
            }.bind(this));
        } else {
            this.push(value);
            setImmediate(cb);
        }
    }.bind(this), callback);
};

Expander.prototype.expandWay = function(way, callback) {
    if (way.refs && way.refs.length > 0) {
        var lon = 0, lat = 0, len = 0;
        // var start = Date.now();
        async.eachLimit(way.refs, CONCURRENCY, function(id, cb) {
            db.get("p:" + id, function(err, data) {
                if (data) {
                    var geohash = GeoHash.decodeGeoHash(data.toString());
                    lon += geohash.longitude[2];
                    lat += geohash.latitude[2];
                    len++;
                    cb();
                } else if (err && err.notFound) {
                    /* Ignore :-/ */
                    console.log("get p:" + id, "notFound");
                    cb();
                } else {
                    /* Ignore :-/ */
                    console.log("get p:" + id, err, data);
                    cb();
                }
            });
        }, function(err) {
            // var stop = Date.now();
            if (len > 0) {
                way.lon = lon / len;
                way.lat = lat / len;
                // console.log("looked up way", way.lon, "/", way.lat, "from", len, "in", stop - start, "ms");
            } else {
                console.log("No location for", way, err);
            }
            callback(err, way);
        });
    } else {
        console.log("Cannot expand way", way);
        callback(null, way);
    }
};


util.inherits(ToDB, Transform);
function ToDB() {
    Transform.call(this, { objectMode: true, highWaterMark: 0 });
    this.stats = {};
}

ToDB.prototype._transform = function(value, encoding, callback) {
    if (!this.stats.hasOwnProperty(value.type)) {
        this.stats[value.type] = 0;
    }
    this.stats[value.type]++;

    if (value.lat && value.lon) {
        var geohash = GeoHash.encodeGeoHash(value.lat, value.lon);
        this.push({
            type: 'put',
            key: "p:" + value.id,
            value: geohash
        });

        var isInteresting = INTERESTING.some(function(field) {
            return value.tags.hasOwnProperty(field);
        });
        if (isInteresting) {
            delete value.info;
            var key = "geo:" + geohash + ":" + value.id;
            this.push({
                type: 'put',
                key: key,
                value: JSON.stringify(value)
            });
        }
    }

    callback();
};

ToDB.prototype._flush = function(callback) {
    console.log("Processed", this.stats);
    callback();
};


util.inherits(BatchBuffer, Transform);
function BatchBuffer(batchSize) {
    Transform.call(this, { objectMode: true });
    this._readableState.highWaterMark = 1;
    this._writableState.highWaterMark = batchSize;
    this.batchSize = batchSize;
    this.buffer = [];
}

BatchBuffer.prototype._transform = function(chunk, encoding, callback) {
    this.buffer.push(chunk);
    this.canFlush(false);
    callback();
};

BatchBuffer.prototype._flush = function(callback) {
    this.canFlush(true);
    callback();
};

BatchBuffer.prototype.canFlush = function(force) {
    while((force && this.buffer.length > 0) || this.buffer.length >= this.batchSize) {
        var chunks = this.buffer.slice(0, this.batchSize);
        this.buffer = this.buffer.slice(this.batchSize);
        this.push(chunks);
    }
};


util.inherits(BatchWriter, Writable);
function BatchWriter() {
    Writable.call(this, { objectMode: true, highWaterMark: 2 });
    this.count = 0;
    this.oldCount = 0;
}

BatchWriter.prototype._write = function(chunk, encoding, callback) {
    db.batch(chunk, callback);

    this.count += chunk.length;
    if (!this.statsTimeout) {
        this.statsTimeout = setTimeout(function() {
            this.statsTimeout = null;

            console.log("Writing", Math.floor((this.count - this.oldCount) / 3), "entries/s");
            this.oldCount = this.count;
        }.bind(this), 3 * 1000);
    }
};


db.open(function(err) {
    if (err) {
        console.log("open", err);
        process.exit(1);
    }

    process.stdin
        .pipe(parseOSM({ parseInfo: false }))
        .pipe(new Expander())
        .pipe(new ToDB())
        .pipe(new BatchBuffer(CONCURRENCY))
        .pipe(new BatchWriter())
        .on('end', function() {
            db.close();
        });
});
