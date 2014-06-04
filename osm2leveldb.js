var async = require('async');
var parseOSM = require('osm-pbf-parser');
var GeoHash = require('geohash').GeoHash;
var leveldown = require('leveldown');
var db = leveldown('./osm');
var Transform = require('stream').Transform;
var Writable = require('stream').Writable;
var util = require('util');

var INTERESTING = [
    "amenity", "emergency", "historic",
    "leisure", "public_transport", "shop",
    "sport", "tourism", "craft",
    "office", "addr:street", "addr:housenumber"
];

var CONCURRENCY = 8;

util.inherits(Expander, Transform);
function Expander() {
    Transform.call(this, { objectMode: true, highWaterMark: CONCURRENCY });
}

Expander.prototype._transform = function(chunk, encoding, callback) {
    if (chunk.type === 'nodes') {
        chunk.nodes.forEach(function(node) {
            node._type = 'node';
            this.push(node);
        }.bind(this));
        callback();
    } else if (chunk.type === 'way') {
        chunk.way._type = 'way';
        this.expandWay(chunk.way, function(way) {
            if (way.lat && way.lon) {
                this.push(way);
            }
            callback();
        }.bind(true));
    } else {
        callback();
    }
};

Expander.prototype.expandWay = function(way, callback) {
    if (way.ref && way.ref.length > 0) {
        var lon = 0, lat = 0, len = 0;
        async.eachLimit(way.nd, CONCURRENCY, function(id, cb) {
            db.get("p:" + id, function(err, data) {
                if (data) {
                    data = JSON.parse(data.toString());
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
                way.lon = lon / len;
                way.lat = lat / len;
                // console.log("looked up", way.lon, "/", way.lat, "from", len);
            } else {
                console.log("No location for", way, err);
            }
            callback(way);
        }.bind(this));
    } else {
        callback(way);
    }
};


util.inherits(ToDB, Transform);
function ToDB() {
    Transform.call(this, { objectMode: true, highWaterMark: CONCURRENCY });
}

ToDB.prototype._transform = function(value, encoding, callback) {
    if (value.lat && value.lon) {
        this.push({
            type: 'put',
            key: "p:" + value.id,
            value: JSON.stringify({
                lat: value.lat,
                lon: value.lon
            })
        });

        var isInteresting = INTERESTING.some(function(field) {
            return value.tags.hasOwnProperty(field);
        });
        if (isInteresting) {
            var geohash = GeoHash.encodeGeoHash(value.lat, value.lon);
            var key = "geo:" + geohash + ":" + value.id;
            // console.log("push", key);
            this.push({
                type: 'put',
                key: key,
                value: JSON.stringify(value)
            });
        }
    }
    callback();
};


util.inherits(BatchBuffer, Transform);
function BatchBuffer(batchSize) {
    Transform.call(this, { objectMode: true, highWaterMark: CONCURRENCY });
    this.batchSize = batchSize;
    this.buffer = [];
}

BatchBuffer.prototype._transform = function(chunk, encoding, callback) {
    this.buffer.push(chunk);
    this.canFlush(false);
    // console.log("left", this.buffer.length, "in buffer");
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
    Writable.call(this, { objectMode: true, highWaterMark: CONCURRENCY });
}

BatchWriter.prototype._write = function(chunk, encoding, callback) {
    db.batch(chunk, callback);
};


db.open(function(err) {
    if (err) {
        console.log("open", err);
        process.exit(1);
    }

    process.stdin
        .pipe(parseOSM())
        .pipe(new Expander())
        .pipe(new ToDB())
        .pipe(new BatchBuffer(CONCURRENCY))
        .pipe(new BatchWriter())
        .on('end', function() {
            db.close();
        });
});
