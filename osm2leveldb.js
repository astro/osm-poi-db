var async = require('async');
var parseOSM = require('osm-pbf-parser');
var GeoHash = require('geohash').GeoHash;
var leveldown = require('leveldown');
var db = leveldown('./osm');
var pDb = leveldown('./osm.points');
var Transform = require('stream').Transform;
var Writable = require('stream').Writable;
var util = require('util');

var INTERESTING = [
    "amenity", "emergency", "historic",
    "leisure", "public_transport", "shop",
    "sport", "tourism", "craft",
    "office", "addr:street", "addr:housenumber"
];

var CONCURRENCY = 16;

util.inherits(ToDB, Transform);
function ToDB() {
    Transform.call(this, { objectMode: true, highWaterMark: CONCURRENCY });
}

ToDB.prototype._transform = function(chunk, encoding, callback) {
    if (chunk.type === 'nodes') {
        async.eachLimit(chunk.nodes, CONCURRENCY, function(node, cb) {
            node._type = 'node';
            this.onElement(node, cb);
        }.bind(this), callback);
    } else if (chunk.type === 'way') {
        chunk.way._type = 'way';
        this.onElement(chunk.way, callback);
    }
};

ToDB.prototype.onElement = function(value, cb) {
    if (value.lat && value.lon) {
        pDb.put("p:" + value.id, JSON.stringify({
            lat: value.lat,
            lon: value.lon
        }), function(err) {
            this.pushElement(value, cb);
        }.bind(this));
    } else if (value.ref && value.ref.length > 0) {
        var lon = 0, lat = 0, len = 0;
        async.eachLimit(value.nd, CONCURRENCY, function(id, cb) {
            pDb.get("p:" + id, function(err, data) {
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
                value.lon = lon / len;
                value.lat = lat / len;
                console.log("looked up", value.lon, "/", value.lat, "from", len);
                this.pushElement(value, cb);
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

ToDB.prototype.pushElement = function(value, cb) {
    var isInteresting = INTERESTING.some(function(field) {
        return value.tags.hasOwnProperty(field);
    });
    if (isInteresting) {
        var geohash = GeoHash.encodeGeoHash(value.lat, value.lon);
        var key = "geo:" + geohash + ":" + value.id;
        this.push({
            type: 'put',
            key: key,
            value: JSON.stringify(value)
        });
        // console.log("push", key);
    }
    cb();
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
        // console.log("flush", chunks.length);
        this.push(chunks);
    }
};

util.inherits(BatchWriter, Writable);
function BatchWriter() {
    Writable.call(this, { objectMode: true, highWaterMark: CONCURRENCY });
}

BatchWriter.prototype._write = function(chunk, encoding, callback) {
    // console.log("batch", chunk.length);
    db.batch(chunk, callback);
};


process.stdin.pause();
db.open(function() {
    pDb.open(function() {
        var count = 0;
        process.stdin
            .on('data', function(data) {
                count += data.length;
                // console.log("Read", Math.floor(count/1024/1024), "MB");
            })
            .pipe(parseOSM())
        // TODO: NodeExpander
            .pipe(new ToDB())
            .pipe(new BatchBuffer(CONCURRENCY))
            .pipe(new BatchWriter())
            .on('end', function() {
                pDb.close(function() {
                    db.close();
                });
            });
        process.stdin.resume();
    });
});
