var express = require('express');
var connect = require('connect');
var WGS84Util = require('wgs84-util');
var AreaStream = require('./area_stream');

var INTERESTING = ["amenity", "emergency", "historic", "leisure", "public_transport", "shop", "sport", "tourism", "craft", "office"];


var app = express();

app.use(connect.compress());

app.get('/lookup/:lat/:lon/:range', function(req, res) {
    var start = Date.now();
    var lat = Number(req.param('lat'));
    var lon = Number(req.param('lon'));
    var range = Math.min(5000, Number(req.param('range')));
    console.log("lookup", lat, lon, range);
    var as = new AreaStream({
        lat: lat,
        lon: lon,
        extent: range
    });
    res.writeHead(200, {
        'Content-Type': "application/json"
    });
    res.write("{\"interesting\":[");

    var address, addressDistance, interestingCount = 0;
    as.on('data', function(data) {
        var distance = WGS84Util.distanceBetween({
            coordinates: [lat, lon]
        }, {
            coordinates: [data.lat, data.lon]
        });
        if ((!address || distance < addressDistance) &&
            data['addr:street'] && data['addr:housenumber']) {

            addressDistance = distance;
            address = {
                _distance: distance
            };
            Object.keys(data).forEach(function(k) {
                var m;
                if ((m = k.match(/^addr:(.+)/))) {
                    address[m[1]] = data[k];
                }
            });
        };

        var isInterested = distance <= range && INTERESTING.some(function(f) {
            return data.hasOwnProperty(f);
        });
        if (isInterested) {
            if (interestingCount > 0) {
                res.write(",");
            }
            var more = res.write(JSON.stringify(data));
            if (!more) {
                console.log("pause");
                as.pause();  // wait for resume
            }
            interestingCount++;
        }
    });
    res.on('drain', function() {
        console.log("drain, resume");
        as.resume();
    });
    as.on('end', function() {
        res.write("]");
        if (address) {
            res.write(",\"address\":");
            res.write(JSON.stringify(address));
        }

        var elapsed = Date.now() - start;
        res.write(",\"elapsed\":" + elapsed + "}");
        res.end();
        console.log("looked up", interestingCount, "around", lat+","+lon, "in", elapsed, "ms");
    });
});

app.use(express.static(__dirname + '/static'));
app.listen(parseInt(process.env.PORT || "8000", 10), '::');
