var i = 0;
var start = Date.now();
var AreaStream = require('../area_stream');
new AreaStream({
    lat: 51,
    lon: 13,
    extent: 3000
}).on('data', function(node) {
    i++;
    var t = node.tags;
    if (t.name) {
        console.log(t.name);
        if (t['addr:city']) {
            console.log("  " + t['addr:street'] + " " + t['addr:housenumber'] + ", " + t['addr:postcode'] + " " + t['addr:city'] + " (" + t['addr:country'] + ")");
        }
        Object.keys(t)
            .sort()
            .forEach(function(k) {
                if (k !== 'name' && !(/^addr:/.test(k))) {
                    console.log("  - " + k + ": " + t[k]);
                }
            });
    } else {
        console.log(
            Object.keys(t)
                .sort()
                .map(function(k) {
                    return t[k];
                })
                .join(" ")
        );
    }
}).on('end', function() {
    var now = Date.now();
    console.log("Results:", i, "items in", now - start, "ms");
});
