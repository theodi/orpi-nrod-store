var CONNECTION_PARAMETERS = {
            'host': 'datafeeds.networkrail.co.uk', 
            'port': 61618, 
            'connectHeaders': {
                'host': '/',
                'login': process.env.NROD_USERNAME,
                'passcode': process.env.NROD_PASSWORD,
                'client-id': ((process.env.DEBUG !== 'true') ? process.env.NROD_USERNAME : undefined),
            }
        },
    SUBSCRIPTION_PARAMETERS = {
            'destination': '/topic/TRAIN_MVT_ALL_TOC',
            'ack': 'client-individual',
            'activemq.subscriptionName': ((process.env.DEBUG !== 'true') ? 'prod-' + process.env.NROD_USERNAME : undefined),
        };

var // https://github.com/caolan/async
    async = require('async'),
    // https://github.com/gdaws/node-stomp
    stompit = require('stompit'),
    // https://github.com/nathanpeck/s3-upload-stream
    Uploader = require('s3-upload-stream').Uploader,
    // http://underscorejs.org/
    _ = require('underscore');

var uploadStream = null,
    lastRecordWritten = null, 
    disconnectAtNextOpportunity = false;

// flattens a JavaScript object, e.g. { "foo": { "bar": 1 }} becomes 
// { "foo.bar": 1 }
var flattenObject = function (o, prefix) {
    if (!_.isObject(o)) return o;
    return _.keys(o).reduce(function (memo, key) {
        if (!_.isObject(o[key])) {
            memo[(prefix ? prefix + "." : "") + key] = o[key];
        } else {
            var subObject = flattenObject(o[key], key);
            _.keys(subObject).forEach(function (subKey) {
                memo[(prefix ? prefix + "." : "") + subKey] = subObject[subKey]
            });
        }
        return memo;
    }, { });
}

var dateToFilename = function (d) {
    return "arrivals_" + d.getFullYear() + (d.getMonth() < 9 ? '0' : '') + (d.getMonth() + 1) + (d.getDate() < 10 ? '0' : '') + d.getDate() + (d.getHours() < 10 ? '0' : '') + d.getHours() + (d.getMinutes() < 10 ? '0' : '') + d.getMinutes() + (d.getSeconds() < 10 ? '0' : '') + d.getSeconds() + ".csv";
}

var createNewFile = function (callback) {
    var UploadStreamObject = new Uploader(
            { 
                "accessKeyId": process.env.AWS_ACCESS_KEY_ID,
                "secretAccessKey": process.env.AWS_SECURE_ACCESS_KEY,
            },
            {
                "Bucket": process.env.AWS_ARRIVALS_ARCHIVE_BUCKET_NAME,
                "Key": dateToFilename(new Date()),
                "ACL": 'public-read',
                "StorageClass": 'REDUCED_REDUNDANCY',
            },
            function (err, newUploadStream) {
                if (err) throw err;
                uploadStream = newUploadStream;
                callback(null);
            }
        );
}

var writeQueue = async.queue(function (jsonContent, callback) {
    var flattenedContent = flattenObject(jsonContent),
        sortedKeys = Object.keys(flattenedContent).sort(),
        sortedValues = sortedKeys.map(function (key) { return JSON.stringify(flattenedContent[key]); }).join(",");
        now = new Date();
    async.series([
        // check if I need to close the existing file
        function (callback) {
            if (uploadStream && lastRecordWritten) {
                if (now.getHours() !== lastRecordWritten.getHours()) {
                    uploadStream.end(null, null, function (err) {
                        uploadStream = null;
                        callback(null);
                    });
                } else {
                    callback(null);
                }
            } else {
                callback(null);
            }
        },
        // checks if I need to start a new file, including the CSV header
        function (callback) {
            if (!uploadStream) {
                createNewFile(function (err) {
                    var csvHeader = sortedKeys.map(function (key) { return JSON.stringify(key); }).join(",");
                    if (process.env.DEBUG === 'true') console.log(csvHeader);
                    uploadStream.write(csvHeader, callback);
                });
            } else {
                callback(null);
            }
        },
        // do the actual writing
        function (callback) {
            lastRecordWritten = now;
            if (process.env.DEBUG === 'true') console.log(sortedValues);
            uploadStream.write("\n" + sortedValues, callback);               
        }
    ], callback);
}, 1);

var write = function (content, callback) {
    writeQueue.push(content, callback);
}

process.once('SIGTERM', function () {
    disconnectAtNextOpportunity = true;
});

var run = function () {
    stompit.connect(CONNECTION_PARAMETERS, function (err, client) {
        if (err) throw err;
        client.subscribe(SUBSCRIPTION_PARAMETERS, function (err, message) {
            if (err) throw err;
            var content = '',
                chunk;
            message.on('readable', function () {
                    while (null !== (chunk = message.read())) { content += chunk; }
                });
            message.on('end', function () {
                if (!disconnectAtNextOpportunity) {
                    message.ack();
                    JSON.parse(content)
                        .filter(function (message) {
                            return (message.body.event_type === 'ARRIVAL') || (message.body.event_type === 'DEPARTURE');
                        })
                        .forEach(function (message) {
                            write(message);
                        })
                    content = '';
                } else {
                    uploadStream.end(null, null, function (err) {
                        client.disconnect();
                    });
                }
            });
        });
    });
}

// console.write(Object.keys("cippa"));
// console.log(typeof({ cippa: 3 }));
// console.log(flattenObject({ cippa: 3, lippa: { sippa: 2, guppa: 1 } }));
run();