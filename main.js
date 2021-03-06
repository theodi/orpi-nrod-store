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
    // NOTE: for some reason stompit 'inhibits' JavaScript's 'throw', check https://github.com/gdaws/node-stomp/issues/7
    stompit = require('stompit'),
    // https://github.com/nathanpeck/s3-upload-stream
    Uploader = require('s3-upload-stream').Uploader,
    // http://underscorejs.org/
    _ = require('underscore');

var client = null,
    uploadStream = null,
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
    return d.getFullYear() + "/" + (d.getMonth() < 9 ? '0' : '') + (d.getMonth() + 1) + "/" + (d.getDate() < 10 ? '0' : '') + d.getDate() + "/arrivals_" + d.getFullYear() + (d.getMonth() < 9 ? '0' : '') + (d.getMonth() + 1) + (d.getDate() < 10 ? '0' : '') + d.getDate() + (d.getHours() < 10 ? '0' : '') + d.getHours() + (d.getMinutes() < 10 ? '0' : '') + d.getMinutes() + (d.getSeconds() < 10 ? '0' : '') + d.getSeconds() + ".csv";
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
                if (err) {
                    console.log("ERROR creating the new log file on S3: " + err.message);
                    process.exit(1);
                }   
                uploadStream = newUploadStream;
                callback(err);
            }
        );
}

var closeCurrentFile = function (callback) {
    uploadStream.end(null, null, function (err) {
        if (err) {
            console.log("ERROR closing the current log file: " + err.message);
            process.exit(1);
        } 
        uploadStream = null;
        callback(err);
    });
}

var writeQueue = async.queue(function (jsonContent, callback) {

    var dateToCSVDateWithoutSeconds = function (d) {
        return d.getFullYear() + "/" + (d.getMonth() < 9 ? '0' : '') + (d.getMonth() + 1) + "/" + (d.getDate() < 10 ? '0' : '') + d.getDate() + " " + (d.getHours() < 10 ? '0' : '') + d.getHours() + ":" + (d.getMinutes() < 10 ? '0' : '') + d.getMinutes() + ":00";
    }

    var trasformContent = function (_flattenedEntry) {
        // work on a clone!
        var flattenedEntry = JSON.parse(JSON.stringify(_flattenedEntry));
        // convert timestamps to CSV dates
        _.keys(flattenedEntry)
            .filter(function (key) { return key.match(/_timestamp$/); })
            .filter(function (key) { return flattenedEntry[key] !== ""; })
            .forEach(function (key) {
                flattenedEntry[key] = new Date(parseInt(flattenedEntry[key]) + (new Date()).getTimezoneOffset() * 60000);
                flattenedEntry[key] = dateToCSVDateWithoutSeconds(flattenedEntry[key]);
            });
        return flattenedEntry;
    }

    var flattenedContent = trasformContent(flattenObject(jsonContent)),
        sortedKeys = Object.keys(flattenedContent).sort(),
        sortedValues = sortedKeys.map(function (key) { return JSON.stringify(flattenedContent[key]); }).join(",");
        now = new Date();
    async.series([
        // check if I need to close the existing file
        function (callback) {
            if (uploadStream && lastRecordWritten) {
                if (now.getHours() !== lastRecordWritten.getHours()) {
                    closeCurrentFile(callback);
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

var saveCurrentFileAndDisconnect = function (callback) {

    var clientDisconnect = function () {
        client.disconnect(function (err) {
            if (err) {
                console.log("ERROR disconnecting the client from NROD: " + err.message);
                process.exit(1);
            } 
            (callback || function () { })(null);
        });
    }

    if (uploadStream) {
        uploadStream.end(null, null, function (err) {
            if (err) {
                console.log("ERROR ending the upload stream to S3: " + err.message);
                process.exit(1);
            } 
            clientDisconnect();
        });
    } else {
        clientDisconnect();
    }
}

var run = function () {
    stompit.connect(CONNECTION_PARAMETERS, function (err, _client) {
        if (err) {
            console.log("ERROR connecting to NROD: " + err.message);
            process.exit(1);
        } 
        client = _client;
        client.subscribe(SUBSCRIPTION_PARAMETERS, function (err, message) {
            if (err) {
                saveCurrentFileAndDisconnect(function () { 
                    // note I am ignoring the err returned from 
                    // saveCurrentFileAndDisconnect intentionally
                    console.log("ERROR subscribing to the feed: " + err.message);
                    process.exit(1);
                });
            } else {
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
                                return (
                                    // Not a freight train
                                    (message.body.toc_id !== '0') 
                                    // See https://groups.google.com/d/msg/openraildata-talk/A-3pV_5ZfNc/EHwPu8v78WsJ .
                                    // The two conditions below are necessary
                                    // to identify real, in-service passengers 
                                    // trains without matching the record
                                    // against the schedule. This information is
                                    // not in the NROD wiki! The train classes
                                    // are described at http://en.wikipedia.org/wiki/Train_reporting_number#Train_classes 
                                    && (message.header.source_system_id === 'TRUST')  
                                    && _.contains(['1', '2', '9'], message.body.train_id.substring(2, 3))  
                                    // Not interested in other events than
                                    // departures and arrivals.
                                    && _.contains(['DEPARTURE', 'ARRIVAL'], message.body.event_type)
                                );
                            })
                            .forEach(function (message) {
                                write(message);
                            })
                        content = '';
                    } else {
                        saveCurrentFileAndDisconnect();
                    }
                });
            }
        });
    });
}

// login fails if you try too soon after a previous disconnection
setTimeout(run, 10000);
