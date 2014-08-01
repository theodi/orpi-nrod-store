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

// https://github.com/gdaws/node-stomp
var async = require('async'),
    stompit = require('stompit'),
    Uploader = require('s3-upload-stream').Uploader;

var uploadStream = null,
    lastRecordWritten = null, 
    disconnectAtNextOpportunity = false;

var dateToFilename = function (d) {
    return d.getFullYear() + (d.getMonth() < 9 ? '0' : '') + (d.getMonth() + 1) + (d.getDate() < 10 ? '0' : '') + d.getDate() + (d.getHours() < 10 ? '0' : '') + d.getHours() + (d.getMinutes() < 10 ? '0' : '') + d.getMinutes() + (d.getSeconds() < 10 ? '0' : '') + d.getSeconds();
}

var createNewFile = function (callback) {
    var UploadStreamObject = new Uploader(
            { 
                "accessKeyId": process.env.AWS_ACCESS_KEY_ID,
                "secretAccessKey": process.env.AWS_SECURE_ACCESS_KEY,
            },
            {
                "Bucket": process.env.AWS_ARRIVALS_ARCHIVE_BUCKET_NAME,
                "Key": dateToFilename(new Date()) + ".txt",
                "ACL": 'public-read',
                "StorageClass": 'REDUCED_REDUNDANCY',
            },
            function (err, newUploadStream) {
                if (err) {
                    callback(err);
                } else {
                    uploadStream = newUploadStream;
                    callback(null);
                }
            }
        );
}

var writeQueue = async.queue(function (content, callback) {
    var now = new Date();
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
        // checks if I need to start a new file
        function (callback) {
            if (!uploadStream && !disconnectAtNextOpportunity) {
                createNewFile(callback);
            } else {
                callback(null);
            }
        },
        // do the actual writing
        function (callback) {
            if (!disconnectAtNextOpportunity) {
                lastRecordWritten = now;
                uploadStream.write(content + "\n", callback);               
            }
        }
    ], callback);
}, 1);

var write = function (content, callback) {
    writeQueue.push(content, callback);
}

process.once('SIGTERM', function () {
    utils.log("SIGTERM received, exiting at the first opportunity...");
    disconnectAtNextOpportunity = true;
});

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
                JSON.parse(content).map(function (message) {
                    process.stdout.write(".");
                    write(JSON.stringify(message));
                })
                content = '';
            } else {
                client.disconnect();
            }
        });
    });
});
