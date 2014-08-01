var NO_EVENTS_WARNING = 5, // minutes
    CONNECTION_PARAMETERS = {
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

 
var async = require('async'),
    // https://github.com/gdaws/node-stomp
    stompit = require('stompit'),
    s3Writer = require('./s3-writer'),
    utils = require('./utils'),
    _ = require('underscore');


var file = null,
    firstBatch = null,
    latestEventsTimestamp = null,
    latestWrittenEventsTimestamp = null,
    disconnectAtNextOpportunity = false;


var generateFilename = function () {
    var d = new Date();
    return 'arrivals_' + d.getFullYear() + (d.getMonth() < 9 ? '0' : '') + (d.getMonth() + 1) + (d.getDate() < 10 ? '0' : '') + d.getDate() + (d.getHours() < 10 ? '0' : '') + d.getHours() + '.csv';
};


var arrivalsProcessingQueue = async.queue(function (event, callback) {

    var startNewFile = function (callback) {
        async.series([
            // if a file was being written, I close it
            function (callback) {
                if (file) {
                    file.close(callback);
                } else {
                    callback(null);
                }
            },
            // start the new file
            function (callback) {
                firstBatch = true;
                s3Writer.create(generateFilename(), function (err, _file) {
                    if (err) throw err;
                    file = _file;
                    callback(null);
                });
            }
        ], callback);
    }

    var write = function() {
        var newEvent = { },
            newEventCSV = null,
            columnNames = Object.keys(event).reduce(function (memo, firstLevel) {
                return memo.concat(Object.keys(event[firstLevel]).map(function (secondLevel) {
                    var newColumnName = firstLevel + "_" + secondLevel;
                    newEvent[newColumnName] = event[firstLevel][secondLevel];
                    return newColumnName;
                }));
            }, [ ]).sort();
        async.series([
            // if this the first data in the new file, write the CSV header
            function (callback) {
                if (firstBatch) {
                    newEventCSV = columnNames.join(",");
                    if (process.env.DEBUG) utils.log(newEventCSV);
                    file.write(newEventCSV, callback);
                    firstBatch = false;
                } else {
                    file.write("\n", callback);
                }
            },
            // then write the actual data
            function (callback) {
                latestWrittenEventsTimestamp = new Date();
                newEventCSV = columnNames.map(function (columnName) { 
                    return newEvent[columnName] ? JSON.stringify(newEvent[columnName]) : "";
                }).join(",");
                if (process.env.DEBUG) utils.log(newEventCSV);
                file.write(newEventCSV, callback);
            }
        ], callback);
    }

    if (!latestWrittenEventsTimestamp || (latestWrittenEventsTimestamp.getHours() !== (new Date()).getHours())) {
        startNewFile(write);
    }  else {
        write();
    }

}, 1);


var initialise = function () {

    setInterval(function () {
    	if (latestEventsTimestamp && ((new Date()).getTime() - latestEventsTimestamp.getTime() > NO_EVENTS_WARNING * 60000)) {
    		utils.log("arrivalsMonitor: *** WARNING: more than " + NO_EVENTS_WARNING + " minutes without receiving events from the server.");		
    	}
    }, 60000);

    stompit.connect(CONNECTION_PARAMETERS, function (err, client) {
        if (err) {
            utils.log('arrivalsMonitor: unable to connect listener to National Rail server - ' + err.message);
            return;
        }
        utils.log("arrivalsMonitor: listener started.");
        client.subscribe(SUBSCRIPTION_PARAMETERS, function (err, message) {
            if (err) {
                utils.log('arrivalsMonitor: error receiving message - ' + err.message);
                return;
            }
            var content = '',
                chunk;
            message.on('readable', function () {
                    while (null !== (chunk = message.read())) { content += chunk; }
                });
            message.on('end', function () {
                    message.ack();
                    latestEventsTimestamp = new Date();
                    arrivalsProcessingQueue.push(JSON.parse(content).filter(function (e) { 
                        // NOTE
                        // The line below drops all events that are not 
                        // arrivals; arrivals at final destinations can be 
                        // distinguished from arrivals at intermediate stations 
                        // by checking for 'DESTINATION' in 
                        // body.planned_event_type. Here is also where you could 
                        // decide to drop information about trains that were not 
                        // delayed.
                        return e.body.event_type === 'ARRIVAL'; 
                    }), function (err) { 
                        if (disconnectAtNextOpportunity) {
                            file.close(function (err) {
                                client.disconnect(function (err) {
                                    utils.log("Exiting gracefully.");
                                    process.exit();
                                });
                            });
                        }
                    });
                });
        });
    });
};

process.once('SIGTERM', function () {
    utils.log("SIGTERM received, exiting at the first opportunity...");
    disconnectAtNextOpportunity = true;
});

initialise();

