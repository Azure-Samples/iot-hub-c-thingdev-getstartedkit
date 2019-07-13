'use strict';

var express = require('express');
var bodyParser = require('body-parser');

var ServiceClient = require('azure-iothub').Client;
var Message = require('azure-iot-common').Message;
var EventHubClient = require('azure-event-hubs').Client;
var DeviceConnectionString = require('azure-iot-device').ConnectionString;
var azure = require('azure-storage');
var nconf = require('nconf');

nconf.argv().env().file('./config.json');
var eventHubName = nconf.get('eventHubName');
var ehConnString = nconf.get('ehConnString');
var deviceConnString = nconf.get('deviceConnString');
var storageAcountName = nconf.get('storageAcountName');
var storageAccountKey = nconf.get('storageAccountKey');
var storageTable = nconf.get('storageTable');
var iotHubConnString = nconf.get('iotHubConnString');

var deviceId = DeviceConnectionString.parse(deviceConnString).DeviceId;
var iotHubClient = ServiceClient.fromConnectionString(iotHubConnString);

// event hub alerts
var alerts = [];
var ehclient = EventHubClient.fromConnectionString(ehConnString, eventHubName)
ehclient.createReceiver('$Default', '0', { startAfterTime: Date.now() })
.then(function(rx) {
    rx.on('errorReceived', function(err) { console.log(err); });
    rx.on('message', function(message) {
        alerts.push(message.body);
        alerts = alerts.slice(-5); // keep last 5
    });
});

// table storage
var tableSvc = azure.createTableService(storageAcountName, storageAccountKey);
tableSvc.createTableIfNotExists(storageTable, function(err, result, response) {
    if (err) {
        console.log('error looking up table');
        console.log(err)
    }
});

// website setup
var app = express();
var port = nconf.get('port');
app.use(express.static('public'));
app.use(express.static('bower_components'));
app.use(bodyParser.json());

// app api
app.get('/api/alerts', function(req, res) {
    res.json(alerts);
});

app.get('/api/temperatures', function (req, res, next) {
    var query = new azure.TableQuery()
        .select(['EventTime', 'TemperatureReading', 'DeviceId'])
        .where('PartitionKey eq ?', deviceId);
    var nextContinuationToken = null;
    var fullresult = { entries: [] };
    queryTable(storageTable, query, nextContinuationToken, fullresult, function (err, result, response) {
        res.json(result.entries.slice(-10));
    })
})

function queryTable(table, query, token, fullresult, callback) {
    var nextContinuationToken = token;
    tableSvc.queryEntities(table, query, nextContinuationToken, function (err, result, response) {
        fullresult.entries.push.apply(fullresult.entries, result.entries);
        if (result.continuationToken) {
            nextContinuationToken = result.continuationToken;
            queryTable(table, query, nextContinuationToken, fullresult, callback);
        } else {
            callback(err, fullresult, response);
        }
    })
};

var completedCallback = function(err, res) {
    if (err) { console.log(err); }
    else { console.log(res); }
};

app.post('/api/command', function(req, res) {
    console.log('command received: ' + req.body.command);

    var command = "TurnFanOff";
    if (req.body.command === 1) {
        command = "TurnFanOn";
    }

    iotHubClient.open(function(err) {
        if (err) {
            console.error('Could not connect: ' + err.message);
        } else { // {"Name":"TurnFanOn","Parameters":""}
            var data = JSON.stringify({ "Name":command,"Parameters":null });
            var message = new Message(data);
            console.log('Sending message: ' + data);
            iotHubClient.send(deviceId, message, printResultFor('send'));
        }
    });

    // Helper function to print results in the console
    function printResultFor(op) {
        return function printResult(err, res) {
            if (err) {
                console.log(op + ' error: ' + err.toString());
            } else {
                console.log(op + ' status: ' + res.constructor.name);
            }
        };
    }

    res.end();
});

app.listen(port, function() {
    console.log('app running on http://localhost:' + port);
});
