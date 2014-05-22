var kafka = require('../kafka'),
  Producer = kafka.Producer,
  Client = kafka.Client;

var argv = require('optimist').argv;
var connectionString = argv.zookeeper || 'localhost:2181';
var topic = argv.topic || 'test';
var p = argv.p || 0;
var count = argv.count || 1, rets = 0;
var client = new Client(connectionString);
var producer = new Producer(client);

producer.on('ready', function () {
  var message = {
    "dimensionSelector": "time=MINUTES:publisherId:type",
    "numTimeUnits": "20",
    "publisherId": "1",
    "type": "1"
  }
  send(JSON.stringify(message));
});

function send(message) {
  for (var i = 0; i < count; i++) {
    producer.send([
      {topic: topic, messages: [message] , partition: p}
    ], function (err, data) {
      //console.log(err);
      //if (err) console.log(arguments);
      //if (++rets === count) process.exit();
    });
  }
}