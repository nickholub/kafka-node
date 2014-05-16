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
  setInterval(function () {
    var random = Math.floor(Math.random() * 100);
    send(JSON.stringify(random));
    //send(JSON.stringify(new Date()));
  }, 500);
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