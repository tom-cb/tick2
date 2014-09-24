var http = require("http"), server;

var async = require('async');
var couchbase = require('couchbase');
require('datejs');

var CURRENCY_ONE = process.env.CURRENCY_ONE;
var CURRENCY_TWO = process.env.CURRENCY_TWO;


var cbGet = new couchbase.Connection({host: 'localhost:8091', bucket: 'default'});

var opts = {};

function randomInt (low, high) {
    return Math.floor(Math.random() * (high - low) + low);
}

server = http.createServer(function (request, response) {
  console.log("processing request");

  // choose between 1 and 5 minutes worth of data
  var datePeriod = randomInt(1,10);
  console.log("rand: " + datePeriod);
  // We're using 10 seconds in the past as our start time
  // Just because we may not have generated the data for this second yet
  var endTime = new Date().setTimeToNow().addSeconds(-10);
  //var startTime = new Date().setTimeToNow().addMinutes(-datePeriod);
  var startTime = new Date().setTimeToNow().addSeconds(-10).addSeconds(-datePeriod);
  console.log(startTime.toString());
  console.log(endTime.toString());

  response.writeHead(200, {
                "Content-Type": "text/plain"
  });


  var keys = [];

  while (startTime.isBefore(endTime)) {
    //var dateStr = startTime.toString("yyyyMMdd HH:mm") + CURRENCY_ONE + "/" + CURRENCY_TWO;
    var dateStr = startTime.toString("yyyyMMdd HH:mm:ss") + CURRENCY_ONE + "/" + CURRENCY_TWO;
//    console.log(dateStr);
    keys.push(dateStr);
    //startTime.addMinutes(1);
    startTime.addSeconds(1);
  }

  console.time('req');
  cbGet.getMulti(keys, opts,
    function(err, results) {
      var res = '';
      for (var k in results) {
        console.log(k);
        console.log(JSON.stringify(results[k], null, 4));
        var res = res +  JSON.stringify(results[k].value);
      } 
      response.write(res);
      console.timeEnd('req');
      response.end();
  });
});

server.listen(6666);

console.log("server running");

server.on('error', function(err){
    console.log(err);
    process.exit(1);
});


