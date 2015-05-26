// loader.js class for inserting tick data into Couchbase
// Input is a readable stream of tick data in CSV format E.g.
// EUR/GBP,20130101 21:59:59.592,0.81156,0.81379\n

// Goals:
// Each tick is inserted into Couchbase with a key in format:
// <currently disabled features>

// In addition there is a document that represents each minute in time.
// As ticks are processed, the doc for that minute is retrieved
// and updated to include the latest tick.

// If the minute doc doesnt exist (new minute started), one is created.
// When the minute doc is updated, it is done using a CAS operation
// This protects us from case where multiple ticks arrive very quickly,
// and the minute doc is updated after we have read it, but before we have written.

// Note: the rest of this class is best read from the bottom up!

var couchbase = require('couchbase');
var Transform = require('stream').Transform
  , csv = require('csv-streamify')
  , JSONStream = require('JSONStream')
  , util = require('util')
  , _ = require('lodash')
  , async = require('async');

var tickStream = require('./tickstream.js');
var sleep = require('sleep');

var csvToJson = csv({objectMode: true});
var parser = new Transform({objectMode: true});

// Hard coded format of our CSV
var COLUMNS_LINE = 'pair,date,a,b';
var COLUMNS = COLUMNS_LINE.split(',');

// Counters to keep track of how many failures we have of each type
// If the number of casMismatches is going up quickly, there's an issue
var getFailures = 0;
var addFailures = 0;
var casMismatches = 0;

var cbCluster = new couchbase.Cluster('couchbase://localhost');
var cbGet = cbCluster.openBucket('default');
var cbSet = cbCluster.openBucket('default');

// Two CB objects, one we use for Gets and one for Sets
//var cbGet = new couchbase.Connection({host: 'localhost:8091', bucket: 'default'});
//var cbSet = new couchbase.Connection({host: 'localhost:8091', bucket: 'default'});

parser.header = null;
parser._rawHeader = [];

var dbGetQueue = async.queue(function(data, callback) {

  // We have our tick data, and our key, format of key is:
  // 20140921 11:03AA5/BB5 
  // I.e. a doc representing the whole of minute 11:03 for currency pair AA5/BB5
  var tick = data.value;
  var key = data.key;
  //console.log(tick);
  //console.log("key: " + key);
  //console.log("data: \n" + JSON.stringify(data));

  // Uncomment to enable tracking of time spent on db call
  //console.time("dbget" + key);

  // Attempt to retrieve the doc representing this minute
  cbGet.get(key, function(err, result) { 
    //console.log(result);
    if(_.isEmpty(result)) {
      // The doc doesn't exist. This is normal for the first tick of every new minute.

      //console.timeEnd("dbget" + key);
      getFailures++;
      //console.log("get Failues: " + getFailures);

      // mt object representing a minute document
      var mt = {};
      mt.pair = tick.pair;
      mt.count = 1;
      mt.date = data.date;
      mt.total_a = tick.a;
      mt.total_b = tick.b;
      mt.avg_a = tick.a;
      mt.avg_b = tick.b;
      mt.ticks = [];
      mt.ticks.push(tick);


      // Key-Value of key->mt for storing to DB
      var mtData = {};
      mtData.key = key;
      mtData.value = mt;
      mtData.opType = 'add';
      //console.log("storing first rollup: %j", mtData);

      dbQueue.push(mtData, function(err,result) {
        if (err) { 
            //console.log(err);
            //console.log("err code:" + err.code);
          if (err.code === 12) {
              //Someone else created the new minute doc before us
              //Push this tick back into the async queue for retry

              console.log("Clash on add, retrying read");
              addFailures++;
              dbGetQueue.push(data, function(err,result) {
              //if (typeof(err) != 'undefined') { console.log(err) }
            });
          }
        }
       else if (data.timeResolution === 'seconds') {
         //console.log("successful op at seconds res");
         //data.key = tick.date.slice(0,-7) + tick.pair;
         //data.timeResolution = 'minutes';
         //dbGetQueue.push(data, function(err,result) {
         //     //if (typeof(err) != 'undefined') { console.log(err) }
         //   });
       }
      });
    } else {
      // Successfully retrieved doc for this minute
      // Update the existing minute rollup

      //console.timeEnd("dbget" + key);
      //console.log("res: %j", result);
      var val = result.value;
//      val.count++;
      if (data.timeResolution === 'minutes') {
        var match = false;
        var tot_a = 0;
        var tot_b = 0;
        for (var i = 0; i < val.ticks.length; i++) {
          //console.log("testing compare of: " + val.ticks[i].date + " with " + data.date);
          if (val.ticks[i].date === data.date) {
            //console.log("Found matching tick: " + data.date); 

            //This is broken, im just storing latest value for second, not avg!
            val.ticks[i].a = Number(tick.a);
            val.ticks[i].b = Number(tick.b);
            match = true;
          }

          tot_a += val.ticks[i].a;
          tot_b += val.ticks[i].b;
        }

        if (match === false) {
          val.count++;
          tot_a += Number(tick.a);
          tot_b += Number(tick.b);
          val.ticks.push(tick);
        }

        val.total_a = tot_a;
        val.total_b = tot_b;

        val.avg_a = (val.total_a / val.count); 
        val.avg_b = (val.total_b / val.count); 
        //console.log("tot_a: " + tot_a + " tot_b: " + tot_b + " count: " + val.count + " avg_a: " + val.avg_a + " avg_b:" + val.avg_b);
      }
      else {
       val.count++;
       val.total_a = Number(val.total_a) + Number(tick.a);
       val.total_b = Number(val.total_b) + Number(tick.b);
       val.avg_a = (val.total_a / val.count); 
       val.avg_b = (val.total_b / val.count); 
       val.ticks.push(tick);
      }
      val.date = data.date;
      var resData = {};
      resData.key = key;
      resData.value = val;
      resData.opType = 'set';
      resData.setOptions = {};
      resData.setOptions.cas = result.cas;
      //console.log("CAS: %j" , resData.setOptions.cas); 
      //console.log("updated rollup: %j", resData);

      // Push the updated minute rollup on queue to be written to DB
      dbQueue.push(resData, function(err,result) {
        if (err) { 
          //console.log(err);
          //console.log("err code:" + err.code);
          if (err.code === 12) {
            //Someone modified this minute rollup inbetween us reading and writing the update
            // Schedule this tick to be pushed back into the async queue for processing, 100milliseconds in future
            setTimeout(function(data, err, result) {
              //console.log("CAS mismatch: " + data.key);
              casMismatches++;
              dbGetQueue.push(data, function(err,result) {
                if (typeof(err) != 'undefined') { console.log(err) }
              });
            }, 100, data, err, result);//setTimeout
         }
        }
       else if (data.timeResolution === 'seconds') {

         var sTick = {};
         sTick.a = resData.value.avg_a;
         sTick.b = resData.value.avg_b;
         sTick.pair = resData.value.pair;
         sTick.date = resData.value.date.slice(0,-4);

         //console.log("sTick: \n" + JSON.stringify(sTick));

        var stickData = {};
        stickData.key = sTick.date.slice(0,-3) + tick.pair;
        stickData.date = sTick.date;
        stickData.timeResolution = 'minutes';
        stickData.value = sTick;
        stickData.opType = 'set';

        //console.log("stickData: \n" + JSON.stringify(stickData));
    dbGetQueue.push(stickData, function(err,result) {
      //if (typeof(err) != 'undefined') { console.log(err) }
      //console.timeEnd("processTick");
    });
       }
      }); 
    }
    callback(err, result);
  });
}, 10); // concurrency val for async queue

var dbQueue = async.queue(function(data, callback) {
  // Store data to the DB
  // data should include an optype (set or add)

  // console.log(data.key + " %j", data.value);
  if (data.opType === 'set') {
     //console.log("setting: " + data.key);
	 //console.log("storing: " + data.key + data.value + data.setOptions);
     cbSet.upsert(data.key, data.value, data.setOptions, function(err, result) {
       if (err) console.log("SET FAILED: " + err);
       if (casMismatches > 100) { 
         console.log("cas mismatches: " + casMismatches);
       }

       callback(err, result);
     });
  } else if (data.opType === 'add') {
     //console.log("adding: " + data.key);
	 //console.log("storing: " + data.key + " value: " +  JSON.stringify(data.value,null, 4) );
     cbSet.insert(data.key, data.value, function(err, result) {
       if (err) console.log("ADD FAILED: " + err);
       //console.log("add failures: " +  addFailures);
       callback(err, result);
     });
  } else {
     console.log("INVALID OPTYPE FOR STORE");
  }
}, 10); //concurrency val for async queue


// If the db queue is saturated, we pause the parser
// This pauses new data being pushed onto the queue
// When the queue is empty, we resume the paser
dbGetQueue.saturated = function() {
  parser.pause();
}

dbGetQueue.empty = function() {
  parser.resume();
}

dbQueue.saturated = function() {
  parser.pause();
}

dbQueue.empty = function() {
  parser.resume();
}

parser._transform = function(data, encoding, done) {
    //Enable logging of time taken to process each tick to console
    //console.time("processTick");

    //Some formatting of the input data into JSON object
    var tick = this._parseRow(data);
    //console.log("tick: \n" + JSON.stringify(tick));
    var tickData = {};

    tickData.key = tick.date.slice(0,-4) + tick.pair;
    tickData.date = tick.date;
    tickData.timeResolution = 'seconds';
    tickData.value = tick;
    tickData.opType = 'set';
    //console.log(tickData);

    // Push this tick into an async queue for processing
    // (dbGetQueue is a bad name, it does several pieces of processing)
    // See var dbGetQueue = async.queue section after this 
    dbGetQueue.push(tickData, function(err,result) {
      //if (typeof(err) != 'undefined') { console.log(err) }
      //console.timeEnd("processTick");
    });

    this.push(tick);

// Uncommenting this section will enable storing every tick as a separate doc
// In addition to storing the ticks within the minute rollup docs.
/*
    dbQueue.push(tickData, function(err, result) {
      if (typeof(err) != 'undefined') { console.log(err) }
    });
*/

  // I've forgotten why I wrapped this in setTimeout for 1ms
  setTimeout(function() {
    done();
  }, 1);

};

parser._parseRow = function(row) {
  var result = _.zipObject(COLUMNS, row);
  return result;
};

var jsonToStrings = JSONStream.stringify(false);


//THIS IS OUR STARTING POINT
// ts object creates stream of tick data in CSV format
// this is pipied into a csvToJson converter, and then piped
// into our customer parser. See _transform function after this

var ts = new tickStream();
//process.stdin
ts.pipe(csvToJson)
  .pipe(parser);

