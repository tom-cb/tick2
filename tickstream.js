// TickStream class

// This is a readable stream class which simulates tick data being read from a file in CSV format
// Instead of generating large files of sample data to read in
// this class can be used to generate a stream of equivalent CSV lines on the fly
// This eliminates any issues around file IO bandwidth
// and gives more flexibility around the number and type of ticks to be used.

// The format of tick that we are generating is as follows:
// "EUR/GBP,20130101 21:59:59.592,0.81156,0.81379\n"
// Instead of using real currency pair names, we're generating these
// Along lines of AA0/BB0, AA1/BB1, etc.
// This is so we can easily vary the number of currency pairs generated.

// The other main feature of this stream is that we want to control the rate ticks are generated
// We do this by setting a number of milliseconds that are waited each time before we fill the buffer.

require('datejs');
var Readable = require('stream').Readable;
var util = require('util');

util.inherits(TickStream, Readable);

var WAIT_MILLIS = 100;
var CURRENCY_PAIRS = 10;
var MAX_TICKS = 100000;
var CURRENCY_ONE = process.env.CURRENCY_ONE;
var CURRENCY_TWO = process.env.CURRENCY_TWO;


function TickStream(opt) {
  Readable.call(this, opt);
  this._max = MAX_TICKS;
  this._index = 1; 
  this._waiting = false;
  this._timeIndex = Date.today();
  this._timeIndex.setTimeToNow();
}

TickStream.prototype._read = function() {
 var i = this._index++;

 if (i > this._max || this._waiting == true) {
  // We've reached max ticks, or we're in our wait period
  this.push(null);
 }
 else if (this._waiting == false) {
  this._waiting = true;
  // generate tick data WAIT_MILLIS in future
  setTimeout(genData, WAIT_MILLIS, this);
 } 
};

function genData(stream) {
  var now = new Date().setTimeToNow();
  var span = new Date.TimeSpan(now - stream._timeIndex);
 
  //console.log(span.getMilliseconds());
 
  stream._timeIndex.setTimeToNow();
  // Mark that we've finished the wait period
  stream._waiting = false;

  // This section is just date time manipulation to create date string in right format
  millis = String(stream._timeIndex.getMilliseconds());
  if (millis.length == 2) { millis = '0' + millis; }
  else if (millis.length == 1) {millis = '00' + millis; }
 
  dateStr = stream._timeIndex.toString("yyyyMMdd HH:mm:ss") + "." + millis; 

  //console.log("datetime: " + dateStr);

  output = '';
  // Build a string with N lines, each representing a currency pair
  // The price values are still fixed at 0.81... for now.
  for (i = 0; i < CURRENCY_PAIRS; i++) {
    var str = CURRENCY_ONE +i+ '/' + CURRENCY_TWO +i+',' + dateStr + ',0.81156,0.81379\n';
    //console.log(str);
    output += str;
  }

  // Push the N lines of tick data CSV into the stream buffer
  var buf = new Buffer(output, 'ascii');
  stream.push(buf);
}


module.exports = TickStream
