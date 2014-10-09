App for loading tick data

# What is a tick?
Financial price info at a specific time.
In the CSV format we are using a tick looks as follows:
    EUR/GBP,20130101 21:59:59.592,0.81156,0.81379

This corresponds to:
    Pair,Date,a,b
(Where a and b probably represent something like high/low, or bid/ask)

# What's this app?
loader.js reads in a stream of tick data in CSV format and inserts it into DB.


As each tick is handled, a document representing 1 second is updated.
All ticks within that second will be included in the 1 doc.
When a new second starts, a new doc will be created.

In addition, a doc is created that has data for each minute.
This contains a maximum of 60 values -- one representing the 'averages' for each second doc.



tickstream.js generates the input data in CSV format.
This removes the need to have large sample CSV files as input.


Sample document for a 1 minute rollup which contains 2 ticks:

```javascript
{
  "pair": "AA0/BB0",
  "count": 2,
  "total_a": 1.62312,
  "total_b": 1.62758,
  "avg_a": 0.81156,
  "avg_b": 0.81379,
  "ticks": [
    {
      "pair": "AA0/BB0",
      "date": "20140921 10:52:48.588",
      "a": "0.81156",
      "b": "0.81379"
    },
    {
      "pair": "AA0/BB0",
      "date": "20140921 10:52:48.629",
      "a": "0.81156",
      "b": "0.81379"
    }
  ]
}
```
