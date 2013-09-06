var assert = require( "assert" )
, TimeSeriesData = require( "../lib/timeseriesdata" ).TimeSeriesData
, fs = require( "fs" )
, sinon = require( "sinon" );

describe("TimeSeriesData", function(){
  var time_series;
  beforeEach(function(){
    time_series= new TimeSeriesData(0,50,5, [0,1,2,3,4,5,6,7,8,9]);
  });
  describe("timestamps()", function(){
    it("should correctly calculate the timestamps", function() {
      var timestamps= time_series.timestamps();
      assert.notEqual( timestamps, null );
      var tsCount= 0;
      timestamps.each( function(ts){
        tsCount++;
        if( tsCount == 1 ) assert.equal(ts, 0);
        if( tsCount == 10 ) assert.equal(ts, 45);
      })
      assert.equal(tsCount, 10);
    });
  });
  describe("data()", function(){
    it("should correctly produce an iterator for the timestamps and values", function() {
      var values= time_series.data();
      var valueCount= 0;
      values.each(function(value){
        valueCount++;
        if( valueCount == 1) assert.deepEqual(value, [0,0]);
        if( valueCount == 10) assert.deepEqual(value, [45,9]);
      });
      assert.equal(valueCount, 10);
    });
  });
  describe("merge()", function(){
    it("should have no effect if the time series has no gaps", function() {
      var other_series = new TimeSeriesData(0, 25, 5, [1.0, 4.0, 9.0, 16.0, 25.0])
      var original_values= time_series.data().force();
      time_series.merge(other_series);
      assert.deepEqual( original_values, time_series.data().force());
    });
    it("should replace empty values (nulls) with the merged in values", function() {
      var new_series = new TimeSeriesData(0, 50, 5, [null,null,null,null,null,null,null,null,null,null]);
      new_series.merge(time_series)
      assert.deepEqual(time_series.data().force(), new_series.data().force())
    });
    it("should handle time series data values with mixed nulls", function() {
      var values= [];
      for(var x=0;x<10;x++) {
        if( x % 2 == 0) values.push( x );
        else values.push( null );
      }
      var new_series = new TimeSeriesData(0, 50, 5, values)
      new_series.merge(time_series)
      assert.deepEqual(time_series.data().force(), new_series.data().force())
    });
  });
});
