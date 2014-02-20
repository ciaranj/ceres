var sloth= require("sloth");
/*
  Constructs a TimeSeriesData object instance
  @param {Number} startTime - The initial timestamp
  @param {Number} endTime - The final timestamp
  @param {Number} timeStep - The interval between consecutive timestamps
  @param {Array} values - The list of values encapsulated within this TimeSeriesData instance.
*/
var TimeSeriesData= function(startTime, endTime, timeStep, values) {
  this.startTime= startTime;
  this.endTime= endTime;
  this.timeStep= timeStep;
  this.values= values;
};

TimeSeriesData.prototype.timestamps= function() {
  return sloth.range(this.startTime, this.endTime, this.timeStep);
}

TimeSeriesData.prototype.data= function() {
  return this.timestamps().zip(this.values);
}

/*
 'Merges' in another TimeSeriesData instance into this one.
 Will ignore timestamp values in 'other' that are earlier than
 the startime of this instance, and those that are later than
 the endtime of this instance.  Also skips null values from the merge.
 @param {TimeSeriesData} other - The other timeseries to merge into this instance (leaves other un-changed)
*/
TimeSeriesData.prototype.merge= function(other) {
  var that= this;
  other.data().each( function(tsValueTuple, i) {
    var ts= tsValueTuple[0];
    var value= tsValueTuple[1];
    if( typeof(value) == 'undefined' || value === null ) return;
    ts -= ( ts % that.timeStep );
    if( ts < that.startTime ) return;
    var index= Math.floor( (ts - that.startTime) / that.timeStep );
    if( index < that.values.length && (typeof(that.values[index]) == 'undefined' || that.values[index] === null )  ) {
      that.values[index]= value;
    }
  });
}

exports.TimeSeriesData= TimeSeriesData;
