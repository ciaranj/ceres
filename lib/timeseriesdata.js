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
exports.TimeSeriesData= TimeSeriesData;
