var  CeresConstants= require('./ceresconstants')
   , CeresErrors= require('./cereserrors')
   , fs= require("fs")
   , mkdirp= require('mkdirp')
   , path= require("path")
   , TimeSeriesData= require("./timeseriesdata").TimeSeriesData;

var DATAPOINT_SIZE= 8;

/*
Represents a particular timeslice for a particular node.

@param {CeresNode} node - The node which this slice forms part of teh data for
@param {Number} startTime - The time (in seconds) for the initial data point this slice represents
@param {Number} timeStep - The granularity of the data stored within this slice.
*/  
var CeresSlice= function(node, startTime, timeStep) {
  this.node= node;
  this.startTime= startTime;
  this.timeStep= timeStep;
  this.fsPath = path.join(node.fsPath, startTime +"@" + this.timeStep +".slice")
};

/*
Reads in the timeslice data as a TimeSeriesData object for the requested range.

@param {Number} fromTime - The time (in seconds) for the initial data point this slice represents
@param {Number} untilTime - The time (in seconds) for the last data point 
@param {function} cb - Standard node.js callback - function(err, results) receives an error if one occurs, or a populated TimeSeriesData object instance.
*/  
CeresSlice.prototype.read= function( fromTime, untilTime, cb) {
  var that= this;
  var timeOffset = fromTime - that.startTime;
  if( timeOffset < 0 ) {
    cb( new CeresErrors.InvalidRequest("requested time range ("+fromTime+", "+untilTime+") preceeds this slice: "+ that.startTime) );
  } else {
    fs.stat( that.fsPath, function (err, stats) {
      var pointOffset = timeOffset / that.timeStep;
      var byteOffset = pointOffset * DATAPOINT_SIZE;
      
      if( byteOffset >= stats.size ) cb( new CeresErrors.NoData() )
      else {
        fs.open(that.fsPath, "r",function(err, fd) {
          if( err ) cb( err);
          else {
            var timeRange = untilTime - fromTime;
            var pointRange = timeRange / that.timeStep;
            var buffer= new Buffer(pointRange * DATAPOINT_SIZE)
            fs.read( fd, buffer, 0, buffer.length, byteOffset, function(readErr, bytesRead, buffer){
              fs.close(fd, function(closeErr) {
                if( readErr ) cb(readErr);
                else {
                  // Ignoring closeErrors.
                   var pointsReturned = bytesRead / DATAPOINT_SIZE;
                  var values= [];
                  for( var i= 0;i<bytesRead;i+=DATAPOINT_SIZE) {
                    values.push( buffer.readDoubleBE(i, true) );
                  }
                  var endTime = fromTime + (values.length * that.timeStep);
                  cb( null, new TimeSeriesData(fromTime, endTime, that.timeStep, values));
                }
              });
            });
          }
        })
      }
    });
  }
}

exports.CeresSlice= CeresSlice;
