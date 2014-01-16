var  CeresConstants= require('./ceresconstants')
   , CeresErrors= require('./cereserrors')
   , fs= require("fs")
   , mkdirp= require('mkdirp')
   , path= require("path")
   , TimeSeriesData= require("./timeseriesdata").TimeSeriesData;

var DATAPOINT_SIZE= 8;

/*
Represents a particular timeslice for a particular node.

@param {CeresNode} node - The node which this slice forms part of the data for
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
			try {
				var timeRange = untilTime - fromTime;
				var pointRange = timeRange / that.timeStep;
				var buffer= new Buffer(pointRange * DATAPOINT_SIZE)
				fs.read( fd, buffer, 0, buffer.length, byteOffset, function(readErr, bytesRead, buffer){
				  fs.close(fd, function(closeErr) {
					if( readErr ) cb(readErr);
					else if( closeErr ) cb(closeErr);
					else {
					  // Ignoring closeErrors.
					   var pointsReturned = bytesRead / DATAPOINT_SIZE;
					  var values= [];
					  var x
					  for( var i= 0;i<bytesRead;i+=DATAPOINT_SIZE) {
						x= buffer.readDoubleBE(i, true) 
						if( isNaN(x) ) values.push ( null );
						else values.push( x );
					  }
					  var endTime = fromTime + (values.length * that.timeStep);
					  cb( null, new TimeSeriesData(fromTime, endTime, that.timeStep, values));
					}
				  });
				});
			}
			catch(e) {
				console.log("CeresSlice#read, error prior to read", err);
				fs.close(fd, function(closeErr) { 
					cb(e);
				});
			}
          }
        })
      }
    });
  }
};

/*
Writes in the timeslice data.
*/
CeresSlice.prototype.write= function( sequence, cb ) {
  var that = this;
  var beginningTime = sequence[0][0];
  var timeOffset = beginningTime - that.startTime;
  var pointOffset = timeOffset / that.timeStep;
  var byteOffset = pointOffset * DATAPOINT_SIZE;
  fs.stat( that.fsPath, function(err, stats){
    if( err ) {
      if( err.code == 'ENOENT' ) {
        cb( new CeresErrors.SliceDeleted() );
      }
      else cb(err);
    }
    else {
      var filesize= stats.size;
      var byteGap = byteOffset - filesize;
      var buffer;
      var bufferOffset= 0;
      if( byteGap > 0 ) {// pad the allowable gap with nan's
        if( byteGap > CeresConstants.MAX_SLICE_GAP ){
          cb( new CeresErrors.SliceGapTooLarge() );
          return; // Ughh should re-factor to avoid this nasty non-obvious exit.
        }
        else {
          var pointGap = byteGap / DATAPOINT_SIZE;
          bufferOffset= pointGap * DATAPOINT_SIZE;
          buffer= new Buffer(bufferOffset + (sequence.length * DATAPOINT_SIZE));
          for(var i=0;i< pointGap; i++) {
            buffer.writeDoubleBE( Number.NaN, i*DATAPOINT_SIZE, true)
          }
          byteOffset -= byteGap;
        }
      }
      else {
        buffer= new Buffer(sequence.length * DATAPOINT_SIZE);
      }
      fs.open(that.fsPath, "r+", function(openerr, fd){
        if( openerr ) {
          cb( openerr )
        } else {
      try {
        for(var i=0;i< sequence.length; i++) {
        buffer.writeDoubleBE( sequence[i][1], bufferOffset + (i*DATAPOINT_SIZE), true)
        }
        fs.write(fd, buffer, 0, buffer.length, byteOffset, function(writeErr, written, buffer){
        fs.close(fd, function(closeErr) {
          if( writeErr ) {
            cb( writeErr );
          }
          else if( closeErr ) {
            cb( closeErr );
          }
          else {
          cb();
          }
        });
        });
        }
      catch(e) {
        fs.close(fd, function() {
          cb( e );
        });
      }
        }
      });
    }
  });  
};

/*
Creates a new slice
@param {CeresNode} node - The node which this slice forms part of the data for
@param {Number} startTime - The time (in seconds) for the initial data point this slice represents
@param {Number} timeStep - The granularity of the data stored within this slice.
@param {function} cb - Standard node.js callback - function(err, slice), receives either an error or the constructed slice as arguments
*/
CeresSlice.create= function(node, startTime, timeStep, cb) {
  var slice= new CeresSlice( node, startTime, timeStep );
  fs.open(slice.fsPath, "w", CeresConstants.SLICE_PERMS, function(err, fd ){
    if( err ) cb(err);
    else {
      fs.close( fd, function(err) {
        if( err ) cb (err);
        else cb( null, slice );
      } );
    }
  })
}

exports.CeresSlice= CeresSlice;
