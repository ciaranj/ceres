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
  console.log( "CeresSlice.write", beginningTime, timeOffset, pointOffset, byteOffset);
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
      if( byteGap > 0 ) {// pad the allowable gap with nan's
        console.log( "SSDSDS SDS FPAIN")
/*        if byteGap > MAX_SLICE_GAP:
          raise SliceGapTooLarge()
        else:
          pointGap = byteGap / DATAPOINT_SIZE
          packedGap = PACKED_NAN * pointGap
          packedValues = packedGap + packedValues
          byteOffset -= byteGap*/
      }
      fs.open(that.fsPath, "r+", function(openerr, fd){
        if( openerr ) {
          console.log(" IOError (Open): fsPath="+ that.fsPath+" byteOffset="+byteOffset+" size="+filesize+" sequence=" + sequence);
          cb( openerr )
        } else {
          var buffer= new Buffer(sequence.length * DATAPOINT_SIZE);
          for(var i=0;i< sequence.length; i++) {
            buffer.writeDoubleBE( sequence[i][1], i*DATAPOINT_SIZE, true)
          }
          fs.write(fd, buffer, 0, buffer.length, byteOffset, function(writeErr, written, buffer){
            if( writeErr ) {
              console.log(" IOError (Write): fsPath="+ that.fsPath+" byteOffset="+byteOffset+" size="+filesize+" sequence=" + sequence);
              cb( writeErr );
            }
            else {
              cb();
            }
          })
        }
      });
    }
  });


/*  beginningTime = sequence[0][0]
  timeOffset = beginningTime - self.startTime
  pointOffset = timeOffset / self.timeStep
  byteOffset = pointOffset * DATAPOINT_SIZE

  values = [v for t,v in sequence]
  format = '!' + ('d' * len(values))
  packedValues = struct.pack(format, *values)

  try:
    filesize = getsize(self.fsPath)
  except OSError, e:
    if e.errno == errno.ENOENT:
      raise SliceDeleted()
    else:
      raise

  byteGap = byteOffset - filesize
  if byteGap > 0:  # pad the allowable gap with nan's

    if byteGap > MAX_SLICE_GAP:
      raise SliceGapTooLarge()
    else:
      pointGap = byteGap / DATAPOINT_SIZE
      packedGap = PACKED_NAN * pointGap
      packedValues = packedGap + packedValues
      byteOffset -= byteGap

  with file(self.fsPath, 'r+b') as fileHandle:
    try:
      fileHandle.seek(byteOffset)
    except IOError:
      print " IOError: fsPath=%s byteOffset=%d size=%d sequence=%s" % (self.fsPath, byteOffset, filesize, sequence)
      raise
    fileHandle.write(packedValues)*/  
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

/*
  def create(cls, node, startTime, timeStep):
    slice = cls(node, startTime, timeStep)
    fileHandle = open(slice.fsPath, 'wb')
    fileHandle.close()
    os.chmod(slice.fsPath, SLICE_PERMS)
    return slice*/
exports.CeresSlice= CeresSlice;
