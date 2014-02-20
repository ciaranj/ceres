var   async = require("async")
    , CeresErrors= require("../cereserrors")
    , CeresSlice= require("../ceresslice").CeresSlice
    , CeresTree= require("../cerestree").CeresTree
    , utils = require("../ceresutils")
    , fs= require("fs")
    , path= require("path")
    , sloth= require("sloth");

module.exports= function () {
};

module.exports.prototype.process= function() {
  var that= this;
  if( that.processing ) {
    that.logger.log("Ignoring a rollup request, already rolling-up", "WARN");
  }
  else {
    that.processing= true;
    that.cnt= 0;
    that.logger.log("Executing Maintenance task (Rollup).", "INFO");
    that.tree.walk( function(node, cb){
      that.cnt++;
      that.node_found( node, cb );
    }, function(err){
      that.logger.log("Completed Maintenance task (Rollup) :: Updated " + that.cnt + " metrics.", "INFO");
      that.processing= false;
    });
  }
};

module.exports.prototype.init= function( tree, config, logger ) {
  this.debug = config.debug;
  this.logger = logger;
  this.tree = tree;
  this.logger.log("Initializing maintenance task (Rollup)", "INFO")
};
module.exports.prototype.node_found= function(node, node_maintained) {
  var archives= [];
  var t= Math.floor(new Date().getTime() / 1000);
  var that= this;
  if( that.debug ) that.logger.log("Processing: "+ node.nodePath, "DEBUG" );
  node.readMetadata( function(err, metadata) {
    if( err ) {
      that.logger.log("Problem reading metadata for node: "+ node, "ERROR")
    }
    else {
      var slicesByPrecision= {};
      node.slices( function(err, slices) { 
        try {
          var s;
          while( s= slices.next() ) {
            if( ! slicesByPrecision[s.timeStep] ) { slicesByPrecision[s.timeStep]= []; }
            slicesByPrecision[s.timeStep].push( s );
          }
        }
        catch(e) {
          // Done.
        }
        var retentions= metadata['retentions'];
        for( var k in retentions ) {
          var precision= retentions[k][0];
          var retention= retentions[k][1];
          var archiveEnd= t - (t % precision);
          var archiveStart= archiveEnd - (precision * retention);
          t= archiveStart;
          archives.push({
            'precision' : precision,
            'retention' : retention,
            'startTime' : archiveStart,
            'endTime'   : archiveEnd,
            'slices'    : slicesByPrecision[precision] ? slicesByPrecision[precision] : []
          });
        }
        var now= Math.floor(new Date().getTime() / 1000);
        that.do_rollup(node, archives, metadata['xFilesFactor'], (metadata['aggregationMethod'] ? metadata['aggregationMethod'] : 'avg'), function(err) {
          if( that.debug ) that.logger.log(node.nodePath + " rollup time: "+ (Math.floor(new Date().getTime()/1000) - now )+" seconds", "DEBUG");
          node_maintained(err);
        });
      });
    }
  })
}

module.exports.prototype.do_rollup= function(node, archives, xff, method, rolled_up) {
  if(!archives || archives.length == 0) rolled_up();
  else {
    var that= this;
    var rollupStat = {}

    for( var k in archives ) {
      rollupStat[archives[k]['precision']] = {
        'aggregate': 0,
        'drop': 0,
        'memory': 0,
        'write': 0,
        'slice_create': 0,
        'slice_delete': 0,
        'slice_delete_points': 0,
        'slice_read': 0,
        'slice_read_points': 0,
        'slice_write': 0,
        'slice_write_points': 0,
        'slice_update': 0,
        'slice_update_points': 0,
      }
    }
    // list of (slice,deletePrioTo) -- will be dropped after aggregation
    var overflowSlices = [];

    // dict of in-memory aggregated points (one or more retentions skipped)
    var coarsePoints = {};

    // start time of node ( = start time of lowest precision archive)
    var coarsestArchive= archives[archives.length-1];
    var windowStart = coarsestArchive['startTime'];

    // dropping data from lowest precision archive
    var fineStep = coarsestArchive['precision']
    for(var k in coarsestArchive['slices']) {
      var slice= coarsestArchive['slices'][k];
      if( slice.startTime < windowStart ){
        overflowSlices.push([slice, windowStart]);
      }
    }

    // Iterate through each of the archives *EXCEPT* the coarsest
    var archivePairs= [];
    for(var i=0;i< archives.length -1;i++ ) {
      archivePairs.push([archives[i], archives[i+1]]);
    }
    async.eachSeries(archivePairs,
      function(archivePair,archivePairProcessedCb) {
        var statTime = new Date();

        // source archive for aggregation
        var fineArchive = archivePair[0]
        var fineStep    = fineArchive['precision']
        var fineStat    = rollupStat[fineStep]

        // lower precision archive
        var coarseArchive = archivePair[1]
        var coarseStep    = coarseArchive['precision']
        var coarseStart   = coarseArchive['startTime']
        var coarseStat    = rollupStat[coarseStep]

        // end time for lower presicion archive ( = start time of source archive)
        var windowEnd = coarseArchive['endTime']

        // reading points from source archive
        var finePoints = []
        async.eachSeries(fineArchive['slices'],
          function(slice, sliceProcessedCb){
            // dropping data prior to start time of this archive
            if( windowStart > slice.endTime() ) {
              overflowSlices.push( [slice, slice.endTime()] );
              setImmediate(sliceProcessedCb);
              return;
            }
            // slice starts after lower precision archive ends -- no aggregation needed
            if( windowEnd <= slice.startTime ) {
              setImmediate(sliceProcessedCb);
              return;
            }
            slice.read(Math.max(windowStart, slice.startTime), windowEnd, function(err, slicePoints) {
              if( err && err instanceof CeresErrors.NoData  || !err) {
                if( !err ) {
                  slicePoints.data().each( function(val) { 
                    fineStat['slice_read_points']+=1;
                    if( val[1] != null && typeof(val[1]) != 'undefined' ) {
                      finePoints.push( val );
                    }
                  });
                  fineStat['slice_read'] += 1;
                }
                else {
                  // no data in slice, just removing slice
                }
                // dropping data, which aggregating right now
                overflowSlices.push([slice, windowEnd]);
                setImmediate(sliceProcessedCb);
              }
              else sliceProcessedCb( err );
            });
          },
          function(err) {
            if( err ) archivePairProcessedCb(err);
            else {
              // All slices for fine archive processed.
              var finePointsDic= {};
              for(var k in finePoints) {
                finePointsDic[finePoints[k][0]]= finePoints[k][1];
              }
              // adding in-memory aggregated data
              for(var k in coarsePoints ) {
                finePointsDic[k]= coarsePoints[k];
              }
              // sort by timestamp in ascending order
              finePoints= [];
              for(var k in finePointsDic) {
                finePoints.push([k, finePointsDic[k]])
              }
              finePoints.sort(function(a,b){
                return a[0] - b[0];
              });
              coarsePoints= {};

              // no points to aggregate :(
              if( finePoints.length == 0 ) {
                archivePairProcessedCb();
                return;
              }
              
              // start time of aggregation (skipping already aggregated points)
              var startTime  = finePoints[0][0];
              startTime -= startTime % coarseStep;

              // ... and last
              var endTime  = finePoints[finePoints.length-1][0];
              endTime -= endTime % coarseStep;
              endTime += coarseStep;

              // since we are trying to write points in bulk and already existing slices
              // we need a list of slice start/end times
              // sliceEvents: list of (time, isEnd, slice-number)
              var sliceEvents   = [];

              // writeSlices: list of slices, where writePoints already exists
              var writeSlices   = []
              // lastSeenSlice: slice with maximum endTime
              // data will be written there with gap if no writeSlices found 
              var lastSeenSlice = null;

              for(var j=0;j<coarseArchive['slices'].length;j++ ) {
                var slice= coarseArchive['slices'][j];
                if( slice.startTime >= endTime ) {
                  // slice starts after end of aggregation
                  continue;
                }
                if( slice.endTime() <= startTime ) {
                  // slice ended before start of aggregation -- it can be lastSeenSlice
                  if( lastSeenSlice == null || lastSeenSlice.endTime() < slice.endTime() ) {
                    lastSeenSlice = slice;
                  }
                  continue;
                }
                if( slice.startTime > startTime ) {
                  // starting point is not covered by slice -- adding start slice event
                  sliceEvents.push([slice.startTime, false, j]);
                } else {
                  // starting point covered by slice
                  writeSlices.push(j);
                }
                // adding end slice event
                sliceEvents.push([slice.endTime(), true, j]);
              }

              // sort slice events by time
              sliceEvents.sort(function(a,b){
                return a[0] - b[0];
              });
              var sliceEventsIterator = sloth.iter(sliceEvents)
              var finePointsIterator = sloth.iter(finePoints)

              // list of points with no gap between and no slice start/end events
              // all these points will be written to one list of slices
              var writePoints = [];
              var sliceEvent = null;
              try {
                var sliceEvent = sliceEventsIterator();
              }
              catch(e) {
              }
              var finePoint = finePointsIterator();
              var tsRange= sloth.range(startTime, endTime, coarseStep);
              var ts= null;
              async.whilst(
                  function () {  
                    try {
                      ts= tsRange.next();
                      return true;
                    }
                    catch(e) {
                      return false;
                    }
                  },
                  function (callback) {
                    var tsEndTime = ts + coarseStep;
                    // no data for current timestamp -- next existing point is newer
                    if( tsEndTime <= finePoint[0] ) {
                      // writing previously found points if needed
                      
                      that.write_points(node, coarseArchive, writePoints, writeSlices, lastSeenSlice, coarseStat, function(err, lastSlice) {
                        if(!err) lastSeenSlice= lastSlice;
                        writePoints = []
                        callback(err);
                      });
                      return;
                    }
                    var values = [];
                    try {
                      //finding all values for current coarse point
                      while( finePoint[0] < tsEndTime ) {
                        values.push(finePoint[1]);
                        finePoint = finePointsIterator();
                      }
                    } catch( e ) {
                      if( !(e == sloth.StopIteration) ) throw e;
                    }
                    fineStat['aggregate'] += 1

                    // checking xff
                    if( (values.length * fineStep / coarseStep) < xff ) {
                      // writing previously found points if needed
                      that.write_points(node, coarseArchive, writePoints, writeSlices, lastSeenSlice, coarseStat, function(err, lastSlice) {
                        if(!err) {
                          lastSeenSlice= lastSlice;
                          writePoints = [];
                          fineStat['drop'] += 1;
                        }
                        callback(err);
                      });
                      return;
                    }

                    var newValue = utils.aggregateValues(method, values);
                    // in-memory aggregated point (writePoints is empty since timestamps processed in ascending order)
                    if( ts < coarseStart ) {
                      coarsePoints[ts]= newValue;
                      fineStat['memory'] += 1;
                      process.nextTick(callback);
                      return;
                    }
                    
                    // slice event found before current timestamp
                    if( sliceEvent != null && sliceEvent[0] <= ts ) {
                      // since writeSlices changed -- writting all points
                      that.write_points(node, coarseArchive, writePoints, writeSlices, lastSeenSlice, coarseStat, function(err, lastSlice) {
                        if(!err) {
                          lastSeenSlice= lastSlice;
                          writePoints = [[ts, newValue]];
                          //updating writeSlices add lastSeenSlice
                          try {
                            while( sliceEvent[0] <= ts) {
                              if( sliceEvent[1] === true ) {
                                var index = writeSlices.indexOf(sliceEvent[2]);
                                if (index > -1) {
                                    writeSlices.splice(index, 1);
                                }                                
                                lastSeenSlice = coarseArchive['slices'][sliceEvent[2]];
                              }
                              else {
                                writeSlices.push(sliceEvent[2]);
                              }
                              sliceEvent = sliceEventsIterator();
                            }
                          }
                          catch(e) {
                            if( (e == sloth.StopIteration) ) {
                              sliceEvent = null;
                            }
                            else throw e;
                          }
                          fineStat['write'] += 1
                        }
                        callback(err);
                      })
                    }
                    else {
                      // no gaps, no events, just adding to list
                      writePoints.push([ts, newValue])
                      fineStat['write'] += 1
                      process.nextTick(callback);
                    }
                  },
                  function (err) {
                    // Coarse timestep Done.

                    // writing last portion of points
                    that.write_points(node, coarseArchive, writePoints, writeSlices, lastSeenSlice, coarseStat, function(err, lastSlice) {
                      // Does not update lastSlice
                      fineStat['time'] = (new Date() - statTime)/1000;
                      archivePairProcessedCb(err);
                    });
                  }
              );
            }
          });
      },
      function(err) {
        // All archives processed
        // after all -- drop aggregated data from source archives
        async.eachSeries(overflowSlices,
          function(overflowSlice, cb) {
            var slice= overflowSlice[0]
            var deletePriorTo= overflowSlice[1];
            rollupStat[slice.timeStep]['slice_delete']        += 1;
            rollupStat[slice.timeStep]['slice_delete_points'] += (Math.min(slice.endTime(), deletePriorTo) - slice.startTime) / slice.timeStep
            slice.deleteBefore(deletePriorTo, function(err) {
              if( err instanceof CeresErrors.SliceDeleted ) { cb(); }
              else cb(err);
            });
          },
          function(err) {
            if( that.debug ) that.logger.log( JSON.stringify(rollupStat), "DEBUG");
            rolled_up();
          });
      })
  }
};

module.exports.prototype.write_points= function(node, archive, points, slices, lastSlice, stat, callback) {
  if( !points || points.length == 0 ) callback(null, lastSlice);
  else {
    var written= false;

    // trying to update all existing slices
    async.eachSeries(slices, 
      function(sliceIndex, cb) {
        archive['slices'][sliceIndex].write(points, function(err) {
          if( err && !(err instanceof CeresErrors.SliceDeleted) ) { cb(err); }
          else {
            if(!err) {
              written = true;
              stat['slice_update']        += 1;
              stat['slice_update_points'] += points.length;
            }
            cb();
          }
        })
      },
      function(err) {
        // End of updating all existing slices.

        // if not -- writing to lastSeenSlice with gap
        if(!written && lastSlice != null ) {
          lastSlice.write(points, function(err) {
            if(!err || ( err instanceof CeresErrors.SliceDeleted || err instanceof CeresErrors.SliceGapTooLarge) ) {
              if(!err) {
                written = true;
                stat['slice_write']        += 1;
                stat['slice_write_points'] += points.length
              }
              
              // gap in last slice too large -- creating new slice
              if(!written) {
                CeresSlice.create(node, points[0][0], archive['precision'], function(err, slice) {
                  if( err ) callback(err);
                  else {
                    slice.write(points, function(err) {
                      if( err ) callback(err);
                      else {
                        archive['slices'].push(slice);
                        stat['slice_create']       += 1;
                        stat['slice_write']        += 1;
                        stat['slice_write_points'] += points.length;
                        callback(null, slice);
                      }
                    });
                  }
                });
              }
              else {
                callback(null, lastSlice);
              }
            }
            else callback(err);
          });
        }
        else if(!written) {
          // gap in last slice too large -- creating new slice
          CeresSlice.create(node, points[0][0], archive['precision'], function(err, slice) {
            if( err ) callback(err);
            else {
              slice.write(points, function(err) {
                if( err ) callback(err);
                else {
                  archive['slices'].push(slice);
                  stat['slice_create']       += 1;
                  stat['slice_write']        += 1;
                  stat['slice_write_points'] += points.length;
                  callback(null, slice);
                }
              });
            }
          });
        }
        else {
          callback(null, lastSlice);
        }
      })
  }
};