var  CeresConstants= require('./ceresconstants')
   , CeresErrors= require('./cereserrors')
   , CeresSlice= require('./ceresslice').CeresSlice
   , fs= require("fs")
   , mkdirp= require('mkdirp')
   , path= require("path")
   , sloth= require("sloth")
   , TimeSeriesData= require('./timeseriesData').TimeSeriesData;

/*
   Represents a tree of Ceres metrics contained within a single path on disk
   This is the primary Ceres API.

   @param {CeresTree} tree - The Ceresree this node 'belongs' to.
   @param {string} nodePath -
   @param {string} fsPath -
*/
var CeresNode= function(tree, nodePath, fsPath) {
  this.tree = tree;
  this.nodePath = nodePath;
  this.fsPath = fsPath;
  this.metadataFile = path.join(fsPath, '.ceres-node')
  this.timeStep = null;
  this.sliceCache = null;
  this.sliceCachingBehavior = CeresConstants.DEFAULT_SLICE_CACHING_BEHAVIOR;
};

/*
 Reads in the relevant data and passes back a populated TimeSeriesData object instance.

 @param {Number} fromTime - The initial timestamp (inclusive)
 @param {Number} untilTime - The fnal timestamp (inclusive)
 @param {function} cb - Standard node.js callback - function(err, results) receives an error if one occurs, or a populated TimeSeriesData object instance.
*/
CeresNode.prototype.read= function(fromTime, untilTime, cb) {
  var that= this;
  function doRead() {
    var resultValues= [];
    var sliceBoundary = null;
    var earliestData = null;
    function slicesAllRead() {
      //Left pad nulls if the start of the requested interval predates all slices
      if( earliestData ) {
        var missing= (earliestData - fromTime) / that.timeStep;
        var newValues= [];
        for(var i=0;i<missing;i++) newValues.push( null );
        for(var k in resultValues) newValues.push( resultValues[k] );
        resultValues= newValues;
      }
      // The end of the requested interval predates all slices
      else {
         var missing= Math.floor((untilTime - fromTime) / that.timeStep);
         for(var i=0;i<missing;i++) resultValues.push(null);
      }

      cb( null, new TimeSeriesData(fromTime, untilTime, that.timeStep, resultValues));
    }
    // Normalize the timestamps to fit proper intervals
    fromTime = Math.floor(fromTime - (fromTime % that.timeStep) + that.timeStep);
    untilTime = Math.floor(untilTime - (untilTime % that.timeStep) + that.timeStep);
    that.slices( function(err, slices) {
      if( err ) cb(err);
      else {
        var nextSlice;
        function carryOn() {
          try {
            nextSlice= slices.next();
          }
          catch(e) {
            slicesAllRead();
            return;
          }
          // if the requested interval starts after the start of this slice
          if( fromTime >= nextSlice.startTime ) {
            var series = nextSlice.read( fromTime, untilTime, function(err, series) {
              if( err && err instanceof CeresErrors.NoData ) {
                // No more data...
                slicesAllRead();
              }
              else if( err ) {
                cb(err);
              }
              else {
                // do something with the data
                earliestData = series.startTime

                var missing= Math.floor((untilTime - series.endTime) / that.timeStep);
                var rightNulls= missing - resultValues.length;
                var newValues = [];
                for(var k in series.values) newValues.push( series.values[k] );
                for(var i=0;i<rightNulls;i++) newValues.push(null);
                for(var k in resultValues) newValues.push(resultValues[k]);
                resultValues= newValues;
                slicesAllRead();
              }
            });
          }
          // or if slice contains data for part of the requested interval
          else if( untilTime >= nextSlice.startTime ) {
            // Split the request up if it straddles a slice boundary
            if (sliceBoundary && untilTime > sliceBoundary) var requestUntilTime= sliceBoundary;
            else var requestUntilTime= untilTime;
            var series = nextSlice.read( nextSlice.startTime, requestUntilTime, function(err, series) {
              if( err && err instanceof CeresErrors.NoData ) {
                carryOn();
              }
              else if( err ) {
                cb (err);
              }
              else {
                earliestData = series.startTime;
                var missing= Math.floor((requestUntilTime - series.endTime) / that.timeStep);
                var newValues = [];
                for(var k in series.values) newValues.push( series.values[k] );
                for(var i=0;i<missing;i++) newValues.push(null);
                for(var k in resultValues) newValues.push(resultValues[k]);
                resultValues= newValues;
                sliceBoundary = nextSlice.startTime
                carryOn();
              }
            });
          }
          else {
            // Can't see why we would end up here, but the python code suggested this branch might exist :/
            sliceBoundary = nextSlice.startTime
            carryOn();
          }
        }
        carryOn();

       }
    });
  }

  if( !that.timeStep ) {
    that.readMetadata( function(err, metadata) {
      if( err ) cb(err);
      else {
        doRead();
      }
    });
  }
  else {
    doRead();
  }

}

/*
 Provides an 'iterator' that can be used to traverse the associated slice infos for this node.
 @param {function} cb - Standard node.js callback - function(err, results) receives an error if one occurs, or an iterator over the node-associated time slice infos
*/

CeresNode.prototype.readSlices= function( cb ) {
  if(!this.fsPath) cb( new CeresErrors.NodeDeleted() )
  else {
    fs.readdir( this.fsPath, function(err, files) {
      if( err ) cb(err);
      else {
        var slice_infos= [];
        for(var f in files) {
          var file= files[f];
          if( /\.slice$/.test(file) ) {
            var id= file.substring(0, file.length-6);
            var indexOfAt= id.indexOf('@');
            slice_infos.push( [+id.substring(0, indexOfAt), +id.substring(indexOfAt+1)] )
          }
        }
        // Ugggh need to check the sort semantics mach python here.. hoffible fear it may be alpha-numeric :/
        cb( null, sloth.ify(slice_infos).sort(function(a,b) {
          return b[0]-a[0];
        }) );
      }
    })
  }
}
  /*
   Provides an 'iterator' that can be used to traverse the associated slices for this node.
   @param {function} cb - Standard node.js callback - function(err, results) receives an error if one occurs, or an iterator over the node-associated time slices
  */
CeresNode.prototype.slices= function( cb ) {
  var that= this;
  if( that.sliceCache ) {

  }
  else {
    that.readSlices(function(err, sliceInfos){
      if( err ) cb( err );
      else {
        var s= sliceInfos.map( function(sliceInfo) {
          return new CeresSlice( that, sliceInfo[0], sliceInfo[1] )
        });
        cb( null, s );
      }
    })

  }
/*  f self.sliceCache:
    if self.sliceCachingBehavior == 'all':
      for slice in self.sliceCache:
        yield slice

    elif self.sliceCachingBehavior == 'latest':
      yield self.sliceCache
      infos = self.readSlices()
      for info in infos[1:]:
        yield CeresSlice(self, *info)

  else:
    if self.sliceCachingBehavior == 'all':
      self.sliceCache = [CeresSlice(self, *info) for info in self.readSlices()]
      for slice in self.sliceCache:
        yield slice

    elif self.sliceCachingBehavior == 'latest':
      infos = self.readSlices()
      if infos:
        self.sliceCache = CeresSlice(self, *infos[0])
        yield self.sliceCache

      for info in infos[1:]:
        yield CeresSlice(self, *info)

    elif self.sliceCachingBehavior == 'none':
      for info in self.readSlices():
        yield CeresSlice(self, *info)

    else:
      raise ValueError("invalid caching behavior configured '%s'" % self.sliceCachingBehavior)*/
}

/*
 Writes the node metadata to disk
 @param {function} cb - Standard node.js callback - function(err, metadata) receives an error if one occurs, the read metadata otherwise.
*/
CeresNode.prototype.readMetadata = function(cb) {
  var that= this;
  fs.readFile( this.metadataFile, {encoding:'utf8'}, function(err, data) {
    if( err ) cb(err);
    else {
      var success= false;
      try {
        this.metadata= JSON.parse(data);
        that.timeStep= +this.metadata.timeStep
        success= true;
      } catch(err) {
        cb( err );
      }
      if( success ) cb( null, this.metadata );
    }
  });
}

/*
 Writes the node metadata to disk
 @param {Object} metadata - The metadata to write out
 @param {function} cb - Standard node.js callback - function(err) receives an error if one occurs
*/
CeresNode.prototype.writeMetadata = function(metadata, cb) {
  this.timeStep= +metadata.timeStep;
  var metaDataAsString= JSON.stringify(metadata);
  fs.writeFile(this.metadataFile, metaDataAsString, {flag: 'w'}, cb );
}

/*
Creates a new metric given a new metric name and optional per-node metadata
@param {CeresTree} tree - The CeresTree this node 'belongs' to.
@param {string} nodePath - The new metric name.
@param {object} properties - Arbitrary key-value properties to store as metric metadata.
@param {function} cb - Standard node.js callback - function(err, tree), receives either an error or the constructed node as arguments
*/
CeresNode.create= function(tree, nodePath, properties, cb) {
  if(!properties) properties= {};

  // Create the node directory
  var fsPath = tree.getFilesystemPath(nodePath);
  var that = this;
  mkdirp( fsPath, CeresConstants.DIR_PERMS, function(err) {
    if( err ) cb( err );
    else {
      // Create the initial metadata
      if(!properties.timeStep)  properties.timeStep= CeresConstants.TIMESTEP;
      var node = new CeresNode(tree, nodePath, fsPath);
      node.writeMetadata(properties, function(err) {
        if( err ) cb( err );
        else cb( null, node );
      })
    }
  });
};

/*
 Determines whether a given node filesystem path is actually a node directory.
 @param {string} - The path to the node directory in question
 @param {function} cb - Standard node.js callback - function(isNodeDir) receives false if the node directory cannot be found, or is not a node directory, true otherwise.
*/
CeresNode.isNodeDir= function(nodeDir, cb) {
  fs.stat( nodeDir, function(err, stats) {
     if( err ) cb ( false );
     else {
       if( stats.isDirectory() ) {
         fs.exists( path.join(nodeDir, '.ceres-node'), function(exists) {
           cb( exists );
         })
       }
       else cb( false );
     }
  });
};

exports.CeresNode= CeresNode;
