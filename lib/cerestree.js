var  async= require("async")
   , CeresConstants= require('./ceresconstants')
   , CeresErrors= require('./cereserrors')
   , CeresNodeModule= require('./ceresnode')
   , fs= require("fs")
   , glob = require("glob")
   , mkdirp= require('mkdirp')
   , path= require("path");

var PATH_REPLACEMENT_REGEX= new RegExp( path.sep, "g");

/*
Represents a tree of Ceres metrics contained within a single path on disk
This is the primary Ceres API.

@param {string} root - The directory root of the Ceres tree
*/
var CeresTree= function(root) {
  try {
    fs.statSync(root);
  }
  catch( e ){
    throw new CeresErrors.ValueError(e.message);
  }
  this.root= path.resolve(root);
  this.nodeCache= {};
};

/*
Creates a new metric given a new metric name and optional per-node metadata
@param {string} nodePath - The new metric name.
@param {object} properties - Arbitrary key-value properties to store as metric metadata.
@param {function} cb - Standard node.js callback - function(err, tree), receives either an error or the constructed node as arguments
*/
CeresTree.prototype.createNode = function( nodePath, properties, cb ) {
  CeresNodeModule.CeresNode.create(this, nodePath, properties, cb)
};


/*
Fetch data within a given interval from the given metric
@param {string} nodePath - The metric name to fetch from.
@param {Number} fromTime - Requested interval start time in unix-epoch.
@param {Number} untilTime - Requested interval end time in unix-epoch.
@param {function} cb - Standard node.js callback - function(err, results), receives either an error or the constructed TimeSeriesData instance as arguments
# :raises: :class:`NodeNotFound`, :class:`InvalidRequest`, :class:`NoData`

*/
CeresTree.prototype.fetch = function(nodePath, fromTime, untilTime, cb) {
  var node = this.getNode( nodePath, function(node){
    if( node ) {
      node.read(fromTime, untilTime, cb);
    }
    else {
      cb( new CeresErrors.NodeNotFound("the node '"+ nodePath +"' does not exist in this tree" ) )
    }
  });
};

/*
Find nodes which match a wildcard pattern, optionally filtering on a time range
@param {string} nodePattern - A glob-style metric wildcard
@param {Number} fromTime - Optional interval start time in unix-epoch
@param {Number} untilTime - Optional interval end time in unix-epoch
@param {function} cb - Standard node.js callback - function(err, results), receives either an error or an array of matching nodes.
*/
CeresTree.prototype.find = function( nodePattern, fromTime, untilTime, cb) {
  var that= this;
  var fsPath= that.getFilesystemPath( nodePattern );
  if( typeof(fromTime)  == 'function' ) {
    cb= fromTime;
    fromTime= untilTime= null;
  }
  if( typeof(untilTime)  == 'function' ) {
    cb= untilTime;
    untilTime= null;
  }

  // The glob library does not like non-unix paths :/
  if( path.sep != "/" ) fsPath= fsPath.replace(/\\/g, '/')
  var nodes= [];
  var mg = new glob.Glob(fsPath, {}, function(err, fsPaths) {
    if( err ) cb( err );
    else {
      if( fsPaths != null && fsPaths.length > 0 ) {
        async.eachSeries( fsPaths,
          function(fsPath, fsPath_cb) {
            var nodePath= that.getNodePath(fsPath);
            that.getNode(nodePath, function(node) {
              if( node != null ) {
                if( fromTime == null && untilTime == null ) {
                  nodes.push( node );
                  fsPath_cb();
                }
                else {
                  node.hasDataForInterval(fromTime,untilTime, function(hasData) {
                    if( hasData ) nodes.push( node );
                    fsPath_cb();
                  });
                }
              }
              else fsPath_cb();
            });
          }, 
          function(err) {
            if(err) cb(err);
            else cb(null, nodes);
          });
      }
      else cb( null, nodes);
    }
  });
}
/* 
Get the on-disk path of a Ceres node given a metric name
@param {string} nodePath - The metric name to retrieve the on-disk path for.
@returns {string} - The on-disk path of the ceres node matching the passed metric name.
*/
CeresTree.prototype.getFilesystemPath = function(nodePath) {
  return path.join(this.root, nodePath.replace(/\./g, path.sep));
}

/*
Returns a Ceres node given a metric name
@param {string} nodePath - A metric name
@param {function} cb - Standard node.js callback - function(node), receives either a CeresNode instance, or null if one not found.
*/
CeresTree.prototype.getNode = function(nodePath, cb) {
   var cachedNode= this.nodeCache[nodePath];
   var that= this;
   if( cachedNode ) cb(cachedNode);
   else {
     var fsPath = that.getFilesystemPath(nodePath);
     CeresNodeModule.CeresNode.isNodeDir(fsPath, function(isNodeDir) {
        if(isNodeDir) {
          cachedNode= new CeresNodeModule.CeresNode( that, nodePath, fsPath )
          that.nodeCache[nodePath]= cachedNode;
          cb(cachedNode);
        }
        else {
          cb(null);
        }
     });
   }
}

/* 
Get the metric name of a Ceres node given the on-disk path
@param {string} fsPath - The on-disk path to the metric filename passed
*/
CeresTree.prototype.getNodePath = function(fsPath) {
  var fsPath= path.resolve( fsPath );
  if( fsPath.indexOf(this.root) !== 0 ) throw new CeresErrors.ValueError("path '"+ fsPath +"' not beneath tree root '"+ this.root +"'");
  var nodePath= fsPath.substring( this.root.length+1 ).replace(PATH_REPLACEMENT_REGEX, '.');
  return nodePath;
}

/*
Store a list of datapoints associated with a metric
@param {string} nodePath - The metric name to write to
@param {Array} datapoints - A list of datapoint tuples : [timestamp, value]
@param {function} cb - Standard node.js callback - function(err), receives an error if there was one, otherwise nothing.
*/
CeresTree.prototype.store = function(nodePath, datapoints, cb) {
  this.getNode( nodePath, function(node) {
    if(node) {
      node.write( datapoints, cb );
    } 
    else cb( new CeresErrors.NodeNotFound("The node '" + nodePath +"' does not exist in this tree") );
  });
};

/*
Create and returns a new Ceres tree with the given properties
@param {string} root - The root directory of the new Ceres tree
@param {object} properties - Arbitrary key-value properties to store as tree metadata
@param {function} cb - Standard node.js callback - function(err, tree), receives either an error or the constructed tree as arguments
*/
CeresTree.create= function(root, properties, cb) {
  if( typeof(properties) == 'function' && cb === undefined) {
    cb= properties;
    properties= {};
  }
  function writeOutProperties() {
    var errored= false;
    var length= 0;
    var someProps= false;
    try {
    for( var k in properties ) {
      someProps= true;
      length++;
    }
    for( var k in properties ) {
      (function(fileName, data){
        fs.writeFile(fileName, data, {mode: CeresConstants.DIR_PERMS}, function(err) {
          if( err ) {
            errored= true;
            cb( err );
          }
          else {
            if( --length <= 0  && !errored) {
              cb( null, new exports.CeresTree(root))
            }
          }
        })
      })( path.join( ceresDir, k ), properties[k]);
    }
    if(!someProps) cb( null, new exports.CeresTree(root));    
 }catch( e ) {
    console.log( "err", e );
  }
}
  
  var ceresDir= path.resolve(path.join(root, '.ceres-tree'));
  fs.stat( ceresDir, function(err, stats) {
    if( err ) {
      mkdirp.mkdirp( ceresDir, CeresConstants.DIR_PERMS, function(err) {
        if( err ) cb( err );
        else {
          writeOutProperties();
        }
      });
    } else {
      writeOutProperties();
    }
  });
};

/*
Search for a CeresTree relative to the passed directory (searches passed directory, and all parent directory for sub directories named '.ceres-tree')
@param {string} dir - The starting directory to search for the presence of a Ceres tree
@param {function} cb - Standard node.js callback - function(err, tree), receives either an error or the constructed tree as arguments, or both arguments as null if no tree located, and no error occurred.
*/
CeresTree.getTree= function( dir, cb ) {
  var huntForTree= function(currentPath) {
    fs.readdir(currentPath, function(err, files) {
      //Ignore/swallow errors as some paths will be 'virtual' at time of creation.
      if( files && files.indexOf(".ceres-tree") != -1 ) {
        cb( null, new exports.CeresTree(currentPath) );
      } else {
        var newPath= path.resolve( path.join(currentPath, "..") );
        if( newPath == currentPath ) cb( null, null );
        else huntForTree(newPath);
      }
    })
  }
  huntForTree( path.resolve(dir) );
};

exports.CeresTree= CeresTree;
