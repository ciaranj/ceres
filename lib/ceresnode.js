var  CeresConstants= require('./ceresconstants')
   , fs= require("fs")
   , mkdirp= require('mkdirp')
   , path= require("path");

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
//  self.timeStep = None
//   self.sliceCache = None
  this.sliceCachingBehavior = CeresConstants.DEFAULT_SLICE_CACHING_BEHAVIOR;
};

/*
 Writes the node metadata to disk
 @param {Object} - The metadata to write out
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


exports.CeresNode= CeresNode;
