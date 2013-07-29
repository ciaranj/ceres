var  fs= require("fs")
   , mkdirp= require('mkdirp')
   , path= require("path");

var DIR_PERMS = 0755;

/*
Represents a tree of Ceres metrics contained within a single path on disk
This is the primary Ceres API.

@param {string} root - The directory root of the Ceres tree
*/
var CeresTree= function(root) {
}

/*
Create and returns a new Ceres tree with the given properties
@param {string} root - The root directory of the new Ceres tree
@param {object} props - Arbitrary key-value properties to store as tree metadata
@param {function} cb - Standard node.js callback - function(err, tree), receives either an error or the constructed tree as arguments
*/
CeresTree.createTree= function(root, props, cb) {
  var ceresDir= path.resolve(path.join(root, '.ceres-tree'));
  mkdirp( ceresDir, DIR_PERMS, function(err) {
    if( err ) cb( err );
    else {
      var errored= false;
      var length= 0;
      var someProps= false;
      for( var k in props ) {
        someProps= true;
        length++;
        (function(fileName, data){
          fs.writeFile(fileName, data, {mode: DIR_PERMS}, function(err) {
            if( err ) {
              errored= true;
              cb( err );
            }
            else {
              if( --length <= 0  && !errored) {
                cb( null, new CeresTree(root))
              }
            }
          })
        })(path.join( ceresDir, k ), props[k]);
      }
      if(!someProps) cb( null, new CeresTree(root));
    }
  });
}

exports.CeresTree= CeresTree;


