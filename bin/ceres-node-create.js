#!/usr/bin/env node
var   CeresTree= require("../index").CeresTree
    , fs= require('fs');
var options= require("nomnom")
   .script("ceres-tree-create")
   .options({
        path: { position: 0, help: "If --tree is specified, <path> is taken as a node path Otherwise <path> is taken as a filesystem path", required: true }
      , tree: { help: "If specified, the path that contains the .ceres-tree" }
      , step: { help: "Default time step", required: true, default: 60 }
   }).parse();

var doNodeCreation= function( nodePath, tree ) {
 tree.createNode( nodePath, {timeStep:options.step} , function(err, node) {
   if( err ) console.log( "error: creating the ceres node", err );
 });
}

if( options.tree ) {
  doNodeCreation( options.path, new CeresTree( options.tree ) );
}
else {
  CeresTree.getTree( options.path, function(err, tree ) {
    if( err || !tree ) { 
      if( err ) console.log( "error: Unable to find a ceres tree", err );
      else console.log( "error: " + options.path + " is not in a ceres tree" );
      process.exit(1);
    } else {
      doNodeCreation( tree.getNodePath(options.path), tree );
    }
  });
}