#!/usr/bin/env node

var   CeresTree= require("../index").CeresTree
    , fs= require('fs'); 
    
var options= require("nomnom")
   .script("ceres-tree-create")
   .options({
        path: { position: 0, help: "If --tree is specified, <path> is taken as a node path Otherwise <path> is taken as a filesystem path", required: true }
      , tree: { help: "If specified, the path that contains the .ceres-tree" }
      , fromtime: { required: true, default: (~~(new Date().getTime() / 1000)-900) }
      , untiltime: { required: true, default: ~~(new Date().getTime() / 1000) }
      , batch: { help: "Use numeric timestamps", flag:true, required: true, default: false }
   }).parse();

var doNodeCreation= function( nodePath, tree ) {
  tree.fetch(nodePath, options.fromtime, options.untiltime, function(err, results) {
    if( err ) console.log( err );
    else {
      results.data().each( function(val) {
        if( options.batch ) {
          console.log( val[0] + "\t" + val[1] );
        }else {
          console.log( new Date(val[0]*1000) + "\t" + val[1] );
        }
      });
    }
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