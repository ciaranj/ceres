#!/usr/bin/env node

var   CeresTree= require("../index").CeresTree
    , fs= require('fs');

var options= require("nomnom")
   .script("ceres-tree-create")
   .options({
        path: { position: 0, help: "If --tree is specified, <path> is taken as a node path Otherwise <path> is taken as a filesystem path", required: true }
      , datapoint: { position: 1, list: true, help: "Each datapoint is of the form <timestamp>:<value> where <timestamp> may be a UNIX epoch time or the character 'N' to indicate 'now'", required: true }
      , tree: { help: "If specified, the path that contains the .ceres-tree" }
   }).parse();

var doNodeCreation= function( nodePath, tree ) {
  var datapoints = [];
  var now = (+new Date())/1000;
  var timestamp, datapoint, split, value;

  for( var key in options.datapoint ) {
    datapoint = options.datapoint[key];
    split= datapoint.split(":");
    if( split[0] == 'N') timestamp = now;
    else timestamp= +split[0];
    
    value = +split[1];
    datapoints.push( [timestamp, value] );
  }

  datapoints.sort(function(a,b) {
    var d= a[0]-b[0];
    if( d != 0 ) return d;
    else return a[1] -b[1];
  })

  tree.getNode(nodePath, function(node) {
    if( node ) {
      node.write( datapoints );
    }
    else { console.log("Unable to find node: " + nodePath)}
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
