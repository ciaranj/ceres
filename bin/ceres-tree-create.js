#!/usr/bin/env node
var   CeresTree= require("../index").CeresTree
    , fs= require('fs');
var nom= require("nomnom")
   .script("ceres-tree-create")
   .options({
        path: { position: 0, help: "Path to tree root", required: true }
      , properties: { position: 1, help: "Properties, property=value", list: true}
      , verbose: { help: "verbosity", flag: true, default: false }
   });

var options= nom.parse();
var props= {};
if( options.properties ) {
  for( var k in options.properties) {
      var prop= options.properties[k].split("=");
      props[prop[0]]= prop[1];
  }
}
options.properties= props;

if (options.verbose) {
  console.log( "Creating tree at " + options.path+" with props= " +  JSON.stringify(options.properties) );
  CeresTree.createTree(options.path, options.properties, function(err, tree) {
     if( err ) {
       console.log( "There was an error : ", err );
       process.exit(1);
     }
  });
}
