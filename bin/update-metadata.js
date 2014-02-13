#!/usr/bin/env node

var   async= require("async")
    , CeresTree= require("../index").CeresTree
    , fs= require('fs')
    , path= require('path')
    
var options= require("nomnom")
   .script("update-metadata")
   .options({
       tree: { help: "If specified, the path that contains the .ceres-tree" }
   }).parse();

var tree= new CeresTree( options.tree );

var schemas= [{name: "default", pattern: /.*/, retentions: "60:50400,900:8064,3600:8760,86400:730"}];

var aggregations= [
  {name: "lower", pattern: /\.lower(_\d+)?$/, xFilesFactor: 0, aggregationMethod: "min"},
  {name: "upper", pattern: /\.upper(_\d+)?$/, xFilesFactor: 0, aggregationMethod: "max"},
  {name: "sum", pattern: /\.((numStats)|(count)|(sum(_\d+)?))$/, xFilesFactor: 0, aggregationMethod: "sum"},
  {name: "default_average", pattern: /.*/, xFilesFactor: 0.5, aggregationMethod: "average"}
];

function parse_retentions( retentions_str ) {
    var retentions= [];
    var archives= retentions_str.split(",");
    for( var i=0;i< archives.length;i++ ){
        var archive= archives[i].split(":");
        retentions[retentions.length]= [parseInt(archive[0]), parseInt(archive[1])];
    }
    return retentions;
}

// Convert the given retentions string format to the internal hoard-compatible format
for(var i=0;i< schemas.length;i++) {
  var schema= schemas[i];
  schema.retentions= parse_retentions( schema.retentions );
}

var walk = function(dir, done) {
  var results = [];
  fs.readdir(dir, function(err, list) {
    if (err) return done(err);
    var i = 0;
    (function next() {
      var file = list[i++];
      if (!file) return done(null, results);
      file = path.join( dir, file);
      fs.stat(file, function(err, stat) {
        if (stat && stat.isDirectory()) {
          if( fs.existsSync(path.join(file, ".ceres-node") ) ) {
            results.push( file );
          }
          walk(file, function(err, res) {
            results = results.concat(res);
            next();
          });
        } else {
          next();
        }
      });
    })();
  });
};
var cnt=0;
walk(tree.root, function(err, results){
  async.eachSeries( results, function(item, cb){
    var node= tree.getNode( tree.getNodePath( item ), function(node) {
      node.readMetadata( function(err, metadata) {
        var x= 0;
        for( var s in schemas ) {
          if( schemas[s].pattern.test(node.nodePath)) {
            console.log( "\t Schema: " + schemas[s].name );
            metadata.retentions= schemas[s].retentions;
            x++;
            break;
          }
        }
        for( var a in aggregations ) {
          if( aggregations[a].pattern.test(node.nodePath)) {
            console.log( "\t Aggregation: " + aggregations[a].name );
            metadata.xFilesFactor= aggregations[a].xFilesFactor;
            metadata.aggregationMethod= aggregations[a].aggregationMethod;
            x++;
            break;
          }
        }
        if( x != 2 ) {
          console.log("COULD NOT SELECT FOR " + node.nodePath );
        }
        else {
//          console.log( "\t " + JSON.stringify( metadata ));
          node.writeMetadata( metadata, function(err) {
            cnt++;
            cb(err);
          })
        }
      });
    });
  },
  function(err){
    console.log( "Updated " + cnt + " metrics." );
  });
});

