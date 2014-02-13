var   async = require("async")
    , Rollup= require('../lib/maintenance/rollup')
    , CeresTree= require("../lib/cerestree").CeresTree;

var tree= new CeresTree( "/Users/ciaran/Documents/Development/ceres/aged_tree");
var rollup= new Rollup();
var logger= {
  log: function(msg, level) {
    if( level ) msg = level + ": "+ msg;
    console.log(msg);
  }
}

rollup.init(tree, {debug:true}, logger);
rollup.process();
