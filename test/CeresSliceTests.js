var assert = require( "assert" )
, CeresNodeModule = require( "../lib/ceresnode")
, CeresSliceModule = require( "../lib/ceresslice")
, CeresTreeModule = require( "../lib/cerestree")
, fs = require( "fs" )
, sinon = require( "sinon" );

describe("CeresSlice", function(){
  var sandbox, ceres_tree, ceres_node;
  beforeEach(function () {
    sandbox = sinon.sandbox.create();
    sandbox.stub(fs, "statSync");
    ceres_tree= new CeresTreeModule.CeresTree('/graphite/storage/ceres')
    ceres_node= new CeresNodeModule.CeresNode(ceres_tree, 'sample_metric', '/graphite/storage/ceres/sample_metric')
  });
  afterEach(function () {
    sandbox.restore();
  });
  describe(".ctor", function(){
    it("should create the correct filename", function(){
      var ceres_slice= new CeresSliceModule.CeresSlice(ceres_node, 0, 60);
      assert.notEqual( ceres_slice.fsPath.match(/0@60.slice$/), null)
    });
  });
});
