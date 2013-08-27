var assert = require( "assert" )
, CeresTree = require( "../lib/cerestree" ).CeresTree
, fs = require( "fs" )
, sinon = require( "sinon" );

describe("CeresTree", function(){
  describe("getTree()", function() {
    var sandbox;
    beforeEach(function (done) {
      sandbox = sinon.sandbox.create();
      done();
    });

    afterEach(function (done) {
      sandbox.restore();
      done();
    });
    it("should return null for an invalid path ", function(done) {
      sandbox.stub(fs, 'readdir', function (path, callback) {
        callback(new Error());
      });
      CeresTree.getTree('/graphite/storage/ceres', function(err, tree) {
        assert.equal( true, err ==  null );
        assert.equal( true, tree == null );
        done();
      });
    });
    it("should return a CeresTree instance constructed with the same path as passed in", function(done) {
      sandbox.stub(fs, 'readdir', function (path, callback) {
        callback(null, [".ceres-tree"]);
      });
      var mock = sandbox.mock(require( "../lib/cerestree" ));
      mock.expects("CeresTree").once().withArgs("/graphite/storage/ceres")

      CeresTree.getTree('/graphite/storage/ceres', function(err, tree) {
        assert.equal( true, err ==  null );
        assert.equal( false, tree == null );
        mock.verify();
        done();
      });
    });
  });
});
