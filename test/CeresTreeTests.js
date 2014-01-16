var assert = require( "assert" )
, CeresConstants = require( "../lib/ceresconstants")
, CeresErrors = require( "../lib/cereserrors")
, CeresNodeModule = require( "../lib/ceresnode" )
, CeresTreeModule = require( "../lib/cerestree" )
, fs = require( "fs" )
, glob = require( "glob" )
, mkdirp = require ("mkdirp" )
, path = require( "path" )
, sinon = require( "sinon" );

describe("CeresTree.getTree()", function() {
  var sandbox;
  beforeEach(function () {
    sandbox = sinon.sandbox.create();
  });
  afterEach(function () {
    sandbox.restore();
  });
  it("should return null for an invalid path ", function(done) {
    sandbox.stub(fs, 'readdir', function (path, callback) {
      callback(new Error());
    });
    CeresTreeModule.CeresTree.getTree('/graphite/storage/ceres', function(err, tree) {
      assert.equal( true, err ==  null );
      assert.equal( true, tree == null );
      done();
    });
  });
  it("should return a CeresTree instance constructed with the same path as passed in", function(done) {
    sandbox.stub(fs, 'readdir', function (path, callback) {
      callback(null, [".ceres-tree"]);
    });
    var mock = sandbox.mock(CeresTreeModule);
    mock.expects("CeresTree").once().withArgs("/graphite/storage/ceres")

    CeresTreeModule.CeresTree.getTree('/graphite/storage/ceres', function(err, tree) {
      assert.equal( true, err ==  null );
      assert.equal( false, tree == null );
      mock.verify();
      done();
    });
  });
});
describe("CeresTree.createTree()", function() {
  var sandbox;
  beforeEach(function() {
    sandbox = sinon.sandbox.create();
  });
  afterEach(function() {
    sandbox.restore();
  });
  it("should create the .ceres-tree directory if not present", function(done) {
    var ceres_tree_init_mock= sandbox.mock(CeresTreeModule);
    ceres_tree_init_mock.expects("CeresTree").once().withArgs("/graphite/storage/ceres");
    var open_mock= sandbox.mock(fs);
    open_mock.expects("writeFile").never();
    var makedirs_mock= sandbox.mock(mkdirp);
    makedirs_mock.expects("mkdirp")
                 .once().withArgs("/graphite/storage/ceres/.ceres-tree", CeresConstants.DIR_PERMS)
                 .callsArg(2);
    CeresTreeModule.CeresTree.create("/graphite/storage/ceres", function(){
      open_mock.verify();
      ceres_tree_init_mock.verify();
      makedirs_mock.verify();
      done();
    });
  });
  it("should use the existing ceres tree folder if present", function(done) {
    var ceres_tree_init_mock= sandbox.mock(CeresTreeModule);
    ceres_tree_init_mock.expects("CeresTree").once().withArgs("/graphite/storage/ceres");
    var open_mock= sandbox.mock(fs);
    open_mock.expects("writeFile").never();
    var makedirs_mock= sandbox.mock(mkdirp);
    makedirs_mock.expects("mkdirp").never();

    sandbox.stub(fs, 'stat', function (path, callback) {
      callback(null);
    });

    CeresTreeModule.CeresTree.create("/graphite/storage/ceres", function() {
      open_mock.verify();
      ceres_tree_init_mock.verify();
      makedirs_mock.verify();
      done();
    });
  });
  it("should create a file in the ceres-tree folder for each passed property", function(done) {
    var ceres_tree_init_mock= sandbox.mock(CeresTreeModule);
    ceres_tree_init_mock.expects("CeresTree").once().withArgs("/graphite/storage/ceres");
    var props = {
      "foo_prop" : "foo_value",
      "bar_prop" : "bar_value"
    };
    var open_mock= sandbox.mock(fs);
    for(var k in props) {
      open_mock.expects("writeFile")
               .once()
               .withArgs( path.join("/graphite/storage/ceres", ".ceres-tree", k), props[k] )
               .callsArg(3);
    }

    sandbox.stub(fs, 'stat', function (path, callback) {
      callback(null);
    });

    CeresTreeModule.CeresTree.create("/graphite/storage/ceres", props, function() {
      ceres_tree_init_mock.verify();
      open_mock.verify();
      done();
    });
  });
});

describe("CeresTree", function() {
  var sandbox;
  beforeEach(function () {
    sandbox = sinon.sandbox.create();
  });

  afterEach(function () {
    sandbox.restore();
  });
  describe(".ctor", function(){
    it("should throw a ValueError error when constructed with an invalid path", function(){
      assert.throws(function(){
        new CeresTreeModule.CeresTree("/nonexistent_path");
      }, CeresErrors.ValueError);
    });
    it("should correctly construct a CeresTree instance when constructed with a valid path", function(){
      sandbox.stub(fs, "statSync");
      var mock= sandbox.mock(path);
      mock.expects("resolve").once().withArgs("/graphite/storage/ceres").returns("/var/graphite/storage/ceres");

      var tree = new CeresTreeModule.CeresTree('/graphite/storage/ceres')
      mock.verify();
      assert.equal('/var/graphite/storage/ceres', tree.root);
    })
  });
  describe("", function() {
    var ceres_tree;
    beforeEach(function () {
      sandbox.stub(fs, "statSync");
      ceres_tree= new CeresTreeModule.CeresTree("/graphite/storage/ceres");
    });
    describe("#getNodePath()", function(){
      it("should return the correct metric name when given a clean path", function(){
        var result= ceres_tree.getNodePath("/graphite/storage/ceres/metric/foo");
        assert.equal(result, 'metric.foo')
      });
      it("should return the correct metric name even when given a path that has a trailing slash", function(){
        var result= ceres_tree.getNodePath("/graphite/storage/ceres/metric/foo/");
        assert.equal(result, 'metric.foo')
      });
      it("should raise an error if a metric is requested from outside the current tree", function(){
        assert.throws(function(){
          ceres_tree.getNodePath("/metric/foo/");
        }, CeresErrors.ValueError);
      });
    });
    describe("#getNode()", function(){
      it("should return a CeresNode instance when a valid metric is requested", function(done){
        var ceres_node_constructor_mock= sandbox.mock(CeresNodeModule);
        ceres_node_constructor_mock.expects("CeresNode")
                                   .once()
                                   .withArgs(sinon.match.any, "metrics.foo", "/graphite/storage/ceres/metrics/foo")
                                   .returns( ceres_node_constructor_mock );

        var ceres_node_mock= sandbox.mock(CeresNodeModule.CeresNode);
        ceres_node_mock.expects("isNodeDir").once().callsArgWith(1, true);

        ceres_tree.getNode('metrics.foo', function(node) {
          ceres_node_mock.verify();
          ceres_node_constructor_mock.verify();
          assert.equal( node, ceres_node_constructor_mock );
          done();
        });
      });
    });
    describe("#find()", function(){
      it("should find an explicit metric when specified", function(done){
        sandbox.stub(CeresNodeModule.CeresNode, "isNodeDir", function(nodeDir, cb) { cb(true); });
        sandbox.stub(glob, "Glob", function(fsPath, config, cb) {
          cb(null, [fsPath]);
        })

        ceres_tree.find('metrics.foo', function(err, metrics) {
          assert(metrics != null);
          assert.equal(metrics.length, 1);
          done();
        });
      });
      it("should ressolve a wildcard metric when specified", function(done){
        sandbox.stub(CeresNodeModule.CeresNode, "isNodeDir", function(nodeDir, cb) { cb(true); });
        
        var matches= ['foo', 'bar', 'baz'];
        sandbox.stub(glob, "Glob", function(fsPath, config, cb) {
          var results= [];
          for(var k in matches) {
            results.push( fsPath.replace("*", matches[k]) );
          }
          cb(null, results);
        });

        var spy= sandbox.spy(CeresNodeModule, "CeresNode");

        ceres_tree.find('metrics.*', function(err, metrics) {
          assert(metrics != null);
          assert.equal(metrics.length, 3);
          spy.calledWithExactly(ceres_tree, "metrics.foo");
          spy.calledWithExactly(ceres_tree, "metrics.bar");
          spy.calledWithExactly(ceres_tree, "metrics.baz");
          done();
        });
      });
      it("should return an empty list when no wildcard matches", function(done) {
        sandbox.stub(CeresNodeModule.CeresNode, "isNodeDir", function(nodeDir, cb) { cb(false); });
        sandbox.stub(glob, "Glob", function(fsPath, config, cb) {
          cb(null, []);
        });        
        ceres_tree.find('metrics.*', function(err, metrics) {
          assert(metrics != null);
          assert.equal(metrics.length, 0);
          done();
        });
      });
      it("should not return metrics who exist, but do not have matching intervals", function(done) {
        var ceres_node_mock= sandbox.mock(CeresNodeModule.CeresNode.prototype);
        ceres_node_mock.expects("hasDataForInterval").once().withArgs(0,1000).callsArgWith(2, false);
        sandbox.stub(CeresNodeModule.CeresNode, "isNodeDir", function(nodeDir, cb) { cb(true); });
        sandbox.stub(glob, "Glob", function(fsPath, config, cb) {
          cb(null, [fsPath]);
        });
        
        ceres_tree.find('metrics.foo', 0, 1000, function(err, metrics) {
          assert(metrics != null);
          assert.equal(metrics.length, 0);
          ceres_node_mock.verify();
          done();
        });
      });
      it("should return metrics who exist, and have matching intervals", function(done) {
        var ceres_node_mock= sandbox.mock(CeresNodeModule.CeresNode.prototype);
        ceres_node_mock.expects("hasDataForInterval").once().withArgs(0,1000).callsArgWith(2, true);
        sandbox.stub(CeresNodeModule.CeresNode, "isNodeDir", function(nodeDir, cb) { cb(true); });
        sandbox.stub(glob, "Glob", function(fsPath, config, cb) {
          cb(null, [fsPath]);
        });

        ceres_tree.find('metrics.foo', 0, 1000, function(err, metrics) {
          assert(metrics != null);
          assert.equal(metrics.length, 1);
          ceres_node_mock.verify();
          done();
        });
      });
    });
    describe("#store()", function() {
      it("should return a NodeNotFound error for an invalid metric", function(done){
        var datapoints = [[100,1.0]];
        sandbox.stub(ceres_tree, "getNode").callsArgWith(1, null);

        ceres_tree.store('metrics.foo', datapoints, function(err) {
          assert.notEqual( err, null );
          assert( err instanceof CeresErrors.NodeNotFound)
          done();
        });        
      });
          
      it("should store given datapoints for a given valid metric", function(done){
        var datapoints = [[100,1.0]];
        var spy= sandbox.spy(CeresNodeModule, "CeresNode");
        var ceres_node_mock= sandbox.mock(CeresNodeModule.CeresNode.prototype);
        ceres_node_mock.expects("write").once().withArgs(datapoints).callsArgWith(1, null);
        sandbox.stub(CeresNodeModule.CeresNode, "isNodeDir", function(nodeDir, cb) { cb(true); });

        ceres_tree.store('metrics.foo', datapoints, function(err) {
          assert.equal( err, null );
          spy.calledWithExactly(ceres_tree, "metrics.foo");
          ceres_node_mock.verify();
          done();
        });
      });
    });
    describe("#fetch()", function(){
      it("should raise a NodeNotFound error if an invalid node is requested", function(done) {
        sandbox.stub(ceres_tree, "getNode").callsArgWith(1, null);
        ceres_tree.fetch("metrics.foo", 0,0, function(err, results) {
          assert.notEqual(null, err );
          assert( err instanceof CeresErrors.NodeNotFound, "NodeNotFound should have been raised when requesting invalid node" );
          done();
        });
      });
      it("should successfully fetch a metric if a valid one is specified", function(done) {
        var tsd= sandbox.mock({});
        sandbox.mock(CeresNodeModule.CeresNode).expects("isNodeDir").once().callsArgWith(1, true);
        var ceres_node_mock= sandbox.mock(CeresNodeModule.CeresNode.prototype);
        ceres_node_mock.expects("read").once().withArgs(0,1000).callsArgWith(2, null, tsd)

        var spy= sandbox.spy(CeresNodeModule, "CeresNode");

        ceres_tree.fetch("metrics.foo", 0, 1000, function(err, results) {
          assert( spy.calledOnce );
          spy.calledWithExactly(ceres_tree, "metrics.foo");
          ceres_node_mock.verify();
          assert.equal(results, tsd);
          done();
        });
      });
    });
  });
});