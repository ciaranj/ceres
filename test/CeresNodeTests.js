var assert = require( "assert" )
, CeresConstants = require( "../lib/ceresconstants")
, CeresErrors = require( "../lib/cereserrors")
, CeresNodeModule = require( "../lib/ceresnode")
, CeresSliceModule = require( "../lib/ceresslice")
, CeresTreeModule = require( "../lib/cerestree")
, fs = require( "fs" )
, mkdirp = require( "mkdirp" )
, sinon = require( "sinon" )
, sloth= require("sloth") 
;

describe("CeresNode", function(){
  var sandbox, ceres_tree, ceres_node;
//  var slice_configs= [[1200,1800,60],[600,1200,60]];
  beforeEach(function () {
    sandbox = sinon.sandbox.create();
    sandbox.stub(fs, "statSync");
    ceres_tree= new CeresTreeModule.CeresTree('/graphite/storage/ceres')
    ceres_node= new CeresNodeModule.CeresNode(ceres_tree, 'sample_metric', '/graphite/storage/ceres/sample_metric')
    ceres_node.timeStep= 60;
    fs.statSync.restore();
  });
  afterEach(function () {
    sandbox.restore();
  });
  describe(".ctor", function(){
    it("should initially construct a CeresNode instance with the default slice caching behavior<sic>", function(){
      var ceres_node = new CeresNodeModule.CeresNode(ceres_tree, 'sample_metric', '/graphite/storage/ceres/sample_metric')
      assert( CeresConstants.DEFAULT_SLICE_CACHING_BEHAVIOR, ceres_node.sliceCachingBehavior)
    });
  });
  describe(".create()", function(){
    it("should initially create a new Node with the default timestep", function(done){
      sandbox.stub(fs, 'stat').callsArg(1);
      sandbox.stub(mkdirp, "mkdirp").callsArg(2);
      
      var write_metadata_mock= sandbox.mock(CeresNodeModule.CeresNode.prototype);
      write_metadata_mock.expects("writeMetadata").once().withArgs(sinon.match({timeStep : CeresConstants.DEFAULT_TIMESTEP })).callsArgWith(1, null);
      
      var ceres_node = CeresNodeModule.CeresNode.create(ceres_tree, 'sample_metric', function(err, node){
        assert.equal(err, null);
        write_metadata_mock.verify();
        done();
      });
    });
    it("should create a new instance of CeresNode", function(done) {
      sandbox.stub(fs, 'stat').callsArg(1);
      sandbox.stub(mkdirp, "mkdirp").callsArg(2);
      sandbox.stub(CeresNodeModule.CeresNode.prototype, "writeMetadata").callsArgWith(1, null);
      var ceres_node = CeresNodeModule.CeresNode.create(ceres_tree, 'sample_metric', function(err, node){
        assert.equal(err, null);
        assert( node instanceof CeresNodeModule.CeresNode)
        done();
      });
    });
  });
  describe("#writeMetadata()", function() {
    it("should write out the expected metadata, when provided", function(done) {
      var metadata= {timeStep: 60, aggregationMethod: 'avg'};
      var metaDataAsString= JSON.stringify(metadata);
      var open_mock= sandbox.mock(fs);
      open_mock.expects("writeFile").once().withArgs(sinon.match.any, metaDataAsString).callsArg(3);

      ceres_node.writeMetadata(metadata, function(err) {
        assert(!err);
        open_mock.verify();
        done();
      });
    });
    it("should set the timestamp when the metadata is read", function(done) {
      var metadata= {timeStep: 40, aggregationMethod: 'avg'};
      var metaDataAsString= JSON.stringify(metadata);
      var open_mock= sandbox.mock(fs);
      open_mock.expects("readFile").once().callsArgWith(2, null, metaDataAsString);

      assert.notEqual( ceres_node.timeStep, 40);
      ceres_node.readMetadata(function(err, metadata) {
        assert.equal(err, null);
        assert.equal( ceres_node.timeStep, 40);
        open_mock.verify();
        done();
      });
    });
  });
  describe("#setSliceCachingBehavior", function(){
    it("should validate the requested behaviour names", function() {
      ceres_node.setSliceCachingBehavior('none');
      assert.equal('none', ceres_node.sliceCachingBehavior);
      ceres_node.setSliceCachingBehavior('all');
      assert.equal('all', ceres_node.sliceCachingBehavior);
      ceres_node.setSliceCachingBehavior('latest');
      assert.equal('latest', ceres_node.sliceCachingBehavior);
      assert.throws(function(){
        ceres_node.setSliceCachingBehavior('foo');
      }, CeresErrors.ValueError);
      
      //Assert unchanged
      assert.equal('latest', ceres_node.sliceCachingBehavior);
    });
  });
  describe("#slices", function(){
    it("should return an iterator", function(done){
      sandbox.stub(fs, "readdir").callsArgWith(1, null, ["foo","bar"]);
      ceres_node.slices( function(err, results ){
        assert.equal(err, null);
        assert( results instanceof sloth.Slothified)
        done();
      });
    });
  });
});

/*
class CeresNodeTest(TestCase):
  def setUp(self):

    slice_configs = [
      ( 1200, 1800, 60 ),
      ( 600, 1200, 60 )]

    self.ceres_slices = []
    for start, end, step in slice_configs:
      slice_mock = Mock(spec=CeresSlice)
      slice_mock.startTime = start
      slice_mock.endTime = end
      slice_mock.timeStep = step

      self.ceres_slices.append(slice_mock)

  def test_slices_returns_cached_set_when_behavior_is_all(self):
    def mock_slice():
      return Mock(spec=CeresSlice)

    self.ceres_node.setSliceCachingBehavior('all')
    cached_contents = [ mock_slice for c in range(4) ]
    self.ceres_node.sliceCache = cached_contents
    with patch('ceres.CeresNode.readSlices') as read_slices_mock:
      slice_list = list(self.ceres_node.slices)
      self.assertFalse(read_slices_mock.called)

    self.assertEquals(cached_contents, slice_list)

  def test_slices_returns_first_cached_when_behavior_is_latest(self):
    self.ceres_node.setSliceCachingBehavior('latest')
    cached_contents = Mock(spec=CeresSlice)
    self.ceres_node.sliceCache = cached_contents

    read_slices_mock = Mock(return_value=[])
    with patch('ceres.CeresNode.readSlices', new=read_slices_mock):
      slice_iter = self.ceres_node.slices
      self.assertEquals(cached_contents, slice_iter.next())
      # We should be yielding cached before trying to read
      self.assertFalse(read_slices_mock.called)

  def test_slices_reads_remaining_when_behavior_is_latest(self):
    self.ceres_node.setSliceCachingBehavior('latest')
    cached_contents = Mock(spec=CeresSlice)
    self.ceres_node.sliceCache = cached_contents

    read_slices_mock = Mock(return_value=[(0,60)])
    with patch('ceres.CeresNode.readSlices', new=read_slices_mock):
      slice_iter = self.ceres_node.slices
      slice_iter.next()

      # *now* we expect to read from disk
      try:
        while True:
          slice_iter.next()
      except StopIteration:
        pass

    read_slices_mock.assert_called_once_with()

  def test_slices_reads_from_disk_when_behavior_is_none(self):
    self.ceres_node.setSliceCachingBehavior('none')
    read_slices_mock = Mock(return_value=[(0,60)])
    with patch('ceres.CeresNode.readSlices', new=read_slices_mock):
      slice_iter = self.ceres_node.slices
      slice_iter.next()

    read_slices_mock.assert_called_once_with()

  def test_slices_reads_from_disk_when_cache_empty_and_behavior_all(self):
    self.ceres_node.setSliceCachingBehavior('all')
    read_slices_mock = Mock(return_value=[(0,60)])
    with patch('ceres.CeresNode.readSlices', new=read_slices_mock):
      slice_iter = self.ceres_node.slices
      slice_iter.next()

    read_slices_mock.assert_called_once_with()

  def test_slices_reads_from_disk_when_cache_empty_and_behavior_latest(self):
    self.ceres_node.setSliceCachingBehavior('all')
    read_slices_mock = Mock(return_value=[(0,60)])
    with patch('ceres.CeresNode.readSlices', new=read_slices_mock):
      slice_iter = self.ceres_node.slices
      slice_iter.next()

    read_slices_mock.assert_called_once_with()

  @patch('ceres.exists', new=Mock(return_value=False))
  def test_read_slices_raises_when_node_doesnt_exist(self):
    self.assertRaises(NodeDeleted, self.ceres_node.readSlices)

  @patch('ceres.exists', new=Mock(return_Value=True))
  def test_read_slices_ignores_not_slices(self):
    listdir_mock = Mock(return_value=['0@60.slice', '0@300.slice', 'foo'])
    with patch('ceres.os.listdir', new=listdir_mock):
      self.assertEquals(2, len(self.ceres_node.readSlices()))

  @patch('ceres.exists', new=Mock(return_Value=True))
  def test_read_slices_parses_slice_filenames(self):
    listdir_mock = Mock(return_value=['0@60.slice', '0@300.slice'])
    with patch('ceres.os.listdir', new=listdir_mock):
      slice_infos = self.ceres_node.readSlices()
      self.assertTrue((0,60) in slice_infos)
      self.assertTrue((0,300) in slice_infos)

  @patch('ceres.exists', new=Mock(return_Value=True))
  def test_read_slices_reverse_sorts_by_time(self):
    listdir_mock = Mock(return_value=[
      '0@60.slice',
      '320@300.slice',
      '120@120.slice',
      '0@120.slice',
      '600@300.slice'])

    with patch('ceres.os.listdir', new=listdir_mock):
      slice_infos = self.ceres_node.readSlices()
      slice_timestamps = [ s[0] for s in slice_infos ]
      self.assertEqual([600,320,120,0,0], slice_timestamps)

  def test_no_data_exists_if_no_slices_exist(self):
    with patch('ceres.CeresNode.readSlices', new=Mock(return_value=[])):
      self.assertFalse(self.ceres_node.hasDataForInterval(0,60))

  def test_no_data_exists_if_no_slices_exist_and_no_time_specified(self):
    with patch('ceres.CeresNode.readSlices', new=Mock(return_value=[])):
      self.assertFalse(self.ceres_node.hasDataForInterval(None,None))

  def test_data_exists_if_slices_exist_and_no_time_specified(self):
    with patch('ceres.CeresNode.slices', new=self.ceres_slices):
      self.assertTrue(self.ceres_node.hasDataForInterval(None,None))

  def test_data_exists_if_slice_covers_interval_completely(self):
    with patch('ceres.CeresNode.slices', new=[self.ceres_slices[0]]):
      self.assertTrue(self.ceres_node.hasDataForInterval(1200,1800))

  def test_data_exists_if_slice_covers_interval_end(self):
    with patch('ceres.CeresNode.slices', new=[self.ceres_slices[0]]):
      self.assertTrue(self.ceres_node.hasDataForInterval(600, 1260))

  def test_data_exists_if_slice_covers_interval_start(self):
    with patch('ceres.CeresNode.slices', new=[self.ceres_slices[0]]):
      self.assertTrue(self.ceres_node.hasDataForInterval(1740, 2100))

  def test_no_data_exists_if_slice_touches_interval_end(self):
    with patch('ceres.CeresNode.slices', new=[self.ceres_slices[0]]):
      self.assertFalse(self.ceres_node.hasDataForInterval(600, 1200))

  def test_no_data_exists_if_slice_touches_interval_start(self):
    with patch('ceres.CeresNode.slices', new=[self.ceres_slices[0]]):
      self.assertFalse(self.ceres_node.hasDataForInterval(1800, 2100))

  def test_compact_returns_empty_if_passed_empty(self):
    self.assertEqual([], self.ceres_node.compact([]))

  def test_compact_filters_null_values(self):
    self.assertEqual([], self.ceres_node.compact([(60,None)]))

  def test_compact_rounds_timestamps_down_to_step(self):
    self.assertEqual([[(600,0)]], self.ceres_node.compact([(605,0)]))

  def test_compact_drops_duplicate_timestamps(self):
    datapoints = [ (600, 0), (600, 0) ]
    compacted = self.ceres_node.compact(datapoints)
    self.assertEqual([[(600, 0)]], compacted)

  def test_compact_groups_contiguous_points(self):
    datapoints = [ (600, 0), (660, 0), (840,0) ]
    compacted = self.ceres_node.compact(datapoints)
    self.assertEqual([[(600, 0), (660,0)], [(840,0)]], compacted)

  def test_write_noops_if_no_datapoints(self):
    with patch('ceres.CeresNode.slices', new=self.ceres_slices):
      self.ceres_node.write([])
      self.assertFalse(self.ceres_slices[0].write.called)

  def test_write_within_first_slice(self):
    datapoints = [(1200, 0.0), (1260, 1.0), (1320, 2.0)]

    with patch('ceres.CeresNode.slices', new=self.ceres_slices):
      self.ceres_node.write(datapoints)
      self.ceres_slices[0].write.assert_called_once_with(datapoints)

  @patch('ceres.CeresSlice.create')
  def test_write_within_first_slice_doesnt_create(self, slice_create_mock):
    datapoints = [(1200, 0.0), (1260, 1.0), (1320, 2.0)]

    with patch('ceres.CeresNode.slices', new=self.ceres_slices):
      self.ceres_node.write(datapoints)
      self.assertFalse(slice_create_mock.called)

  @patch('ceres.CeresSlice.create', new=Mock())
  def test_write_within_first_slice_with_gaps(self):
    datapoints = [ (1200,0.0), (1320,2.0) ]

    with patch('ceres.CeresNode.slices', new=self.ceres_slices):
      self.ceres_node.write(datapoints)

      # sorted most recent first
      calls = [call.write([datapoints[1]]), call.write([datapoints[0]])]
      self.ceres_slices[0].assert_has_calls(calls)

  @patch('ceres.CeresSlice.create', new=Mock())
  def test_write_within_previous_slice(self):
    datapoints = [ (720,0.0), (780,2.0) ]

    with patch('ceres.CeresNode.slices', new=self.ceres_slices):
      self.ceres_node.write(datapoints)

      # 2nd slice has this range
      self.ceres_slices[1].write.assert_called_once_with(datapoints)

  @patch('ceres.CeresSlice.create')
  def test_write_within_previous_slice_doesnt_create(self, slice_create_mock):
    datapoints = [ (720,0.0), (780,2.0) ]

    with patch('ceres.CeresNode.slices', new=self.ceres_slices):
      self.ceres_node.write(datapoints)
      self.assertFalse(slice_create_mock.called)

  @patch('ceres.CeresSlice.create', new=Mock())
  def test_write_within_previous_slice_with_gaps(self):
    datapoints = [ (720,0.0), (840,2.0) ]

    with patch('ceres.CeresNode.slices', new=self.ceres_slices):
      self.ceres_node.write(datapoints)

      calls = [call.write([datapoints[1]]), call.write([datapoints[0]])]
      self.ceres_slices[1].assert_has_calls(calls)

  @patch('ceres.CeresSlice.create', new=Mock())
  def test_write_across_slice_boundaries(self):
    datapoints = [ (1080,0.0), (1140,1.0), (1200, 2.0), (1260, 3.0) ]

    with patch('ceres.CeresNode.slices', new=self.ceres_slices):
      self.ceres_node.write(datapoints)
      self.ceres_slices[0].write.assert_called_once_with(datapoints[2:4])
      self.ceres_slices[1].write.assert_called_once_with(datapoints[0:2])

  @patch('ceres.CeresSlice.create')
  def test_write_before_earliest_slice_creates_new(self, slice_create_mock):
    datapoints = [ (300, 0.0) ]
    with patch('ceres.CeresNode.slices', new=self.ceres_slices):
      self.ceres_node.write(datapoints)
      slice_create_mock.assert_called_once_with(self.ceres_node, 300, 60)

  @patch('ceres.CeresSlice.create')
  def test_write_before_earliest_slice_writes_to_new_one(self, slice_create_mock):
    datapoints = [ (300, 0.0) ]
    with patch('ceres.CeresNode.slices', new=self.ceres_slices):
      self.ceres_node.write(datapoints)
      slice_create_mock.return_value.write.assert_called_once_with(datapoints)

  @patch('ceres.CeresSlice.create')
  def test_create_during_write_clears_slice_cache(self, slice_create_mock):
    self.ceres_node.setSliceCachingBehavior('all')
    self.ceres_node.sliceCache = self.ceres_slices
    datapoints = [ (300, 0.0) ]
    with patch('ceres.CeresNode.slices', new=self.ceres_slices):
      self.ceres_node.write(datapoints)
      self.assertEquals(None, self.ceres_node.sliceCache)
*/
