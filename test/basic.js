var expect = require('chai').expect;
var Spool = require('../lib/spool');

describe('basic', function() {
  it('should create the appropriate spool directory and one spool file', function(done) {
    var spool = Spool({
      dir: 'test_dir'
    });
    done();
  });
});
