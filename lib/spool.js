var fs = require('fs');
var zlib = require('zlib');
var path = require('path');
var stream = require('stream');

function pad(val) {
  var str = val.toString();
  return Array(30 - str.length).join('0') + str;
}

function strip(filename) {
  var val = parseInt(path.basename(filename, '.log'), 10);
  return isNaN(val) ? 0 : val;
}

function nullstream() {
  return new stream.Writable({
    write: function(chunk, encoding, next) {
      next();
    }
  });
}

function make_directories(fullpath, tiers) {
  var parts = fullpath.split(path.sep).filter(function(part) { return part.length > 0; });
  var curr = [];
  while (parts.length > 0) {
    curr.push(parts.shift());
    var dir = path.sep + path.join.apply(this, curr);
    try {
      fs.mkdirSync(dir);
    }
    catch(err) {

    }
  }

  for (var i = 0; i < tiers; ++i) {
    try {
      fs.mkdirSync(path.join(fullpath, i.toString()));
    }
    catch(err) {

    }
  }
}

function Spool(opts) {
  if (!(this instanceof Spool)) {
    return new Spool(opts);
  }

  opts = opts || {};

  var self = this;
  self.opts = opts;
  self.dir = path.resolve(opts.dir || 'spool');
  self.num_tiers = opts.num_tiers || 8;
  self.tier_size = opts.tier_size || 10;

  make_directories(self.dir, self.num_tiers);

  self.count = 0;
  self.tiers = {};

  for (var tier = 0; tier < self.num_tiers; ++tier) {
    var unlocked_count = 0;
    var records = fs.readdirSync(path.resolve(self.dir, tier.toString())).map(function(filename) {
      self.count = Math.max(self.count, strip(filename));
      ++unlocked_count;
      return {filename: filename, fullpath: path.resolve(self.dir, tier.toString(), filename), locked: false, status: 'stored', tier: tier};
    });

    self.tiers[tier] = {
      unlocked: unlocked_count,
      records: records
    };
  }
}

Spool.prototype._remove_record = function(record) {
  var self = this;

  record.status = 'deleting';
  fs.unlink(record.fullpath, function(err) {
    if (!err) {
      record.status = 'deleted';
    }
  });

  var tier_record = self.tiers[record.tier];
  tier_record.records = tier_record.records.filter(function(rec) {
    return rec.filename != record.filename;
  });
}

Spool.prototype._handle_errors = function(stream, record) {
  var self = this;

  stream.on('error', function(err) {
    console.error(err);
    self._remove_record(record);
  });
}

Spool.prototype._next_file = function(tier) {
  var self = this;
  var filename = pad(self.count) + '.log.gz';
  var fullpath = path.resolve(self.dir, tier.toString(), filename);
  ++self.count;

  var zip = zlib.createGzip();
  var out = fs.createWriteStream(fullpath);
  var retain = true;

  zip.remove = function() { retain = false; };

  var record = {filename: filename, fullpath: fullpath, locked: true, status: 'writing', tier: tier};
  var tier_record = self.tiers[tier];
  tier_record.records.push(record);

  function resolve() {
    if (retain) {
      record.locked = false;
      record.status = 'stored';
      ++tier_record.unlocked;
      if (tier_record.unlocked >= self.tier_size) {
        self.merge(tier);
      }
    }
    else {
      self._remove_record(record);
    }
  }

  out.on('finish', resolve)
  out.on('error', function(err) {
    zip.emit('error', err);
  });

  zip.pipe(out);

  self._handle_errors(zip, record);

  return zip;
}

Spool.prototype._make_input_stream = function(record) {
  var self = this;

  var gunzip = zlib.createGunzip();
  var input = fs.createReadStream(record.fullpath);

  input.pipe(gunzip);

  input.on('error', function(err) {
    gunzip.emit('error', err);
  });

  self._handle_errors(gunzip, record);

  return gunzip;
}

Spool.prototype.merge = function(tier) {
  var self = this;

  var records = [];
  var record_files = {};
  var tier_record = self.tiers[tier];
  Object.keys(tier_record.records).forEach(function(name) {
    var record = tier_record.records[name];

    if (!record.locked) {
      record.locked = true;
      record.status = 'merging';
      records.push(record);
      record_files[record.filename] = true;
      --tier_record.unlocked;
    }
  });

  // if we're at our maximum tier, essentially pipe to null in order to remove oldest segments
  var output_stream = (tier + 1 >= self.num_tiers) ? nullstream() : self._next_file(tier + 1);

  var merged = false;
  output_stream.on('finish', function() {
    tier_record.records = tier_record.records.filter(function(rec) { return !record_files[rec.filename]; });
  });

  setTimeout(function() {
    if (!merged) {
      console.error('merge timeout:', tier);
      console.error(records.map(function(record) { return {filename: record.filename, status: record.status}; }));
    }
  }, 10000);

  var idx = 0;
  function process() {
    var next = records[idx++];
    if (!next) {
      merged = true;
      output_stream.end();
    }
    else {
      next.stream = self._make_input_stream(next);
      next.stream.on('close', function() {
        delete next.stream;
        self._remove_record(next);
        process();
      });
      next.stream.on('error', function() {
        process();
      });
      next.stream.pipe(output_stream, {end: false});
    }
  }

  process();
}

Spool.prototype.output = function() {
  var self = this;
  return self._next_file(0);
}

Spool.prototype.read_oldest = function() {
  var self = this;
  for (var tier = self.num_tiers - 1; tier >= 0; --tier) {
    var tier_record = self.tiers[tier];
    for (var idx = 0; idx < tier_record.records.length; ++idx) {
      var record = tier_record.records[idx];
      if (!record.locked) {
        record.locked = true;
        record.status = 'reading';
        --tier_record.unlocked;

        var retain = true;
        record.stream = self._make_input_stream(record);
        record.stream.on('close', function() {
          if (!retain) {
            delete record.stream;
            self._remove_record(record);
          }
        });
        record.stream.remove = function() {
          retain = false;
        }
        return record.stream;
      }
    }
  }
  return null;
}

module.exports = Spool;
