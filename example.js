#!/usr/bin/env node

var spool = require('./lib/spool.js')();
var es = require('event-stream');
var uuid = require('node-uuid');
var fs = require('fs');
var stream = require('stream');

var counter = 0;


function nullstream() {
  return new stream.Writable({
    write: function(chunk, encoding, next) {
      next();
    }
  });
}

function send(input, retain) {
  var output1 = spool.output();
  var output2 = retain ? nullstream() : process.stdout;

  input.pipe(output1, {end: false});
  input.pipe(output2);

  input.on('end', function() {
    if (!retain) {
      output1.remove();
    }
    output1.end();
  });
}

function write() {
  var docs = [];
  for (var i = 0; i < 10; ++i) {
    docs.push({count: ++counter, id: uuid.v4()});
  }

  var retain = Math.random() > 0.5;
  //var retain = true;

  var input = es.readArray(docs);
  var stringify = es.stringify();
  var inpipe = input.pipe(stringify);
  send(inpipe, retain);
}

setInterval(write, 10);

function resend() {
  var input = spool.read_oldest();
  if (!input) {
    return;
  }

  send(input, false);
  input.on('end', function() {
    input.remove();
  });
}

setInterval(function() {
  resend();
}, 1000);
