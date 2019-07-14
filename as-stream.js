var Stream = require ('./pure-mongodb-stream');
var async = require ('async');
var rand = require ('random-object').randomObject;
var stringify = require('stringify-stream');

function ros (opts) {
  opts = opts || {};

  var count = parseInt(opts.count || 0, 10);
  if (count < 0 || isNaN(count)) throw new Error("count must be >= 0");
  var infinite = count == 0 || count == Infinity;

  var Readable = require('stream').Readable;

  var r = new Readable({objectMode: true});

  r._read = function () {
    r.push(rand(2, 2));

    if (!infinite && --count == 0) {
      r.push(null) // done
    }
  }

  return r;
}



//stream.consumer ('group1', (err, consumer) => {
//  if (err) return console.error (err);

async.waterfall ([
  cb => Stream ({url: 'mongodb://localhost/akka'}, cb),
  (factory, cb) =>  factory.stream ('q1', (err, qstream) => {
    if (err) return cb (err);
    cb (null, factory, qstream);
  }),
  (factory, qstream, cb) => qstream.producer ((err, producer) => {
    if (err) return cb (err);
    cb (null, factory, qstream, producer);
  }),
  (factory, qstream, producer, cb) => qstream.consumer ('group-1', (err, consumer) => {
    if (err) return cb (err);
    cb (null, factory, qstream, producer, consumer);
  }),
], (err, factory, qstream, producer, consumer) => {
  if (err) return console.error (err);
  ros({ count: 11111}).pipe(producer);

  consumer.pipe (stringify()).pipe (process.stdout);
});
