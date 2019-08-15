var async = require ('async');
var stringify = require('stringify-stream');

var ros = require ('./test-lib/ros');

// var Stream = require ('./pure-mongodb-stream');
var Stream = require ('./simple-mongodb-stream');


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
  var src = ros({ count: 100000});
  src.on ('end', () => {console.log ('DONE PUSH'); process.exit(0)});

  consumer.pipe (stringify()).pipe (process.stdout);
  src.pipe(producer);
});
