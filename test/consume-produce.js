var async =  require('async');
var _ =      require('lodash');
var should = require('should');


var Stream = require ('../pure-mongodb-stream');


function doTimes (fn, n, cb) {
  if (n == 0) return cb ();

  fn (n, err => {
    if (err) return cb (err);
    doTimes (fn, n - 1, cb);
  });
}


describe('TreadMill test', () => {
  describe('section 0', () => {
    it('produces as expected on 1 consumer, 1 producer, 3 elements', done => {

      Stream ({url: 'mongodb://localhost/_test_treadmill_pure_mongo_stream_'}, (err, factory) => {
        if (err) return done (err);

        factory.stream ('q0', (err, stream) => {
          if (err) return done (err);

          stream.producer ((err, producer) => {
            if (err) return console.error (err);

            doTimes ((n, cb) => producer.push ({a: 55, b: 'ewey', n: n}, cb), 3, err => {
              if (err) return done (err);
            });
          });

          stream.consumer ('group1', (err, consumer) => {
            if (err) return done (err);

            var accum = [];

            doTimes ((n, cb) => consumer.pop ((err, res) => {
              if (err) return done (err);
              accum.push (res);
              cb ();
            }), 3, err => {
              if (err) return done (err);
              accum.length.should.equal (3);
              accum.should.eql ([
                { a: 55, b: 'ewey', n: 3 },
                { a: 55, b: 'ewey', n: 2 },
                { a: 55, b: 'ewey', n: 1 }
              ]);

              factory.end (done);
            });
          });
        });
      });
    });


    it('produces as expected on 3 consumer on separated groups, 1 producer, 3 elements', done => {

      Stream ({url: 'mongodb://localhost/_test_treadmill_pure_mongo_stream_'}, (err, factory) => {
        if (err) return done (err);

        factory.stream ('q1', (err, stream) => {
          if (err) return done (err);

          stream.producer ((err, producer) => {
            if (err) return console.error (err);

            doTimes ((n, cb) => producer.push ({a: 55, b: 'ewey', n: n}, cb), 3, err => {
              if (err) return done (err);
            });
          });

          var completed = 0;
          function consumer_body (err, consumer) {
            if (err) return done (err);

            var accum = [];

            doTimes ((n, cb) => consumer.pop ((err, res) => {
              if (err) return done (err);
              accum.push (res);
              cb ();
            }), 3, err => {
              if (err) return done (err);
              accum.length.should.equal (3);
              accum.should.eql ([
                { a: 55, b: 'ewey', n: 3 },
                { a: 55, b: 'ewey', n: 2 },
                { a: 55, b: 'ewey', n: 1 }
              ]);

              completed++;
              if (completed == 3) factory.end (done);
            });
          }

          stream.consumer ('group1', consumer_body);
          stream.consumer ('group2', consumer_body);
          stream.consumer ('group3', consumer_body);
        });
      });
    });

  });
});
