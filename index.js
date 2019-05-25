var _ =     require ('lodash');


var Stream = require ('./stream');



function doTimes (fn, n, cb) {
  if (n == 0) return cb ();

  fn (n, err => {
    if (err) return cb (err);
    doTimes (fn, n - 1, cb);
  });
}




Stream ('mongodb://localhost/akka', {}, (err, factory) => {
  if (err) return console.error (err);
  console.log ('init ook');


  factory.stream ('q1', (err, stream) => {
    if (err) return console.error (err);

    setTimeout (() => {
      stream.producer ((err, producer) => {
        if (err) return console.error (err);

        doTimes ((n, cb) => {
          producer.push ({a: 55, b: 'ewey', n: n}, cb);
        }, 100000, err => {
          if (err) return console.error (err);
          console.log ('push done');
        });
      });
    }, 100);

    stream.consumer ('group1', (err, consumer) => {
      if (err) return console.error (err);

      doTimes ((n, cb) => {
        consumer.pop ((err, res) => {
          if (_.floor (res.n % 1000) == 0) console.log ('c1', res);
          cb (err);
        });
      }, 100000, err => {
        if (err) return console.error (err);
        console.log ('pop done');
      });
    });

    stream.consumer ('group1', (err, consumer) => {
      if (err) return console.error (err);

      doTimes ((n, cb) => {
        consumer.pop ((err, res) => {
          if (_.floor (res.n % 1000) == 0) console.log ('c2', res);
          cb (err);
        });
      }, 100000, err => {
        if (err) return console.error (err);
        console.log ('pop done');
      });
    });

  });
});
