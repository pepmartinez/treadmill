var async =  require('async');
var _ =      require('lodash');
var should = require('should');


var Stream = require ('./pure-mongodb-stream');

describe('TreadMill test', () => {
  describe('section 0', () => {
    it('produces as expected on 1 consumer, 1 producer, 3 elements', done => {

      Stream ({url: 'mongodb://localhost/akka'}, (err, factory) => {
        if (err) return console.error (err);
      });

    });
  });
});
