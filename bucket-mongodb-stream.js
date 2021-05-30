var MongoClient = require ('mongodb').MongoClient;

var async = require ('async');
var _ =     require ('lodash');

var Interfaces = require ('./interfaces');

var debug = require('debug')('treadmill:bucket-mongodb-stream');


/////////////////////////////////////////////////////////////////////////////////////////////
class StreamFactory extends Interfaces.StreamFactory{
  constructor (opts) {
    super (opts);
    this._mongo_url = this.opts().url;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  init (cb) {
    MongoClient.connect (this._mongo_url, { useNewUrlParser: true }, (err, client) => {
      if (err) return cb (err);
      this._mongodb_cl = client;
      this._db = client.db ( this._opts.db);

      debug ('factory initialized ok with url %s, opts %o', this._mongo_url, this._opts);
      cb (null, this);
    });
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  end (cb) {
    this._mongodb_cl.close (err => {
      if (err && cb) return cb (err);
      debug ('factory with url %s, opts %o closed ok', this._mongo_url, this._opts);
      if (cb) cb ();
    });
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  stream (name, cb) {
    var s = new Stream (this, name);
    s.init (cb);
  }
}



/////////////////////////////////////////////////////////////////////////////////////////////
class Stream extends Interfaces.Stream {
  constructor (factory, name, opts) {
    super (factory, name);
    this._opts = opts || {};
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  init (cb) {
    async.waterfall ([
      cb => this._factory._db.createCollection (this._name, cb),
      (coll, cb) => coll.createIndex ({t: 1}, {expireAfterSeconds: this._opts.retention || 300}, (err, res) => cb (err, res, coll))
    ], (err, res, coll) => {
      if (err) return cb (err);
      this._coll = coll;
      debug ('created stream %s', this._name);
      cb (null, this);
    });
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  producer (cb) {
    cb (null, new StreamProducer (this));
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  consumer (group, cb) {
    var c = new StreamConsumer (this, group);
    c.init (cb);
  }
}


/////////////////////////////////////////////////////////////////////////////////////////////
class StreamProducer extends Interfaces.StreamProducer {
  constructor (stream) {
    super (stream);
    this._insert_bucket = [];
    this._flush_timer = null;
    this._bucket_max_size = stream._opts.bucket_max_size || 32;
    this._flush_period = stream._opts.flush_period || 250;

    debug ('streamproducer created on stream %s, bucket-max-size %d, flush-period', stream.name(), this._bucket_max_size);
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  // float to interface
  _write (chunk, enc, next) {
    debug ('called _write with ', chunk, enc);
    this.push (chunk, next);
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  push (pl, cb) {
    const t = new Date ();
    this._insert_bucket.push ({t, pl});
    debug ('pushed with ts %o', t);

    if (this._insert_bucket.length >= this._bucket_max_size) {
      if (this._flush_timer) clearTimeout (this._flush_timer);
      this._flush_timer = null;

      debug ('cancelled periodic_flush');

      this._flush_bucket (cb);
    }
    else {
      if (this._flush_timer == null) {
        debug ('first insert of bucket, set periodic_flush');
        this._set_periodic_flush ();
      }

      setImmediate (() => cb (null, null));
    }
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  _set_periodic_flush () {
    if (this._flush_timer) return;
  
    this._flush_timer = setTimeout (() => {
      this._flush_timer = null;
  
      debug ('flush_timer went off');
  
      if (this._insert_bucket.length) {
        this._flush_bucket ((err, res) => {
          if (err) {
            // keep retrying
            this._set_periodic_flush ();
          }
        });
      }
      else {
        // nothing to insert, stop
      }
    }, this._flush_period);
  
    debug ('_set_periodic_flush set, wait %d msecs', this._flush_period);
  }


  /////////////////////////////////////////
  _flush_bucket (cb) {
    debug ('flushing bucket with %d elems', this._insert_bucket.length);
    
    const p = {
      t:  new Date (), 
      b:  this._insert_bucket,
      gi: []
    };
  
    this._stream._coll.insertOne (p, err => {
      this._insert_bucket = [];
      if (err) return cb (err);
      debug ('stream %s: pushed payload %o', this._qname, p);
      cb ();
    });
  }
}







/////////////////////////////////////////////////////////////////////////////////////////////
class StreamConsumer extends Interfaces.StreamConsumer {
  constructor (stream, group) {
    super (stream, group);
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  init (cb) {
    cb (null, this);
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  pop (cb) {
    debug ('pop called');
    this._try_a_pop (cb);
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  _try_a_pop (cb) {
    this._ensure_cursor (err => {
      if (err) return cb (err);

      debug ('reading from cursor');

      this._cursor.next ((err, elem) => {
        if (!elem) {
          debug ('cursor exhausted, reload...');
          this._cursor.close (() => {
            this._cursor = null;
            setTimeout (() => this._try_a_pop (cb), 5000);
          });
        }
        else {
          debug ('got an element %o', elem);
          this._last_read_ts = elem.ts;
          cb (null, elem.pl);
        }
      });
    });
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  _read () {
    this._try_a_pop ((err, ret) => {
      if (err) return this.emit ('error', err);
      this.push (ret);
    });
  }

}


module.exports = (opts, cb) => {
  var f = new StreamFactory (opts);
  f.init (cb);
}
