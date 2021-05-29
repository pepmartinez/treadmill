var MongoClient = require ('mongodb').MongoClient;
var Timestamp =   require ('mongodb').Timestamp;

var async = require ('async');
var _ =     require ('lodash');

var Interfaces = require ('./interfaces');

var debug = require('debug')('treadmill:simple-mongodb-stream');


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
  constructor (factory, name) {
    super (factory, name);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  init (cb) {
    this._factory._db.createCollection (this._name, {
      capped: true,
      size: this._factory._opts.size || 1024*1024
    }, (err, res) => {
      if (err) return cb (err);
      this._capped_coll = res;
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
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  _write (chunk, enc, next) {
    debug ('called _write with ', chunk, enc);
    this.push (chunk, next);
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  push (pl, cb) {
    var ts = new Timestamp (0, _.floor (_.now () / 1000));
    debug ('pushing with ts %o', ts);

    this._stream._capped_coll.insertOne ({ts: ts, pl: pl}, err => {
      if (err) return cb (err);
      debug ('stream %s: pushed payload %o', this._qname, pl);
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


  /////////////////////////////////////////////////////////////////////////////////////////////
  _ensure_cursor (cb) {
    if (!this._cursor) {
      var ts = this._get_last_ts_processed ();
      debug ('creating cursor, last_ts_processed is %o', ts);

      var cursor_opts = {
        awaitData: true,
        tailable: true,
        noCursorTimeout: true,
        oplogReplay: true
      };

      var q = {
        ts: {$gt: ts}
      };

      this._cursor = this._stream._capped_coll.find (q, cursor_opts);
      debug ('cursor created with q %O, opts %O', q, cursor_opts);
      cb ();
    }
    else {
      debug ('cursor still standing');
      setImmediate (cb);
    }
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  _get_last_ts_processed () {
    var res;

    if (this._last_read_ts) {
      res = this._last_read_ts;
    }
    else {
      res = new Timestamp (0, _.floor (_.now () / 1000));  // get only new elems TODO set low to millis
      // res = new Timestamp (0, 0);                       // get all
    }

    return res;
  }
}


module.exports = (opts, cb) => {
  var f = new StreamFactory (opts);
  f.init (cb);
}
