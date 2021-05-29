var MongoClient = require ('mongodb').MongoClient;
var Timestamp =   require ('mongodb').Timestamp;

var async = require ('async');
var _ =     require ('lodash');

var Interfaces = require ('./interfaces');

var debug = require('debug')('pure-mongodb-stream');


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

      // create utility collections
      this._monotonic_ids_coll =      this._db.collection ("__treadmill_monotonic_ids__");
      this._last_ids_processed_coll = this._db.collection ("__treadmill_last_ids_processed__");

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
    async.waterfall ([
      cb => this._next_in_seq (cb),
      (nis, cb) => {
        var ts = new Timestamp (nis, _.floor (_.now () / 1000));
        debug ('pushing with ts %o', ts);
        this._stream._capped_coll.insertOne ({ts: ts, pl: pl}, cb);
      },
    ], (err, res) => {
      if (err) return cb (err);
      debug ('stream %s: pushed payload %o', this._qname, pl);
      cb ();
    });
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  _next_in_seq (cb) {
    var q = {_id: this._qname};
    var upd = {$inc: {v: 1}};
    var opts = {upsert: true, returnOriginal: false};

    this._factory._monotonic_ids_coll.findOneAndUpdate (q, upd, opts, (err, res) => {
      if (err) return cb (err);
      var val = res.value.v || 0;
      debug ('next_in_seq: got %d', val);
      cb (null, val);
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
    // insert 'lower-limit' for last_processed_id, ignore errors
    this._factory._last_ids_processed_coll.insertOne ({_id: {q: this._qname, g: this._group}, v: new Timestamp (1, 1)}, () => cb (null, this));
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  pop (cb) {
    this._try_a_pop (cb);
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  _try_a_pop (cb) {
    this._ensure_cursor (err => {
      if (err) return cb (err);

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
          this._last_read_id = elem.ts;

          this._update_group_read_window ((err, is_ok) => {
            if (is_ok) {
              debug ('element not processed on group %s, return it', this._group);
              return cb (null, elem.pl);
            }

            debug ('element already processed on group %s, keep on looping', this._group);
            this._try_a_pop (cb);
          });
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
  _update_group_read_window (cb) {
    var q = {_id: {q: this._qname, g: this._group}, v: {$lt: this._last_read_id}};
    var upd = {$set: {v: this._last_read_id}};

// TODO cache last _last_read_id, do not attemt update if posterior


    this._factory._last_ids_processed_coll.findOneAndUpdate (q, upd, (err, res) => {
      if (err) return cb (err);
      var ok = res.lastErrorObject.updatedExisting;
      debug ('_update_group_read_window[%s]: got permission for the id %o: %s', this._group, this._last_read_id, ok);
      cb (null, ok);
    });
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  _ensure_cursor (cb) {
    if (!this._cursor) {
      this._get_last_ts_processed ((err, ts) => {
        if (err) return cb (err);

        debug ('creating cursor, last_ts_processed is %o', ts);

        var cursor_opts = {
          awaitData: true,
          tailable: true,
          noCursorTimeout: true,
          oplogReplay: true
        };

        this._cursor = this._stream._capped_coll.find ({ts: {$gt: ts}}, cursor_opts).sort ({$natural: -1});
        debug ('cursor created');
        cb ();
      });
    }
    else {
      setImmediate (cb);
    }
  }


  /////////////////////////////////////////////////////////////////////////////////////////////
  _get_last_ts_processed (cb) {
    var q = {_id: {q: this._qname, g: this._group}};

    this._factory._last_ids_processed_coll.findOne (q, (err, res) => {
      if (err) return cb (err);

      if (res) {
        debug ('_get_last_ts_processed: got %o', res);
        return cb (null, res.v);
      }

      res = new Timestamp (0, _.floor (_.now () / 1000));
      debug ('_get_last_ts_processed: got no result from db, init to %o', res);
      return cb (null, res);
    });
  }
}


module.exports = (opts, cb) => {
  var f = new StreamFactory (opts);
  f.init (cb);
}
