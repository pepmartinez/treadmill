var MongoClient = require ('mongodb').MongoClient;
var Timestamp =   require ('mongodb').Timestamp;

var async = require ('async');
var _ =     require ('lodash');

var debug = require('debug')('stream');


/////////////////////////////////////////////////////////////////////////////////////////////
class StreamFactory {
  constructor (mongo_url, opts) {
    this._mongo_url = mongo_url;
    this._opts = opts || {};
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
      if (err) return cb (err);
      debug ('factory with url %s, opts %o closed ok', this._mongo_url, this._opts);
    });
  }

  /////////////////////////////////////////////////////////////////////////////////////////////
  stream (name, cb) {
    var s = new Stream (this, name);
    s.init (cb);
  }
}



/////////////////////////////////////////////////////////////////////////////////////////////
class Stream {
  constructor (factory, name) {
    this._factory = factory;
    this._name = name;
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


  producer (cb) {
    cb (null, new StreamProducer (this));
  }


  consumer (group, cb) {
    var c = new StreamConsumer (this, group);
    c.init (cb);
  }
}


/////////////////////////////////////////////////////////////////////////////////////////////
class StreamProducer {
  constructor (stream) {
    this._stream = stream;
    this._factory = stream._factory;
    this._qname = stream._name;

    debug ('created producer in stream %s', stream._name);
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
class StreamConsumer {
  constructor (stream, group) {
    this._stream = stream;
    this._factory = stream._factory;
    this._qname = stream._name;
    this._group = group;

    debug ('created consumer in stream %s for group %s', stream._name, group);
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
            if (is_ok) return cb (null, elem.pl);
            debug ('element already processed, keep on looping');
            this._try_a_pop (cb);
          });
        }
      });
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
      debug ('_update_group_read_window: got permission for the id %o: %s', this._last_read_id, ok);
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

        this._cursor = this._stream._capped_coll.find ({ts: {$gt: ts}}, cursor_opts);
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


module.exports = (mongo_url, opts, cb) => {
  var f = new StreamFactory (mongo_url, opts);
  f.init (cb);
}
