
var debug = require('debug')('stream');
var stream = require ('stream');

class StreamFactory {
  constructor (opts) {
    this._opts = opts || {};
    debug ('created stream factory %j', this.opts());
  }

  opts () {return this._opts;}

  init (cb) {
    setImmediate (() => cb (null, this));
  }

  end (cb) {
    setImmediate (() => cb ());
  }

  stream (name, cb) {
    setImmediate (() => cb ());
  }
}


class Stream {
  constructor (factory, name) {
    this._factory = factory;
    this._name = name;
    debug ('created stream %s', this.name());
  }

  factory () {return this._factory;}
  name () {return this._name;}

  init (cb) {
    setImmediate (() => cb (null, this));
  }


  producer (cb) {
    setImmediate (() => cb ());
  }


  consumer (group, cb) {
    setImmediate (() => cb ());
  }
}


class StreamProducer extends stream.Writable {
  constructor (stream) {
    super ({objectMode: true});
    this._stream = stream;
    this._factory = stream.factory ();
    this._qname = stream.name ();

    debug ('created producer in stream %s', stream.name());
  }

  factory () {return this._factory;}
  qname () {return this._qname;}

  init (cb) {
    setImmediate (() => cb (null, this));
  }

  push (pl, cb) {
    setImmediate (() => cb ());
  }
}

class StreamConsumer extends stream.Readable {
  constructor (stream, group) {
    super ({objectMode: true});
    this._stream = stream;
    this._factory = stream.factory ();
    this._qname = stream.name ();
    this._group = group;

    debug ('created consumer in stream %s for group %s', stream.name (), group);
  }

  factory () {return this._factory;}
  qname () {return this._qname;}
  group () {return this._group;}

  init (cb) {
    setImmediate (() => cb (null, this));
  }

  pop (cb) {
    setImmediate (() => cb ());
  }
}


module.exports = {
  StreamFactory,
  Stream,
  StreamProducer,
  StreamConsumer
};
