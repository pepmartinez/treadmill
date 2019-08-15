var rand = require ('random-object').randomObject;

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

module.exports = ros;
