const rand = require ('random-object').randomObject;

const debug = require('debug')('treadmill:ros');

function ros (opts) {
  opts = opts || {};

  let count = parseInt(opts.count || 0, 10);
  if (count < 0 || isNaN(count)) throw new Error("count must be >= 0");
  const infinite = count == 0 || count == Infinity;

  const wait = opts.wait || 0;

  const Readable = require('stream').Readable;
  const r = new Readable({objectMode: true});

  let timer = null;

  r._read = function () {
    if (!wait) {
      debug ('push element');

      r.push(rand(2, 2));
      
      if (!infinite && --count == 0) {
        r.push(null); // done
        if (timer) {
          clearTimeout (timer);
          timer = null;
          debug ('timer cleared')
        }
        debug ('EOF is here');
      }
    }
    else {
      if (timer) {
        debug ('timer active, NOOP')
        return;
      }

      timer = setTimeout (() => {
        timer = null;
        debug ('** push element');

        r.push(rand(2, 2));
        
        if (!infinite && --count == 0) {
          r.push(null); // done
          if (timer) {
            clearTimeout (timer);
            timer = null;
            debug ('** timer cleared')
          }
          debug ('** EOF is here')
        }
      }, wait);
      debug ('** timer set');
    }
  }

  return r;
}

module.exports = ros;
