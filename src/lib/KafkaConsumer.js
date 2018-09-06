const { KafkaConsumer: Consumer } = require('node-rdkafka');
// const { ERRORS: errCodes } = require('node-rdkafka').CODES;
const { from, to, duplex } = require('mississippi');
// const uuid = require('uuid').v4;
const logger = require('./logger');

function noop(cb) { return cb(); }

class KafkaConsumer extends Consumer {
  constructor(options) {
    const { opts, globalConfig, topicConfig = {} } = options;
    const { name = 'KafkaConsumer' } = opts;
    super(globalConfig, topicConfig);

    this.logger = logger.getLogger(name);
    this._stats = {
      consume: {
        total: 0,
        errors: 0,
      },
      commit: {
        total: 0,
        errors: 0,
      },
    };
    this.stopReading = false;
  }

  consumeReadable(options = { highWaterMark: 4 }) {
    const opts = { ...options, objectMode: true }; // enforce ObjectMode

    if (this.readable) return this.readable;

    this.readable = from(opts, (size, next) => {
      if (this.stopReading) {
        this.logger.info('stop reading');
        return next(null, null); // stop feeding the readStream
      }
      const fetcher = () => { // eslint-disable-line arrow-body-style
        return this.consume(1, (err, messages) => {
          if (err) {
            this._stats.consume.errors += 1;
            return next(err, null);
          }
          this._stats.consume.total += 1;

          if (!Array.isArray(messages)) {
            const error = new TypeError('invalid messages (no Array)');
            return next(error, null);
          }
          if (!messages.length) {
            this.logger.trace({ stream: 'read' }, 'no messages available');
            return setTimeout(fetcher, 50);
          }
          const message = messages[0];

          return setImmediate(() => next(null, message));
        });
      };
      return fetcher();
    });

    return this.readable;
  }

  /**
   * @description commit writeable stream
   * @param {object=} options
   * @returns {Writable} Writeable for commits
   * @memberof KafkaConsumer
   */
  commitWritable(options = { highWaterMark: 4 }, flushCb = noop) {
    const opts = { ...options, objectMode: true }; // enforce ObjectMode

    if (this.writable) return this.writable;

    this.writable = to(opts, (commit, enc, cb) => {
      const { topic, partition, offset } = commit;
      this._stats.commit.total += 1;
      try {
        this.commit({ topic, partition, offset });
        this.logger.trace({ topic, partition, offset }, 'committed');
      } catch (err) {
        this._stats.commit.errors += 1;
        this.logger.error(err);
        return cb(err);
      }

      return setImmediate(() => cb());
    }, (cb) => {
      this.logger.trace('flush writable');

      // @ts-ignore
      return setImmediate(() => flushCb(cb));
    });

    return this.writable;
  }

  consumerDuplex(options = { readableHighWaterMark: 4, writableHighWaterMark: 4 }) {
    const opts = {
      ...options,
      readableObjectMode: true,
      writableObjectMode: true,
    };

    if (this.duplexStream) return this.duplexStream;

    this.duplexStream = duplex(this.commitWritable(), this.consumeReadable(), opts);

    return this.duplexStream;
  }

  stop() {
    this.stopReading = true;
  }

  shutdown(cb) {
    this
      .unsubscribe()
      .disconnect((err, data) => {
        if (err) {
          this.logger.error(err);
          return cb(err, null);
        }
        return cb(null, data);
      })
    ;
  }
}

module.exports = KafkaConsumer;
