const { Producer } = require('node-rdkafka');
const { ERRORS: errCodes } = require('node-rdkafka').CODES;
const { from, to, duplex } = require('mississippi');
const logger = require('./logger');

class KafkaProducer extends Producer {
  constructor(options = {}) {
    const { opts = {}, globalConfig = {}, topicConfig = {} } = options;
    const { name = 'KafkaProducer' } = opts;
    super(globalConfig, topicConfig);

    this.logger = logger.getLogger(name);
    this.deliveryreports = [];
    this.stopReading = false;
    this._stats = {
      produce: {
        total: 0,
        queueFull: 0,
        errors: 0,
      },
      delivery: {
        total: 0,
        errors: 0,
      },
    };

    this.on('delivery-report', (err, report) => {
      if (err) {
        this._stats.delivery.errors += 1;
        return this.logger.error(err);
      }
      const { topic, partition, offset, opaque } = report;

      this._stats.delivery.total += 1;
      this.logger.trace({ topic, partition, offset, opaque }, 'delivery report');

      if (report) return this.deliveryreports.push(report);
      return report;
    });
  }

  /**
   * @description
   * @param {*} [options={ highWaterMark: 4 }]
   * @returns {NodeJS.ReadableStream}
   * @memberof KafkaProducer
   */
  deliveryreportReadable(options = { highWaterMark: 4 }) {
    const opts = { ...options, objectMode: true }; // enforce ObjectMode

    if (this.readable) return this.readable;

    this.readable = from(opts, (size, next) => {
      const fetcher = () => {
        if (this.stopReading) this.deliveryreports.push(null);
        if (this.deliveryreports.length) {
          return setImmediate(() => next(null, this.deliveryreports.shift()));
        }
        return setTimeout(fetcher, 100);
      };

      return fetcher();
    });

    return this.readable;
  }

  /**
   * @description
   * @param {*} [options={ highWaterMark: 4 }]
   * @returns {NodeJS.WritableStream}
   * @memberof KafkaProducer
   */
  produceWritable(options = { highWaterMark: 4 }) {
    const opts = { ...options, objectMode: true }; // enforce ObjectMode

    if (this.writable) return this.writable;

    this.writable = to(opts, (message, enc, cb) => {
      const {
        topic,
        partition = null,
        value,
        key = undefined,
        timestamp = Date.now(),
        opaque = undefined,
      } = message;

      this.logger.trace({ topic, opaque }, 'produce message');

      const pusher = () => {
        try {
          this.produce(topic, partition, value, key, timestamp, opaque);
          this._stats.produce.total += 1;
        } catch (err) {
          this._stats.produce.errors += 1;
          if (errCodes.ERR__QUEUE_FULL === err.code) {
            this._stats.produce.queueFull += 1;
            this.logger.warn({ errorcode: err.code }, 'local queue full');
            this.poll();
            return setTimeout(pusher, 500);
          }
          this.logger.error(err);
          return cb(err);
        }

        return setImmediate(() => cb());
      };

      return pusher();
    }, (cb) => {
      this.logger.info('flush Kafka Consumer ...');
      this.flush(5000, (err, data) => { // flush Kafka Consumer queue
        this.logger.info('Kafka Consumer flushed');
        if (err) {
          this.logger.error(err);
          return cb(err);
        }

        return setTimeout(() => {
          this.logger.info('poll Kafka Consumer for events ...');
          this.poll(); // poll for events, delivery-reports
          return setTimeout(() => cb(), 2000);
        }, 2500);
      });
    });

    // @ts-ignore
    this.writable.on('finish', () => {
      this.logger.info('Stop fetching delivery reports ...');
      this.deliveryreports.push(null);
    });

    return this.writable;
  }

  /**
   * @description
   * @param {object} options
   * @param {number=} options.readableHighWaterMark
   * @param {number=} options.writableHighWaterMark
   * @returns
   * @memberof KafkaProducer
   */
  producerDuplex(options = { readableHighWaterMark: 4, writableHighWaterMark: 4 }) {
    const opts = {
      ...options,
      readableObjectMode: true,
      writableObjectMode: true,
    };

    if (this.duplexStream) return this.duplexStream;

    // @ts-ignore
    this.duplexStream = duplex(this.produceWritable(), this.deliveryreportReadable(), opts);

    return this.duplexStream;
  }

  stop() {
    this.stopReading = true;
  }

  shutdown(cb) {
    this
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

module.exports = KafkaProducer;
