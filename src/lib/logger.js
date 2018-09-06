const Pino = require('pino');
const pretty = require('pino-pretty');

const pino = Pino({
  level: process.env.LOG_LEVEL || 'info',
  prettyPrint: {
    levelFirst: true,
  },
  prettifier: pretty,
});

exports.pino = pino;
exports.getLogger = context => pino.child({ context });
