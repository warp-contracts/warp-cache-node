const Koa = require('koa');
const cors = require('@koa/cors');
const bodyParser = require('koa-bodyparser');
const compress = require('koa-compress');
const zlib = require('zlib');
const router = require('./router');
const { logConfig, config } = require('./config');
const { drePool, dreReplicaPool } = require('./db/nodeDb');
const accessLogMiddleware = require('./routes/accessLogMiddleware');

const logger = require('./logger')('listener');
const exitHook = require('async-exit-hook');
const { pgClient, warp } = require('./warp');
const { postEvalQueue, registerQueue, updateQueue } = require('./bullQueue');
let port = 8080;

async function runListener() {
  logger.info('🚀🚀🚀 Starting listener node');
  await logConfig();

  await pgClient.open();

  const app = new Koa();
  app
    .use(accessLogMiddleware)
    .use(corsConfig())
    .use(compress(compressionSettings))
    .use(bodyParser())
    .use(router.routes())
    .use(router.allowedMethods())
    .use(async (ctx, next) => {
      await next();
      ctx.redirect('/status');
    });
  app.context.registerQueue = registerQueue;
  app.context.updateQueue = updateQueue;
  app.context.postEvalQueue = postEvalQueue;

  app.listen(port);
}

runListener().catch((e) => {
  logger.error(e);
});

const compressionSettings = {
  threshold: 2048,
  deflate: false,
  br: {
    params: {
      [zlib.constants.BROTLI_PARAM_QUALITY]: 4
    }
  }
};

function corsConfig() {
  return cors({
    async origin() {
      return '*';
    }
  });
}

// Graceful shutdown
async function cleanup(callback) {
  logger.info('Interrupted');
  await warp.close();
  await drePool.end();
  await dreReplicaPool.end();
  logger.info('Clean up finished');
  callback();
}

exitHook(cleanup);
