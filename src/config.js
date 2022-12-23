require('dotenv').config();
const fs = require('fs');
const Arweave = require('arweave');
const pjson = require('../package.json');
const validate = require('./configValidator');
const logger = require('./logger')('config');

const nodeJwk = readNodeJwk();
const arweave = getArweave();
let warpSdkConfig = {
  'warp-contracts': pjson.dependencies['warp-contracts'],
  'warp-contracts-lmdb': pjson.dependencies['warp-contracts-lmdb'],
  'warp-contracts-evaluation-progress-plugin': pjson.dependencies['warp-contracts-evaluation-progress-plugin'],
  'warp-contracts-nlp-plugin': pjson.dependencies['warp-contracts-nlp-plugin'],
  'warp-contracts-plugin-ethers': pjson.dependencies['warp-contracts-plugin-ethers']
};
const evaluationOptions = {
  useVM2: process.env.EVALUATION_USEVM2,
  maxCallDepth: process.env.EVALUATION_MAXCALLDEPTH,
  maxInteractionEvaluationTimeSeconds: process.env.EVALUATION_MAXINTERACTIONEVALUATIONTIMESECONDS,
  allowBigInt: process.env.EVALUATION_ALLOWBIGINT,
  unsafeClient: process.env.EVALUATION_UNSAFECLIENT,
  internalWrites: process.env.EVALUATION_INTERNALWRITES
};

const config = {
  env: process.env.ENV,
  streamId: process.env.STREAMR_STREAM_ID,
  arweave,
  gwPubSubConfig: {
    port: process.env.GW_PORT,
    host: process.env.GW_HOST,
    username: process.env.GW_USERNAME,
    password: process.env.GW_PASSWORD,
    tls: process.env.GW_TLS === 'true',
    enableOfflineQueue: process.env.GW_ENABLE_OFFLINE_QUEUE,
    lazyConnect: process.env.GW_LAZY_CONNECT
  },
  bullMqConnection: {
    port: process.env.BULLMQ_PORT,
    host: process.env.BULLMQ_HOST,
    username: process.env.BULLMQ_USERNAME,
    password: process.env.BULLMQ_PASSWORD,
    tls: process.env.BULLMQ_TLS,
    enableOfflineQueue: process.env.BULLMQ_ENABLE_OFFLINE_QUEUE,
    lazyConnect: process.env.BULLMQ_LAZY_CONNECT
  },
  appSync: {
    key: process.env.APPSYNC_KEY,
    publishState: process.env.APPSYNC_PUBLISH_STATE.toLowerCase() === 'true'
  },
  pubsub: {
    type: process.env.PUBSUB_TYPE
  },
  nodeJwk,
  evaluationOptions,
  warpSdkConfig,
  nodeManifest: (async () => await getNodeManifest())(),
  workersConfig: {
    register: process.env.WORKERS_REGISTER,
    update: process.env.WORKERS_UPDATE,
    jobIdRefreshSeconds: process.env.WORKERS_JOB_ID_REFRESH_SECONDS,
    maxFailures: process.env.WORKERS_MAX_FAILURES,
    maxStateSizeB: process.env.WORKERS_MAX_STATESIZE
  }
};

(async () => await logConfig(config))();
validate(config);
module.exports.config = config;

function getArweave() {
  return Arweave.init({
    host: 'arweave.net',
    port: 443,
    protocol: 'https',
    timeout: 60000,
    logging: false
  });
}

function readNodeJwk() {
  if (!process.env.NODE_JWK_KEY) throw new Error('NODE_JWK_KEY is required');
  return JSON.parse(process.env.NODE_JWK_KEY);
}

async function getNodeManifest() {
  return {
    gitCommitHash: getGitCommitHash(),
    warpSdkConfig,
    evaluationOptions,
    owner: nodeJwk.n,
    walletAddress: await arweave.wallets.ownerToAddress(nodeJwk.n)
  };
}

function getGitCommitHash() {
  let hash = '';
  if (fs.existsSync('./GIT_HASH')) {
    hash = fs.readFileSync('./GIT_HASH').toString().trim();
  } else if (fs.existsSync('.git')) {
    hash = require('child_process').execSync('git rev-parse HEAD').toString().trim();
  } else {
    throw new Error("Can't read git commit hash.");
  }
  return hash;
}

async function logConfig(config) {
  const nodeManifest = await config.nodeManifest;
  logger.info('---------');
  logger.info('Node configuration');
  logger.info('---------');
  logger.info('Environment', config.env);
  logger.info('---------');
  logger.info('Arweave public address', nodeManifest.walletAddress);
  logger.info('gitCommitHash', nodeManifest.gitCommitHash);
  logger.info('---------');
  logger.info('--- gwPubSubConfig');
  logger.info('--- host', config.gwPubSubConfig.host);
  logger.info('--- port', config.gwPubSubConfig.port);
  logger.info('--- tls', config.gwPubSubConfig.tls);
  logger.info('--- lazyConnect', config.gwPubSubConfig.lazyConnect);
  logger.info('--- enableOfflineQueue', config.gwPubSubConfig.enableOfflineQueue);
  logger.info('--- /gwPubSubConfig');
  logger.info('---------');
  logger.info('--- bullMqConnection');
  logger.info('--- port', config.bullMqConnection.port);
  logger.info('--- host', config.bullMqConnection.host);
  logger.info('--- username', config.bullMqConnection.username);
  logger.info('--- tls', config.bullMqConnection.tls);
  logger.info('--- enableOfflineQueue', config.bullMqConnection.enableOfflineQueue);
  logger.info('--- lazyConnect', config.bullMqConnection.lazyConnect);
  logger.info('--- /bullMqConnection');
  logger.info('---------');
  logger.info('evaluationOptions', config.evaluationOptions);
  logger.info('workersConfig', config.workersConfig);
}
