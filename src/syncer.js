const Redis = require('ioredis');
const { config, logConfig } = require('./config');
const {
  getFailures,
  getLastSyncTimestamp,
  createNodeDbTables,
  drePool,
  insertFailure,
  doBlacklist,
  dreReplicaPool
} = require('./db/nodeDb');

const logger = require('./logger')('syncer');
const exitHook = require('async-exit-hook');
const { warp, pgClient } = require('./warp');
const pollGateway = require('./workers/pollGateway');
const { createAggDbTables } = require('./db/aggDbSetup');
const {
  queuesCleanUp,
  initQueue,
  postEvalQueue,
  registerQueue,
  maintenanceQueue,
  updateQueue
} = require('./bullQueue');

const isTestInstance = config.env === 'test';
const subscriptionMode = config.updateMode === 'subscription';

async function runSyncer() {
  logger.info(`🚀🚀🚀 Starting syncer node in ${config.updateMode} mode.`);
  await logConfig();

  await createNodeDbTables();
  await createAggDbTables();

  await pgClient.open();
  await initQueue(postEvalQueue);
  await initQueue(registerQueue, onFailedJob);
  await initQueue(maintenanceQueue);
  if (subscriptionMode) {
    await initQueue(updateQueue, onFailedJob);
  }

  if (!subscriptionMode) {
    const theVeryFirstTimestamp = config.firstInteractionTimestamp;
    if (!theVeryFirstTimestamp) {
      logger.error('FIRST_INTERACTION_TIMESTAMP .env param not set');
      process.exit(0);
    }
    const lastSyncTimestamp = await getLastSyncTimestamp();
    logger.info('Last sync timestamp result', lastSyncTimestamp);
    const startTimestamp = lastSyncTimestamp ? lastSyncTimestamp : theVeryFirstTimestamp;

    await pollGateway(
      config.evaluationOptions.whitelistSources,
      config.evaluationOptions.blacklistedContracts,
      startTimestamp,
      windowsMs(),
      false,
      blacklist,
      isBlacklisted
    );
  }
  scheduleMaintenance();

  await subscribeToGatewayNotifications();
}

function windowsMs() {
  const windows = [];
  if (config.syncWindowSeconds.length < 1) {
    throw new Error('Provide at least one sync window (in seconds)');
  }
  for (let w of config.syncWindowSeconds) {
    windows.push(parseInt(w) * 1000);
  }
  return windows;
}

runSyncer().catch((e) => {
  logger.error(e);
});

async function subscribeToGatewayNotifications() {
  const onError = (err) => logger.error('Failed to subscribe:', err);
  const onMessage = async (data) => await processGatewayMessage(data);

  const subscriber = new Redis(config.gwPubSubConfig);
  await subscriber.connect();
  logger.info('Connected to Warp Gateway notifications', subscriber.status);

  subscriber.subscribe('contracts', (err, count) => {
    if (err) {
      onError(err.message);
    } else {
      logger.info(`Subscribed successfully! This client is currently subscribed to ${count} channels.`);
    }
  });
  subscriber.on('message', async (channel, message) => {
    try {
      const msgObj = JSON.parse(message);
      if (((msgObj.interaction && subscriptionMode) || msgObj.initialState) && isWhitelistedSource(msgObj.srcTxId)) {
        await onMessage(msgObj);
      }
    } catch (e) {
      logger.error(e);
      logger.error(message);
    }
  });
  process.on('exit', () => subscriber.disconnect());
}

async function processGatewayMessage(msgObj) {
  logger.info(`Received message for '${msgObj.contractTxId}'`);
  logger.debug('message object', msgObj);

  let validationMessage = null;
  if (!isTxIdValid(msgObj.contractTxId)) {
    validationMessage = `Invalid contract tx id format: ${msgObj.contractTxId}`;
  }
  if (!isTxIdValid(msgObj.srcTxId)) {
    validationMessage = `Invalid src tx id format: ${msgObj.srcTxId}`;
  }

  if ((!msgObj.initialState && !msgObj.interaction) || (msgObj.initialState && msgObj.interaction)) {
    validationMessage = 'Invalid message format';
  }

  if (msgObj.test && !isTestInstance) {
    validationMessage = 'Skipping test instance message';
  }

  if (!msgObj.test && isTestInstance) {
    validationMessage = 'Skipping non-test instance message';
  }

  if (validationMessage == null) {
    const contractFailures = await getFailures(null, msgObj.contractTxId);
    if (Number.isInteger(contractFailures) && contractFailures > config.workersConfig.maxFailures - 1) {
      validationMessage = `Contract blacklisted: ${msgObj.contractTxId}`;
    }
  }

  if (validationMessage !== null) {
    logger.warn('Message rejected:', validationMessage);
    return;
  }

  const contractTxId = msgObj.contractTxId;
  const isRegistered = await warp.stateEvaluator.hasContractCached(contractTxId);

  const baseMessage = {
    contractTxId,
    appSyncKey: config.appSync.key,
    test: isTestInstance,
    requiresPublish: true
  };
  if (msgObj.initialState) {
    if (isRegistered) {
      validationMessage = 'Contract already registered';
      logger.warn(validationMessage);
      return;
    }
    const jobId = msgObj.contractTxId;
    await registerQueue.add(
      'initContract',
      {
        ...baseMessage,
        tags: msgObj.tags
      },
      { jobId }
    );
    logger.info('Published to register queue', jobId);
  } else if (subscriptionMode && msgObj.interaction) {
    await updateQueue.add('evaluateInteraction', {
      ...baseMessage,
      interaction: msgObj.interaction
    });
    logger.info('Published to update queue');
  }
}

function scheduleMaintenance() {
  if (config.workersConfig.maintenance > 0 && config.workersConfig.maintenanceWindow > 0) {
    (function workerLoop() {
      setTimeout(async function () {
        await maintenanceQueue.add('maintenance');
        workerLoop();
      }, config.workersConfig.maintenanceWindow);
    })();
  }
}

function isTxIdValid(txId) {
  const validTxIdRegex = /[a-z0-9_-]{43}/i;
  return validTxIdRegex.test(txId);
}

// Graceful shutdown
async function cleanup(callback) {
  logger.info('Interrupted');
  await queuesCleanUp();
  await warp.close();
  await drePool.end();
  await dreReplicaPool.end();
  logger.info('Clean up finished');
  callback();
}

async function onFailedJob(contractTxId, jobId, failedReason) {
  await insertFailure({
    contract_tx_id: contractTxId,
    evaluation_options: config.evaluationOptions,
    sdk_config: config.warpSdkConfig,
    job_id: jobId,
    failure: failedReason
  });
  if (failedReason.includes('[MaxStateSizeError]')) {
    await blacklist(contractTxId, failedReason);
  }
}

async function blacklist(contractTxId, reason) {
  try {
    await doBlacklist(contractTxId, config.workersConfig.maxFailures, reason || '');
  } catch (e) {
    logger.error(`Error while blacklisting ${contractTxId}`, e);
  }
}

async function isBlacklisted(contractTxId) {
  const failures = await getFailures(null, contractTxId);
  return failures >= config.workersConfig.maxFailures;
}

function isWhitelistedSource(srcTxId) {
  const whitelistedSources = config.evaluationOptions.whitelistSources;
  return whitelistedSources.length == 0 || whitelistedSources.includes(srcTxId);
}

exitHook(cleanup);
