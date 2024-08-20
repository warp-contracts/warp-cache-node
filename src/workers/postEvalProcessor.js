const { LoggerFactory } = require('warp-contracts');
const { publish } = require('./common');
const { signState } = require('../signature');
const { warp } = require('../warp');
const { upsertDeployment } = require('../db/aggDbUpdates');
const { config } = require('../config');
const { onNewState } = require('../routes/agg/onState');
const { onNewInteraction } = require('../routes/agg/onInteraction');

LoggerFactory.INST.logLevel('info', 'setStatePostProcessor');
LoggerFactory.INST.logLevel('info', 'PgContractCache');
const logger = LoggerFactory.INST.create('setStatePostProcessor');

const isTestInstance = config.env === 'test';

module.exports = async (job) => {
  const { contractTxId, tags, result, interactions, requiresPublish } = job.data;
  try {
    logger.info('PostEval Processor', contractTxId);
    const contractState = result.cachedValue.state;

    if (tags && tags.length > 0) {
      await onContractDeployment(contractTxId, tags);
    }
    const signed = config.signState
      ? await sign(contractTxId, result.sortKey, contractState)
      : { stateHash: '', sig: '' };
    await onNewState(job.data);

    if (interactions) {
      for (const interaction of interactions) {
        await onNewInteraction(contractTxId, interaction);
      }
    }

    if (requiresPublish && !isTestInstance) {
      await publish(logger, contractTxId, contractState, result.sortKey, signed.stateHash, signed.sig);

      if (interactions && interactions.length > 0) {
        await publishInternalWritesContracts(interactions);
      }
    }
  } catch (e) {
    logger.error(`PostEval Processor, contract ${contractTxId} failed with err`, e);
    throw new Error(e);
  }
};

async function publishInternalWritesContracts(interactions) {
  const iwTagsValues = new Set();
  interactions.forEach((i) => {
    const tags = i.tags;
    if (tags) {
      tags.forEach((t) => {
        if (t.name === 'Interact-Write') {
          iwTagsValues.add(t.value);
        }
      });
    }
  });

  for (const iwContract of iwTagsValues) {
    const interactWriteContractResult = await warp.stateEvaluator.latestAvailableState(iwContract);
    if (config.signState) {
      await sign(iwContract, interactWriteContractResult.sortKey, interactWriteContractResult.cachedValue.state);
    }
    await onNewState({ contractTxId: iwContract, result: interactWriteContractResult });
  }
}

async function sign(contractTxId, sortKey, state) {
  const signed = await signState(contractTxId, sortKey, state);

  await warp.stateEvaluator.getCache().setSignature({ key: contractTxId, sortKey }, signed.stateHash, signed.sig);

  return signed;
}

async function onContractDeployment(contractTxId, tags) {
  const indexesString = tags.find((tag) => tag.name === 'Indexed-By');

  if (indexesString) {
    const indexes = indexesString.value.split(';');

    if (indexes.length > 0) {
      await upsertDeployment(contractTxId, indexes);
    }
  }
}
