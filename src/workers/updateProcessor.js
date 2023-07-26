const warp = require('../warp');
const { LoggerFactory } = require('warp-contracts');
const { storeAndPublish, checkStateSize } = require('./common');
const { config } = require('../config');
const { KnownErrors } = require('warp-contracts');
const { publishToRedis } = require('../workers/publish');
const stableHeight = require("../stableHeight");

LoggerFactory.INST.logLevel('debug');
LoggerFactory.INST.logLevel('debug', 'interactionsProcessor');
// LoggerFactory.INST.logLevel('none', 'DefaultStateEvaluator');
const logger = LoggerFactory.INST.create('interactionsProcessor');
LoggerFactory.INST.logLevel('debug', 'EvaluationProgressPlugin');

module.exports = async (job) => {
  const { contractTxId, isTest, interaction } = job.data;

  // workaround for https://github.com/taskforcesh/bullmq/issues/1557
  try {
    logger.info('Update Processor', contractTxId);

    const contract = warp.contract(contractTxId).setEvaluationOptions(config.evaluationOptions);

    let lastSortKey = null;
    let result = null;

    const lastCachedKey = (await warp.stateEvaluator.latestAvailableState(contractTxId))?.sortKey;
    if (interaction.lastSortKey && lastCachedKey?.localeCompare(interaction.lastSortKey) === 0) {
      logger.debug('Safe to use latest interaction');
      lastSortKey = interaction.lastSortKey;
      result = await contract.readStateFor(lastSortKey, [interaction]);
    }

    if (result == null) {
      logger.debug('Not safe to use latest interaction, reading via Warp GW.');
      const height = await stableHeight();
      result = await contract.readState(height);
    }

    logger.info(`Evaluated ${contractTxId} @ ${result.sortKey}`, contract.lastReadStateStats());
    checkStateSize(result.cachedValue.state);
    storeAndPublish(logger, isTest, contractTxId, result).finally(() => {});

    if (!isTest) {
      const tags = interaction.tags;
      if (tags) {
        const interactWritesTags = tags.filter((t) => t.name == 'Interact-Write');
        if (interactWritesTags) {
          const interactWritesContracts = interactWritesTags.map((t) => t.value);
          for (const contract1 of interactWritesContracts) {
            const interactWriteContractResult = await warp.stateEvaluator.latestAvailableState(contract1);

            logger.debug("Publishing to agg node for IW contract", contract1);
            await publishToRedis(logger, contract1, {
              contractTxId: contract1,
              sortKey: interactWriteContractResult.sortKey,
              state: interactWriteContractResult.cachedValue.state,
              node: null,
              signature: null,
              manifest: null,
              stateHash: null
            });
          }
        }
      }
      /*if (interaction.originalContractTxId) {
        const ogContractTxId = interaction.originalContractTxId;
        const ogContractResult = await warp.stateEvaluator.latestAvailableState(ogContractTxId);
        logger.debug("Publishing to agg node for original contract", ogContractTxId);
        await publishToRedis(logger, ogContractTxId, {
          contractTxId: ogContractTxId,
          sortKey: ogContractResult.sortKey,
          state: ogContractResult.cachedValue.state,
          node: null,
          signature: null,
          manifest: null,
          stateHash: null
        });
      }*/
    }

    return { lastSortKey };
  } catch (e) {
    logger.error('Exception in update processor', e);

    if (e.name == KnownErrors.NetworkCommunicationError) {
      return;
    }
    throw new Error(`${contractTxId}|${interaction.id}|${e}`);
  }
};
