const { warp } = require('../warp');
const { LoggerFactory } = require('warp-contracts');
const { checkStateSize } = require('./common');
const { config } = require('../config');
const { postEvalQueue } = require('../bullQueue');
const { insertContractEvent } = require('../db/nodeDb');

LoggerFactory.INST.logLevel('info', 'updateProcessor');
LoggerFactory.INST.logLevel('info', 'EvaluationProgressPlugin');
LoggerFactory.INST.logLevel('debug', 'WarpGatewayInteractionsLoader');
LoggerFactory.INST.logLevel('debug', 'ContractHandler');
LoggerFactory.INST.logLevel('info', 'HandlerBasedContract');
LoggerFactory.INST.logLevel('info', 'DefaultStateEvaluator');
LoggerFactory.INST.logLevel('info', 'SqliteContractCache');
LoggerFactory.INST.logLevel('info', 'WarpGatewayContractDefinitionLoader');
LoggerFactory.INST.logLevel('debug', 'p5OI99-BaY4QbZts266T7EDwofZqs-wVuYJmMCS0SUU');
LoggerFactory.INST.logLevel('debug', 'HandlerExecutorFactory');

const logger = LoggerFactory.INST.create('updateProcessor');

let cachedState = null;

module.exports = async (job) => {
  try {
    let { contractTxId, isTest, interaction } = job.data;

    logger.info('Update Processor', contractTxId);
    if (typeof interaction === 'string' || interaction instanceof String) {
      interaction = JSON.parse(interaction);
    }

    const contract = warp.contract(contractTxId).setEvaluationOptions(config.evaluationOptions);
    const lastCachedKey =
      cachedState?.sortKey || (await warp.stateEvaluator.latestAvailableState(contractTxId))?.sortKey;
    if (lastCachedKey >= interaction.sortKey) {
      logger.warn(
        `Interaction ${interaction.id}:${interaction.sortKey} for contract ${contractTxId} already evaluated`
      );
      return;
    }
    logger.debug('SortKeys:', {
      lastCachedKey,
      sortKey: interaction.sortKey,
      lastSortKey: interaction.lastSortKey
    });

    if (config.availableFunctions.contractEvents) {
      logger.info('Adding interactionCompleted listener');
      warp.eventTarget.addEventListener('interactionCompleted', interactionCompleteHandler);
    }

    let result;

    // note: this check will work properly with at most 1 update processor per given contract...
    if (lastCachedKey && lastCachedKey === interaction.lastSortKey) {
      result = await contract.readStateFor(lastCachedKey, [interaction], undefined, cachedState);
    } else {
      result = await contract.readState(interaction.sortKey);
    }

    cachedState = result;

    logger.info(`Evaluated ${contractTxId} @ ${result.sortKey}`, contract.lastReadStateStats());

    checkStateSize(result.cachedValue.state);
    if (!isTest) {
      await postEvalQueue.add(
        'sign',
        { contractTxId, result, interactions: [interaction], requiresPublish: true },
        { priority: 1 }
      );
    }
  } catch (e) {
    logger.error(e);
    throw e;
  } finally {
    if (config.availableFunctions.contractEvents) {
      warp.eventTarget.removeEventListener('interactionCompleted', interactionCompleteHandler);
    }
  }
};

async function interactionCompleteHandler(event) {
  const eventData = event.detail;
  logger.debug('New contract event', eventData);
  await insertContractEvent(eventData);
}
