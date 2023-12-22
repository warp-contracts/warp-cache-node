const { warp } = require('../warp');
const { LoggerFactory, genesisSortKey } = require('warp-contracts');
const { checkStateSize } = require('./common');
const { config } = require('../config');
const { postEvalQueue, registerQueue } = require('../bullQueue');

// LoggerFactory.INST.logLevel('none', 'DefaultStateEvaluator');
LoggerFactory.INST.logLevel('info', 'pollUpdateProcessor');
LoggerFactory.INST.logLevel('info', 'EvaluationProgressPlugin');
LoggerFactory.INST.logLevel('error', 'WarpGatewayInteractionsLoader');
LoggerFactory.INST.logLevel('error', 'ContractHandler');
LoggerFactory.INST.logLevel('error', 'HandlerBasedContract');
LoggerFactory.INST.logLevel('error', 'DefaultStateEvaluator');
LoggerFactory.INST.logLevel('error', 'SqliteContractCache');
LoggerFactory.INST.logLevel('error', 'WarpGatewayContractDefinitionLoader');
LoggerFactory.INST.logLevel('error', 'SqliteContractCache');
const logger = LoggerFactory.INST.create('pollUpdateProcessor');

class CacheConsistencyError extends Error {
  constructor(message) {
    super(message);
    this.name = 'CacheConsistencyError';
  }
}

module.exports = async (job) => {
  const { contractTxId, isTest, partition } = job.data;

  logger.info('Poll update Processor', contractTxId);
  if (!partition || partition.length == 0) {
    throw new Error('Wrong partition - no interactions', contractTxId);
  }

  const firstInteraction = partition[0];
  const contract = warp.contract(contractTxId).setEvaluationOptions(config.evaluationOptions);
  const lastCachedKey = (await warp.stateEvaluator.latestAvailableState(contractTxId))?.sortKey;
  logger.info('Sort keys', {
    lastCachedKey,
    firstInteractionLastSortKey: firstInteraction.lastSortKey,
    firstInteractionSortKey: firstInteraction.sortKey
  });

  // state not cached (or cached at genesisSortKey - i.e. initial contract state),
  // but first interaction in partition has lastSortKey set (i.e. it is NOT the very first interaction with a contract)
  if ((!lastCachedKey || lastCachedKey == genesisSortKey) && firstInteraction.lastSortKey != null) {
    throw new CacheConsistencyError(
      `Inconsistent state for ${contractTxId} - first interaction ${firstInteraction.lastSortKey} in partition has lastSortKey != null - while there is no state cached.`
    );
  }

  // first interaction for contract (i.e. first interaction in partition has lastSortKey = null), but we have already state cached at sortKey > genesisSortKey
  /*
  if (lastCachedKey && lastCachedKey != genesisSortKey && firstInteraction.lastSortKey == null) {
    throw new CacheConsistencyError(`Inconsistent state for ${contractTxId} - first interaction in partition has lastSortKey = null - while there is already state cached at ${lastCachedKey}`);
  }

  // state cached at a sortKey > genesisSortKey and first interaction in partition has lastSortKey set - but lastSortKey is different from the last cached sort key
  if (lastCachedKey && lastCachedKey != genesisSortKey && firstInteraction.lastSortKey != lastCachedKey) {
    throw new CacheConsistencyError(`Inconsistent state for ${contractTxId} - state cached at a different sortKey then first interaction lastSortKey`);
  }*/
  let filteredPartition = partition;
  if (lastCachedKey && firstInteraction.sortKey.localeCompare(lastCachedKey) <= 0) {
    logger.info('First sort key lower than last cached key, removing interactions');
    filteredPartition = partition.filter((i) => i.sortKey.localeCompare(lastCachedKey) > 0);
    logger.info('Partition size after filtering', filteredPartition.length);
  }

  if (!lastCachedKey) {
    await contract.readState(genesisSortKey);

    // ppe: ponapsichuj to?
    await registerQueue.add(
      'initContract',
      {
        contractTxId,
        requiresPublish: false
      },
      { jobId: contractTxId }
    );
  }

  if (filteredPartition.length > 0) {
    const interactions = filteredPartition.map((i) => i.interaction);
    const result = await contract.readStateFor(lastCachedKey || genesisSortKey, interactions);

    logger.info(`Evaluated ${contractTxId} @ ${result.sortKey}`, contract.lastReadStateStats());

    checkStateSize(result.cachedValue.state);
    if (!isTest) {
      await postEvalQueue.add('sign', { contractTxId, result, interactions, requiresPublish: true }, { priority: 1 });
    }
  } else {
    logger.info('Skipping empty partition');
  }
};
