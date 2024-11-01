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

let interactionCompleteHandlerWrapper;
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
      logger.info('Adding interactionCompleted listener', interaction.id);
      interactionCompleteHandlerWrapper = (event) =>
        interactionCompleteHandler(event, isTest, interaction, contractTxId);
      warp.eventTarget.addEventListener('interactionCompleted', interactionCompleteHandlerWrapper);
    }

    let result;

    // note: this check will work properly with at most 1 update processor per given contract...
    logger.info('Cached state?: ', cachedState != null);
    if (lastCachedKey && lastCachedKey === interaction.lastSortKey) {
      logger.info('Using cached state!');
      result = await contract.readStateFor(lastCachedKey, [interaction], undefined, cachedState);
    } else {
      result = await contract.readState(interaction.sortKey);
    }

    cachedState = result;
    cachedState.cachedValue.errorMessages = {};
    cachedState.cachedValue.validity = {};

    logger.info(`Evaluated ${contractTxId} @ ${result.sortKey}`, contract.lastReadStateStats());

    checkStateSize(result.cachedValue.state);
  } catch (e) {
    logger.error(e);
    throw e;
  }
};

async function interactionCompleteHandler(event, isTest, interaction, contractTxId) {
  warp.eventTarget.removeEventListener('interactionCompleted', interactionCompleteHandlerWrapper);

  const eventData = event.detail;
  logger.debug('New contract event', eventData);
  const interactionExcluded = getExcludedInteraction(interaction);
  logger.info(`Should exclude from postEval: ${interactionExcluded}`);
  if (!interactionExcluded && !isTest) {
    await postEvalQueue.add(
      'sign',
      // result set to null as for Warpy state is neither signed nor published
      { contractTxId, result: null, event: eventData, interactions: [interaction], requiresPublish: true },
      { priority: 1 }
    );
  }
  await insertContractEvent(eventData);
}

function getExcludedInteraction(interaction) {
  const input = interaction?.tags?.find((t) => t.name == 'Input')?.value;
  if (!input) {
    logger.info(`Could not get input tag, ${JSON.stringify(interaction)}`);
    return false;
  }
  let parsedInput;
  try {
    parsedInput = JSON.parse(input);
  } catch (e) {
    logger.warn(`Could not parse input value. ${JSON.stringify(e)}. `);
    return false;
  }

  const interactionName = parsedInput?.function;
  if (interactionName == 'addPointsWithCap' && !parsedInput?.cap) {
    return true;
  }

  return false;
}
