const { LoggerFactory } = require('warp-contracts');
const { upsertBalances, balancesLastSortKey, updateWalletAddress } = require('../../db/aggDbUpdates');
const { getWarpyLastUserAddress } = require('../../db/nodeDb');

LoggerFactory.INST.logLevel('none');
LoggerFactory.INST.logLevel('debug', 'listener');

const logger = LoggerFactory.INST.create('listener');

module.exports = {
  onNewState: async function (data) {
    const { contractTxId, result } = data;
    const contractState = result.cachedValue.state;
    const lastSK = await balancesLastSortKey(contractTxId);

    if (result.sortKey.localeCompare(lastSK)) {
      console.time('upsertBalances');
      await upsertBalances(contractTxId, result.sortKey, contractState);
      console.timeEnd('upsertBalances');
    } else {
      logger.warn('Received state with older or equal sort key', {
        contract: contractTxId,
        received: result.sortKey,
        latest: lastSK
      });
    }
  }
};
