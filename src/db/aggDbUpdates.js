const logger = require('./../logger')('aggUpdates');
const { drePool } = require('./nodeDb');

const TAGS_LIMIT = 5;

module.exports = {
  upsertBalances: async function (contractTxId, sortKey, state) {
    const balances = state.balances;
    const ticker = state.ticker; // pst standard
    const symbol = state.symbol; // warp nft/erc standard
    if (!balances || (!ticker && !symbol)) {
      logger.error(`Contract ${contractTxId} is not compatible with token standard`);
      return;
    }
    const token_ticker = ticker ? ticker.trim() : symbol.trim();
    const name = state.name;

    const walletAddresses = Object.keys(balances);
    for (const walletAddress of walletAddresses) {
      const balance = balances[walletAddress] ? balances[walletAddress].toString() : null;
      await drePool.query(
        `
            INSERT INTO dre.balances(wallet_address, contract_tx_id, token_ticker, sort_key, token_name, balance)
            VALUES ($1, $2, $3, $4, $5, $6)
            ON CONFLICT (wallet_address, contract_tx_id) DO UPDATE SET sort_key = excluded.sort_key,
                                                                       balance = excluded.balance`,
        [walletAddress.trim(), contractTxId.trim(), token_ticker, sortKey, name?.trim(), balance]
      );
    }
  },

  upsertDeployment: async function (contractTxId, indexes) {
    logger.info('Upserting deployment', contractTxId);

    const effectiveIndexesCount = Math.min(TAGS_LIMIT, indexes.length);

    const queryArgs = Array(TAGS_LIMIT).fill(null);
    for (let i = 0; i < effectiveIndexesCount; i++) {
      queryArgs[i] = indexes[i];
    }
    queryArgs.unshift(contractTxId);

    await drePool.query(
      `
          INSERT INTO dre.deployments(contract_tx_id, tag_index_0, tag_index_1, tag_index_2, tag_index_3, tag_index_4)
          VALUES ($1, $2, $3, $4, $5, $6);`,
      queryArgs
    );
  },

  balancesLastSortKey: async function (contractTxId) {
    const result = await drePool.query(
      `SELECT max(sort_key) as maxSortKey
       FROM dre.balances
       WHERE contract_tx_id = $1`,
      [contractTxId]
    );

    if (!result || !result.rows || result.rows.length < 1) {
      return null;
    }

    return result.rows[0].maxSortKey;
  },

  upsertInteraction: async function (contractTxId, id, ownerAddress, blockHeight, indexes) {
    logger.info('Upserting interactions', contractTxId);

    const effectiveIndexesCount = Math.min(TAGS_LIMIT, indexes.length);
    const queryArgs = Array(TAGS_LIMIT).fill(null);
    for (let i = 0; i < effectiveIndexesCount; i++) {
      queryArgs[i] = indexes[i];
    }
    queryArgs.unshift(blockHeight);
    queryArgs.unshift(ownerAddress);
    queryArgs.unshift(contractTxId);
    queryArgs.unshift(id);

    await drePool.query(
      `
          INSERT INTO dre.interactions(id, contract_tx_id, owner_address, block_height, tag_index_0, tag_index_1, tag_index_2, tag_index_3, tag_index_4)
          VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);`,
      queryArgs
    );
  }
};
