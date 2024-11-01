const logger = require('./../logger')('aggUpdates');
const { drePool } = require('./nodeDb');

const TAGS_LIMIT = 5;
const chunkSize = 10000;
const ticker = 'REDSTONE_TICKER';
const tokenName = 'RedStone PST';

module.exports = {
  upsertBalances: async function (event) {
    const { contractTxId, sortKey, data } = event;

    const walletAddresses = data.users.map((u) => u.address);
    console.log(`Wallet addresses to update balances: ${walletAddresses.length}`);

    for (let i = 0; i < walletAddresses.length; i += chunkSize) {
      const chunk = walletAddresses.slice(i, i + chunkSize);
      const values = [];
      const placeholders = [];
      let counter = 1;

      for (const walletAddress of chunk) {
        const balance = data.users.find((u) => u.address == walletAddress)?.balance?.toString() || null;
        values.push(walletAddress.trim(), contractTxId.trim(), ticker, sortKey, tokenName, balance);
        placeholders.push(`($${counter++}, $${counter++}, $${counter++}, $${counter++}, $${counter++}, $${counter++})`);
      }

      if (placeholders.length > 0) {
        await drePool.query(
          `
            INSERT INTO dre.balances(wallet_address, contract_tx_id, token_ticker, sort_key, token_name, balance)
            VALUES ${placeholders.join(',')}
            ON CONFLICT (wallet_address, contract_tx_id) DO UPDATE SET sort_key = excluded.sort_key,
                                                                       balance = excluded.balance;
          `,
          values
        );
      }
    }
  },

  changeWallet: async function (event) {
    const { data } = event;

    console.log(`Changing wallet address: ${data.oldAddress} to: ${data.address}.`);
    await drePool.query(`update dre.balances set wallet_address = $1 where wallet_address ilike $2;`, [
      data.address.trim(),
      data.oldAddress.trim()
    ]);
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
  },

  updateWalletAddress: async function (oldAddress, newAddress) {
    await drePool.query(
      `
      UPDATE dre.balances SET wallet_address = $1 WHERE wallet_address = $2;
    `,
      [newAddress, oldAddress]
    );
  }
};

function diffBalances(obj1, obj2) {
  const diffed = {};
  const keys2 = Object.keys(obj2);
  const keys1 = Object.keys(obj1);

  for (const key of keys2) {
    if (obj1[key] !== obj2[key]) {
      diffed[key] = obj2[key];
    }
    const index = keys1.indexOf(key);
    if (index !== -1) {
      keys1.splice(index, 1);
    }
  }

  return { diffed, removed: keys1 };
}
