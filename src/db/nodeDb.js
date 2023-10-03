const { signState } = require('../signature');
const { config } = require('../config');
const dreDbConfig = require('../../postgresConfigDreDb.js');
const { Pool } = require('pg');

const drePool = new Pool(dreDbConfig);

module.exports = {
  drePool,
  createNodeDbTables: async () => {
    await drePool.query(
      `
        --------------- errors
        CREATE TABLE IF NOT EXISTS errors (
            contract_tx_id text,
            evaluation_options jsonb,
            sdk_config jsonb,
            job_id text unique,
            failure text not null, 
            timestamp timestamp with time zone default now()
        );
        CREATE INDEX IF NOT EXISTS idx_errors_contract_tx_id ON errors(contract_tx_id);
        
        --------------- black_list
        CREATE TABLE IF NOT EXISTS black_list (
            contract_tx_id text unique,
            failures bigint
        );
        CREATE INDEX IF NOT EXISTS idx_black_list_contract_id ON black_list(contract_tx_id);
        
        --------------- view_state
        CREATE TABLE IF NOT EXISTS view_state
        (
            contract_tx_id text,
            sort_key       text,
            caller         text,
            signature      text,
            view_hash      text,
            input          jsonb,
            result         jsonb,
            UNIQUE (contract_tx_id, input, caller)
        );
        CREATE INDEX IF NOT EXISTS idx_view_state_contract_id ON view_state (contract_tx_id);

        --------------- sync_log
        CREATE TABLE IF NOT EXISTS sync_log (
            start_timestamp BIGINT,
            end_timestamp BIGINT,
            response_length BIGINT,
            response_hash text,
            response_first_sortkey text, 
            response_last_sortkey text,
            errors jsonb
        );
        CREATE INDEX IF NOT EXISTS idx_sync_log_end_timestamp ON sync_log(end_timestamp);
`
    );
  },

  insertFailure: async (data) => {
    await drePool.query(
      `
                INSERT INTO errors (contract_tx_id,evaluation_options,sdk_config,job_id,failure,timestamp)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT(job_id) DO NOTHING `,
      [data.contract_tx_id, data.evaluation_options, data.sdk_config, data.job_id, data.failure, data.timestamp]
    );
  },

  insertSyncLog: async (data) => {
    await drePool.query(
      `
                INSERT INTO sync_log (start_timestamp, end_timestamp, response_length, response_hash, response_first_sortkey, response_last_sortkey, errors)
                VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [
        data.start_timestamp,
        data.end_timestamp,
        data.response_length,
        data.response_hash,
        data.response_first_sortkey,
        data.response_last_sortkey,
        data.errors
      ]
    );
  },

  getLastSyncTimestamp: async () => {
    const result = await drePool.query('SELECT max(end_timestamp) as "lastTimestamp" from sync_log');
    if (result && result.rows && result.rows.length > 0) {
      return Number(result.rows[0].lastTimestamp);
    } else {
      return null;
    }
  },

  // TODO
  deleteStates: async (contractTxId) => {
    await drePool.query(`DELETE FROM states WHERE contract_tx_id = $1;`, [contractTxId]);
  },

  deleteBlacklist: async (contractTxId) => {
    await drePool.query(`DELETE FROM black_list WHERE contract_tx_id = $1;`, [contractTxId]);
  },

  doBlacklist: async (contractTxId, failures) => {
    await drePool.query(
      `INSERT INTO black_list VALUES ($1, $2) ON CONFLICT (contract_tx_id) DO UPDATE SET failures = EXCLUDED.failures `,
      [contractTxId, failures]
    );
  },

  getFailures: async (drePool, contractTxId) => {
    const result = drePool.query(`SELECT * FROM black_list WHERE contract_tx_id = $1`, [contractTxId]);
    if (result && result.rows && result.rows.length > 0) {
      return result.rows[0].failures;
    }

    return 0;
  },

  getAllBlacklisted: async () => {
    return (await drePool.query(`SELECT * FROM black_list`))?.rows;
  },

  getAllErrors: async () => {
    return (await drePool.query(`SELECT * FROM errors;`))?.rows;
  },

  getContractErrors: async (contractTxId) => {
    return (
      await drePool.query(`SELECT * FROM errors WHERE contract_tx_id = $1 ORDER BY timestamp DESC;`, [contractTxId])
    )?.rows;
  },

  deleteErrors: async (contractTxId) => {
    await drePool.query(`DELETE FROM errors WHERE contract_tx_id = ?;`, [contractTxId]);
  },

  getSyncLog: async (start, end) => {
    return await drePool.query(`SELECT * FROM sync_log WHERE start_timestamp = $1 AND end_timestamp = $2`, [
      start,
      end
    ]);
  },

  getCachedViewState: async (contractTxId, sortKey, input, caller) => {
    caller = caller || '';
    const result = await drePool.query(
      `SELECT * FROM view_state WHERE contract_tx_id = $1 AND sort_key = $2 AND input = $3 AND caller = $4;`,
      [contractTxId, sortKey, input, caller]
    );
    if (result && result.rows && result.rows.length > 0) {
      return result.rows[0];
    }
    return null;
  },

  insertViewStateIntoCache: async (contractTxId, sortKey, input, result, caller) => {
    caller = caller || '';
    const manifest = await config.nodeManifest;
    const { sig, stateHash } = await signState(contractTxId, sortKey, result, caller, manifest);

    const entry = {
      contract_tx_id: contractTxId,
      sort_key: sortKey,
      input: input,
      caller: caller,
      signature: sig,
      view_hash: stateHash,
      result: result
    };

    await drePool.query(
      `
                INSERT INTO view_state (contract_tx_id, sort_key, caller, signature, view_hash, input, result)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                ON CONFLICT(contract_tx_id, input, caller) DO UPDATE SET result = EXCLUDED.result`,
      [contractTxId, sortKey, caller, sig, stateHash, input, result]
    );

    return entry;
  },

  countAllContracts: async () => {
    const result = await drePool.query(`SELECT count(DISTINCT key) AS total FROM warp.sort_key_cache;`);
    if (result && result.rows && result.rows.length > 0) {
      return {
        total: Number(result.rows[0].total)
      };
    }
    return 0;
  },

  getAllContractsIds: async () => {
    const result = await drePool.query(
      `SELECT count(DISTINCT key) AS total, array_agg(DISTINCT key) AS ids FROM warp.sort_key_cache;`
    );
    if (result && result.rows && result.rows.length > 0) {
      return {
        total: Number(result.rows[0].total),
        ids: result.rows[0].ids
      };
    }
    return null;
  },

  countContractValidity: async (contractTxId, sortKey) => {
    const result = await drePool.query(
      `
          select count(*) as total
          from warp.validity
          where key = $1 and sort_key <= $2;`,
      [contractTxId, sortKey]
    );
    if (result && result.rows && result.rows.length > 0) {
      return Number(result.rows[0].total);
    }
    return 0;
  },

  getContractValidity: async (contractTxId, sortKey) => {
    const result = await drePool.query(
      `
        select json_object_agg(tx_id, valid) as v from warp.validity
        where key = $1
          and sort_key <= $2;`,
      [contractTxId, sortKey]
    );
    return result?.rows[0].v;
  },

  getContractErrorMessages: async (contractTxId, sortKey) => {
    const result = await drePool.query(
      `
          select json_object_agg(tx_id, error_message) as em 
          from warp.validity
          where key = $1
            and sort_key <= $2
            and error_message is not null;`,
      [contractTxId, sortKey]
    );
    return result?.rows[0].em;
  },

  hasContract: async (contractTxId) => {
    const result = await drePool.query(`SELECT count(*) > 0 AS has from warp.sort_key_cache WHERE key = $1;`, [
      contractTxId
    ]);
    if (result && result.rows && result.rows.length > 0) {
      return result.rows[0].has;
    }
    return false;
  }
};
