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
        SET search_path TO 'dre';
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
            failures bigint,
            reason text
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
        
        --------------- contract_event
        CREATE TABLE IF NOT EXISTS contract_event (
            contract_tx_id text,
            sort_key text,
            tx_id text,
            caller text,
            input jsonb,
            block_timestamp bigint,
            block_height bigint,
            data jsonb,
            UNIQUE (contract_tx_id, sort_key)
        );
        CREATE INDEX IF NOT EXISTS idx_contract_event_contract_tx_id ON contract_event(contract_tx_id);
        CREATE INDEX IF NOT EXISTS idx_contract_event_sort_key ON contract_event(sort_key);
        CREATE INDEX IF NOT EXISTS idx_contract_event_tx_id ON contract_event(tx_id);
        CREATE INDEX IF NOT EXISTS idx_contract_event_caller ON contract_event(caller);
        CREATE INDEX IF NOT EXISTS idx_contract_event_block_timestamp ON contract_event(block_timestamp);
        CREATE INDEX IF NOT EXISTS idx_contract_event_block_height ON contract_event(block_height);
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

  queryLastState: async (contractTxId, query) => {
    const output = await drePool.query(
      `SELECT sort_key, jsonb_path_query(value, $2) as result
       FROM warp.sort_key_cache
       WHERE key = $1 ORDER BY sort_key DESC LIMIT 1;`,
      [contractTxId, query]
    );
    return {
      sortKey: output?.rows[0]?.sort_key,
      result: output?.rows[0]?.result
    };
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

  /**
   * Blacklists contract and avoid further interactions' evaluations.
   * @param {string} contractTxId
   * @param {number} failures
   * @param {object} reason
   */
  doBlacklist: async (contractTxId, failures, reason) => {
    await drePool.query(
      `INSERT INTO black_list(contract_tx_id, failures, reason) VALUES ($1, $2, $3) ON CONFLICT (contract_tx_id) DO UPDATE SET failures = EXCLUDED.failures, reason = EXCLUDED.reason;`,
      [contractTxId, failures, reason]
    );
  },

  /**
   * This function is used by warp-contracts-plugin-blacklist plugin.
   * Any changes here must be coordinated with the plugin.
   * @param {Pool | null} customPool If customPool is not provided, a default drePool will be used.
   * @param {string} contractTxId
   * @return {Promise<number>} Number of failures.
   */
  getFailures: async (customPool, contractTxId) => {
    const result = await (customPool || drePool).query(`SELECT failures FROM black_list WHERE contract_tx_id = $1;`, [
      contractTxId
    ]);
    if (result && result.rows && result.rows.length > 0) {
      return parseInt(result.rows[0].failures);
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

  getContractValidityTotalCount: async (contractTxId, sortKey) => {
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

  /**
   * @return {Promise<{count: (number), validity: (Object.<string, boolean>)}>}
   */
  getContractValidity: async (contractTxId, sortKey, parsedLimit, offset) => {
    const result = await drePool.query(
      `
          WITH validity_page AS (
              SELECT tx_id, valid from warp.validity
              where key = $1
                and sort_key <= $2
              ORDER BY sort_key DESC, id DESC
              LIMIT $3 OFFSET $4
          )
          select json_object_agg(tx_id, valid) as v, count(*) as count from validity_page;`,
      [contractTxId, sortKey, parsedLimit, offset]
    );
    return {
      validity: result?.rows[0].v || {},
      count: Number(result?.rows[0].count || 0)
    };
  },

  /**
   * @return {Promise<{count: (number), errorMessages: (Object.<string, string>)}>}
   */
  getContractErrorMessages: async (contractTxId, sortKey, parsedLimit, offset) => {
    const result = await drePool.query(
      `
          WITH validity_page AS (
              SELECT tx_id, valid, error_message from warp.validity
              where key = $1
                and sort_key <= $2
              ORDER BY sort_key DESC, id DESC
              LIMIT $3 OFFSET $4
          )
          select json_object_agg(tx_id, error_message) as em, count(*) as count 
          from validity_page 
          where error_message is not null;`,
      [contractTxId, sortKey, parsedLimit, offset]
    );
    return {
      errorMessages: result?.rows[0].em || {},
      count: Number(result?.rows[0].count || 0)
    };
  },

  getSignatures: async (contractTxId, sortKey) => {
    const result = await drePool.query(
      `select state_hash as "stateHash", signature as sig from warp.sort_key_cache where key = $1 and sort_key = $2;`,
      [contractTxId, sortKey]
    );
    return result?.rows[0];
  },

  hasContract: async (contractTxId) => {
    const result = await drePool.query(`SELECT count(*) > 0 AS has from warp.sort_key_cache WHERE key = $1;`, [
      contractTxId
    ]);
    if (result && result.rows && result.rows.length > 0) {
      return result.rows[0].has;
    }
    return false;
  },

  insertContractEvent: async (event) => {
    const { contractTxId, caller, transactionId, sortKey, input, blockHeight, blockTimestamp, data } = event;

    // manual stringify due to issues w node-postgres
    const inputStringified = JSON.stringify(input);
    const dataStringified = JSON.stringify(data);

    await drePool.query(
      `
                INSERT INTO contract_event (contract_tx_id, sort_key, tx_id, caller, input, block_timestamp, block_height, data)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT(contract_tx_id, sort_key) DO UPDATE SET data = EXCLUDED.data`,
      [contractTxId, sortKey, transactionId, caller, inputStringified, blockTimestamp, blockHeight, dataStringified]
    );
  },

  getUserLastRewards: async (contractId, userId, limit) => {
    const result = await drePool.query(
      `
        SELECT sort_key, block_timestamp, tx_id, data ->> 'points' AS points
        FROM dre.contract_event
        WHERE contract_tx_id = $1
        AND data ->> 'userId' = $2
        UNION ALL
        SELECT dre.sort_key, dre.block_timestamp, tx_id, users ->> 'points' AS points
        FROM dre.contract_event dre
        CROSS JOIN LATERAL jsonb_array_elements(data -> 'users') AS users
        WHERE dre.contract_tx_id = $3
        AND users ->> 'userId' = $4        
        ORDER BY sort_key DESC 
        LIMIT $5;
      `,
      [contractId, userId, contractId, userId, limit]
    );
    return result.rows;
  },

  getWarpySeasonRanking: async (limit, address, contractId, from) => {
    const result = await drePool.query(
      `
      WITH users_points AS (
        SELECT users ->> 'userId' AS user_id, 
                users::jsonb ->> 'points' AS points 
                FROM dre.contract_event, 
                jsonb_array_elements(data -> 'users') AS users 
        WHERE contract_tx_id = $1 
        AND block_timestamp >= $2
        UNION ALL
        SELECT data::jsonb ->> 'userId' AS user_id, 
                data::jsonb ->> 'points' AS points 
                FROM dre.contract_event 
        WHERE contract_tx_id = $3
        AND block_timestamp >= $4),
      balance_aggregated AS (
        SELECT user_id, 
                SUM(points::int) AS balance 
                FROM users_points 
                GROUP BY user_id),
      last_state AS (
        SELECT value
        FROM warp.sort_key_cache
        ORDER BY sort_key DESC
        LIMIT 1),
      last_users AS (
        SELECT TRIM(users.value::text, '"') as value, users.key
        FROM last_state, jsonb_each(last_state.value -> 'users') users),
      ranked AS (
        SELECT row_number() OVER (ORDER BY ba.balance::int desc nulls last) AS rn, 
        ba.user_id AS user_id, 
        lu.value AS wallet_address, 
        balance 
        FROM balance_aggregated ba LEFT JOIN last_users lu ON ba.user_id = lu.key)
      ${
        address
          ? `(SELECT r.rn, 
      r.user_id, 
      r.wallet_address, 
      r.balance
      FROM ranked r
      WHERE wallet_address = '${address}')
      UNION ALL`
          : ''
      }
      (SELECT r.rn, 
      r.user_id, 
      r.wallet_address, 
      r.balance 
      FROM ranked r LIMIT $5);
      `,
      [contractId, from, contractId, from, limit]
    );

    return result.rows;
  },

  getWarpyUserBalance: async (userId) => {
    const result = await drePool.query(
      `
        WITH max_state AS (
          SELECT value FROM warp.sort_key_cache ORDER BY sort_key DESC LIMIT 1
        ),
        wallet_balance AS (
          SELECT value -> 'users' -> $1 ->> 0  as wallet_address
          from max_state
        )
        SELECT wallet_address, value::jsonb -> 'balances' -> wallet_address AS balance
        FROM max_state, wallet_balance;
      `,
      [userId]
    );

    return result.rows;
  },

  getWarpyUserCounter: async (userId) => {
    const result = await drePool.query(
      `
        WITH max_state AS (
        SELECT value FROM warp.sort_key_cache ORDER BY sort_key DESC LIMIT 1
        )
        SELECT value -> 'counter' -> $1  AS counter
        FROM max_state;
      `,
      [userId]
    );

    return result.rows;
  },

  getWarpyUserId: async (address) => {
    const result = await drePool.query(
      `
        WITH w1 AS (
          SELECT value -> 'users' AS users
          FROM warp.sort_key_cache
          WHERE sort_key = (SELECT MAX(sort_key) FROM warp.sort_key_cache)
        )
        SELECT key FROM w1, jsonb_each(w1.users)
        WHERE value ->> 0 ILIKE $1;
      `,
      [address]
    );

    return result.rows;
  },

  getWarpyUserIds: async (addresses) => {
    const result = await drePool.query(
      `
          WITH users as (
              SELECT users.key as dicord_id, users.value ->> 0 as address
              FROM warp.sort_key_cache, jsonb_each(value -> 'users') as users
              WHERE sort_key = (SELECT MAX(sort_key) FROM warp.sort_key_cache)
          )
          select json_object_agg(address, dicord_id) as wallet_to_id 
          from users
          where address = ANY($1::text[]);
      `,
      [addresses]
    );

    return result?.rows[0];
  },

  getWarpyUserIdsFixed: async (addresses) => {

    const lowercasedAddresses = addresses.map(a => a ? a.toLowerCase() : a);

    const result = await drePool.query(
      `
          WITH users as ( 
              SELECT users.key as dicord_id, lower(users.value ->> 0) as address
              FROM warp.sort_key_cache, jsonb_each(value -> 'users') as users
              WHERE sort_key = (SELECT MAX(sort_key) FROM warp.sort_key_cache)
          )
          select json_object_agg(address, dicord_id) as wallet_to_id 
          from users
          where address = ANY($1::text[]);
      `,
      [lowercasedAddresses]
    );

    return result?.rows[0];
  },

  getWarpyUserRanking: async (limit, address, contractId) => {
    const result = await drePool.query(
      `
        WITH ranked AS (
          SELECT row_number() OVER (ORDER BY balance::int DESC NULLS LAST ) AS rn, wallet_address, balance
          FROM dre.balances
          WHERE contract_tx_id = $1),
        last_state AS (
          SELECT value
          FROM warp.sort_key_cache
          ORDER BY sort_key DESC
          LIMIT 1),
        last_users AS (
          SELECT TRIM(users.value::text, '"') as value, users.key
          FROM last_state, jsonb_each(last_state.value -> 'users') users)
        ${
          address
            ? `(SELECT r.rn, 
          (SELECT key from last_users where value = '${address}') as user_id, 
          r.wallet_address, 
          r.balance
        FROM ranked r
        WHERE wallet_address = '${address}')
        UNION ALL`
            : ''
        }
        (SELECT r.rn, lu.key as user_id, r.wallet_address, r.balance
        FROM ranked r
        LEFT JOIN last_users lu ON r.wallet_address = lu.value
        LIMIT $2);
      `,
      [contractId, limit]
    );

    return result.rows;
  },

  getWarpyLastUserAddress: async (id) => {
    const result = await drePool.query(
      `WITH last_state AS (
        SELECT value
        FROM warp.sort_key_cache
        ORDER BY sort_key DESC
        OFFSET 1
        LIMIT 1),
      last_users AS (
        SELECT TRIM(users.value::text, '"') AS value, users.key
        FROM last_state, jsonb_each(last_state.value -> 'users') users)
      SELECT value FROM last_users WHERE KEY = $1;`,
      [id]
    );

    return result.rows;
  },

  getWarpySeasonsBoosts: async (timestamp) => {
    const result = await drePool.query(
      `
        WITH max_state AS (
          SELECT value FROM warp.sort_key_cache ORDER BY sort_key DESC LIMIT 1),
        boosts AS (
          SELECT value -> 'boosts' AS boosts FROM max_state
        ),
        seasons AS (
          SELECT seasons.value AS value, seasons.key
          FROM max_state, jsonb_each(value -> 'seasons') AS seasons
        ),
        limited_seasons AS (
          SELECT * FROM seasons 
          WHERE value -> 'from' < value -> 'to'  and (value ->> 'to')::int >= $1 and (value ->> 'from')::int <= $1
        )
        SELECT boosts, jsonb_object_agg(key, value) FROM boosts, limited_seasons GROUP BY boosts.boosts;`,
      [timestamp]
    );

    return result.rows;
  }
};
