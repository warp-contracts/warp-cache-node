const { drePool } = require('../../db/nodeDb');
const MAX_NFTS_PER_PAGE = 100;
const ATOMIC_ASSET_TAG_VALUE = 'atomic-asset';

module.exports = {
  taggedNftByOwner: async (ctx) => {
    const { page, limit, ownerAddress } = ctx.query;

    const parsedPage = page ? parseInt(page) : 1;
    const parsedLimit = limit ? Math.min(parseInt(limit), MAX_NFTS_PER_PAGE) : MAX_NFTS_PER_PAGE;
    const offset = parsedPage ? (parsedPage - 1) * parsedLimit : 0;

    try {
      if (!ownerAddress) {
        throw Error(`"ownerAddress" param is required`);
      }

      const result = await drePool.query(
        `
            WITH deploy AS (
                SELECT * FROM dre.deployments
                WHERE $1 IN (deployments.tag_index_0, deployments.tag_index_1, deployments.tag_index_2, deployments.tag_index_3, deployments.tag_index_4)
            ),
             states AS (
                 SELECT DISTINCT ON (key) key AS contract_tx_id, value as state
                 FROM warp.sort_key_cache
                 WHERE value ->> 'owner' = $2
                 ORDER BY key, sort_key desc
             )
        SELECT states.contract_tx_id, states.state
        FROM states
        JOIN deploy
        ON deploy.contract_tx_id = states.contract_tx_id
        ORDER BY deploy.id ASC
        LIMIT $3 OFFSET $4`,
        [ATOMIC_ASSET_TAG_VALUE, ownerAddress, parsedLimit, offset]
      );

      ctx.body = {
        paging: {
          limit: parsedLimit,
          items: result?.rows?.length,
          page: parsedPage
        },
        contracts: result?.rows
      };

      ctx.status = 200;
    } catch (e) {
      ctx.body = e.message;
      ctx.status = 500;
    }
  }
};
