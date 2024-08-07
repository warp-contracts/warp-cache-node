const { getWarpySeasonsBoosts } = require('../../db/nodeDb');
const { config } = require('../../config');

module.exports = {
  warpySeasonsBoosts: async function (ctx) {
    if (!config.availableFunctions.warpyAggreging) {
      ctx.body = 'Warpy aggreging functionality is disabled';
      ctx.status = 404;
      return;
    }

    const { timestamp } = ctx.query;

    if (!timestamp) {
      ctx.throw(422, 'Timestamp must be provided.');
    }

    try {
      const result = await getWarpySeasonsBoosts(timestamp);
      ctx.body = result;
      ctx.status = 200;
    } catch (e) {
      ctx.body = e.message;
      ctx.status = 500;
    }
  }
};
