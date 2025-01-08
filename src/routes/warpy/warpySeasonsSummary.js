const { getWarpySeasonsSummaryUserActivity, getWarpySeasonsSummaryUserHistory } = require('../../db/nodeDb');
const { config } = require('../../config');

module.exports = {
  warpySeasonsSummary: async function (ctx) {
    if (!config.availableFunctions.warpyAggreging) {
      ctx.body = 'Warpy aggreging functionality is disabled';
      ctx.status = 404;
      return;
    }

    const { id } = ctx.query;

    if (!id) {
      ctx.throw(422, 'User Id must be provided.');
    }

    try {
      const activity = await getWarpySeasonsSummaryUserActivity(id);
      const history = await getWarpySeasonsSummaryUserHistory(id);
      ctx.body = {
        ...activity,
        ...history
      };
      ctx.status = 200;
    } catch (e) {
      ctx.body = e.message;
      ctx.status = 500;
    }
  }
};
