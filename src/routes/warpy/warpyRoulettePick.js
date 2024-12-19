const { getRoulettePick } = require('../../db/nodeDb');
const { config } = require('../../config');

module.exports = {
  warpyRoulettePick: async function (ctx) {
    if (!config.availableFunctions.warpyAggreging) {
      ctx.body = 'Warpy aggreging functionality is disabled';
      ctx.status = 404;
      return;
    }
    const { interactionId } = ctx.query;

    if (!interactionId) {
      ctx.throw(422, 'Interaction id must be provided.');
    }

    try {
      const result = await getRoulettePick(interactionId);
      if (result.length) {
        ctx.body = {
          result
        };
        ctx.status = 200;
      } else {
        ctx.body = `No pick found for indicated interaction id.`;
        ctx.status = 404;
      }
    } catch (e) {
      ctx.body = e.message;
      ctx.status = 500;
    }
  }
};
