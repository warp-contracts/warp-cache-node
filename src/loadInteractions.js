const { config } = require('./config');
module.exports = async (startTimestamp, endTimestamp, whiteListedSources, blacklistedContracts, limit) => {
  const response = await postData({
    start: startTimestamp,
    end: endTimestamp,
    limit,
    src_ids: whiteListedSources,
    blacklisted_contracts: blacklistedContracts
  });

  if (response) {
    if (response.status == 204) {
      throw new Error('Blocks not yet ready for this timestamp range, wait!');
    } else if (response.ok) {
      return await response.json();
    } else {
      const text = await response.text();
      throw new Error(`Wrong response code: ${response.status}. ${text}`);
    }
  } else {
    throw new Error('Response null or undefined');
  }
};

async function postData(data = {}) {
  return await fetch(config.pollLoadInteractionsUrl, {
    method: 'POST',
    cache: 'no-store',
    headers: {
      'Content-Type': 'application/json'
    },
    body: JSON.stringify(data)
  });
}
