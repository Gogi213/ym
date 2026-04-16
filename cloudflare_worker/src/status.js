'use strict';

async function handleStatus(runDate, tursoClient) {
  return tursoClient.fetchRunDateSummary(runDate);
}

module.exports = {
  handleStatus,
};
