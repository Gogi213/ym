'use strict';

async function handleReset(request, tursoClient) {
  const payload = await request.json();
  if (!payload || payload.action !== 'reset' || !payload.run_date) {
    return {
      ok: false,
      error: 'invalid_reset_payload',
    };
  }

  return tursoClient.resetRunDate(payload.run_date);
}

module.exports = {
  handleReset,
};
