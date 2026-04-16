const test = require('node:test');
const assert = require('node:assert/strict');

const { handleRequest } = require('../../cloudflare_worker/src/index.js');

function buildEnv(overrides = {}) {
  return {
    INGEST_TOKEN: 'secret-token',
    TURSO_DATABASE_URL: 'libsql://example.turso.io',
    TURSO_AUTH_TOKEN: 'secret',
    RAW_FILES_BUCKET: { put() {} },
    __tursoClient: {
      async resetRunDate(runDate) {
        return { ok: true, action: 'reset', run_date: runDate };
      },
      async fetchRunDateSummary(runDate) {
        return {
          ok: true,
          run_date: runDate,
          exists: true,
          raw_files: 3,
          uploaded_files: 1,
          parsed_files: 1,
          failed_files: 1,
          normalize_status: 'pending_normalize',
        };
      },
    },
    ...overrides,
  };
}

test('POST /reset returns current run-date reset summary', async () => {
  const response = await handleRequest(
    new Request('https://example.com/reset', {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-ingest-token': 'secret-token',
      },
      body: JSON.stringify({ action: 'reset', run_date: '2026-04-17' }),
    }),
    buildEnv()
  );

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), {
    ok: true,
    action: 'reset',
    run_date: '2026-04-17',
  });
});

test('GET /pipeline-runs/:run_date returns manifest summary', async () => {
  const response = await handleRequest(
    new Request('https://example.com/pipeline-runs/2026-04-17', {
      headers: {
        'x-ingest-token': 'secret-token',
      },
    }),
    buildEnv()
  );

  assert.equal(response.status, 200);
  assert.deepEqual(await response.json(), {
    ok: true,
    run_date: '2026-04-17',
    exists: true,
    raw_files: 3,
    uploaded_files: 1,
    parsed_files: 1,
    failed_files: 1,
    normalize_status: 'pending_normalize',
  });
});
