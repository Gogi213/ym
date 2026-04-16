const test = require('node:test');
const assert = require('node:assert/strict');

const { handleRequest } = require('../../cloudflare_worker/src/index.js');

function stubEnv(overrides = {}) {
  return {
    INGEST_TOKEN: 'token',
    TURSO_DATABASE_URL: 'libsql://example.turso.io',
    TURSO_AUTH_TOKEN: 'secret',
    ...overrides,
  };
}

test('router returns ok for GET /health', async () => {
  const response = await handleRequest(
    new Request('https://example.com/health'),
    stubEnv()
  );

  assert.equal(response.status, 200);
  assert.equal(await response.text(), 'ok');
});
