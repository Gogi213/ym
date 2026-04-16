const test = require('node:test');
const assert = require('node:assert/strict');

const { buildConfig } = require('../../cloudflare_worker/src/config.js');

test('buildConfig validates required bindings', () => {
  assert.throws(() => buildConfig({}), /INGEST_TOKEN/);
});
