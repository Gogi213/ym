'use strict';

const { buildConfig } = require('./config.js');
const { jsonResponse, textResponse } = require('./response.js');
const { createTursoClient } = require('./turso.js');
const { handleReset } = require('./reset.js');
const { handleStatus } = require('./status.js');

function isAuthorized(request, config) {
  return request.headers.get('x-ingest-token') === config.ingestToken;
}

async function handleRequest(request, env) {
  const config = buildConfig(env);

  const url = new URL(request.url);
  const pathname = url.pathname;

  if (request.method === 'GET' && pathname === '/health') {
    return textResponse('ok');
  }

  if (!isAuthorized(request, config)) {
    return jsonResponse({ ok: false, error: 'Unauthorized' }, 401);
  }

  const tursoClient = createTursoClient(env);

  if (request.method === 'POST' && pathname === '/reset') {
    const result = await handleReset(request, tursoClient);
    return jsonResponse(result, result.ok === false ? 400 : 200);
  }

  if (request.method === 'POST' && pathname === '/ingest') {
    return jsonResponse({ ok: false, error: 'not_implemented' }, 501);
  }

  if (request.method === 'GET' && /^\/pipeline-runs\/[^/]+$/.test(pathname)) {
    const runDate = pathname.split('/').pop();
    return jsonResponse(await handleStatus(runDate, tursoClient));
  }

  return jsonResponse({ ok: false, error: 'not_found' }, 404);
}

module.exports = {
  handleRequest,
};
