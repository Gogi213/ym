'use strict';

const { buildConfig } = require('./config.js');
const { jsonResponse, textResponse } = require('./response.js');

async function handleRequest(request, env) {
  buildConfig(env);

  const url = new URL(request.url);
  const pathname = url.pathname;

  if (request.method === 'GET' && pathname === '/health') {
    return textResponse('ok');
  }

  if (request.method === 'POST' && pathname === '/reset') {
    return jsonResponse({ ok: false, error: 'not_implemented' }, 501);
  }

  if (request.method === 'POST' && pathname === '/ingest') {
    return jsonResponse({ ok: false, error: 'not_implemented' }, 501);
  }

  if (request.method === 'GET' && /^\/pipeline-runs\/[^/]+$/.test(pathname)) {
    return jsonResponse({ ok: false, error: 'not_implemented' }, 501);
  }

  return jsonResponse({ ok: false, error: 'not_found' }, 404);
}

module.exports = {
  handleRequest,
};
