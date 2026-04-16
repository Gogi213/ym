'use strict';

function createTursoClient(env) {
  if (env && env.__tursoClient) {
    return env.__tursoClient;
  }

  throw new Error('Cloudflare Worker Turso client is not configured yet');
}

module.exports = {
  createTursoClient,
};
