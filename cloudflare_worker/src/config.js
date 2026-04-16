'use strict';

function requireString(env, name) {
  const value = env && typeof env[name] === 'string' ? env[name].trim() : '';
  if (!value) {
    throw new Error(`Missing required Worker binding: ${name}`);
  }
  return value;
}

function requireBinding(env, name) {
  const value = env ? env[name] : undefined;
  if (!value) {
    throw new Error(`Missing required Worker binding: ${name}`);
  }
  return value;
}

function buildConfig(env) {
  return {
    ingestToken: requireString(env, 'INGEST_TOKEN'),
    tursoDatabaseUrl: requireString(env, 'TURSO_DATABASE_URL'),
    tursoAuthToken: requireString(env, 'TURSO_AUTH_TOKEN'),
    rawFilesBucket: requireBinding(env, 'RAW_FILES_BUCKET'),
  };
}

module.exports = {
  buildConfig,
};
