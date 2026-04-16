'use strict';

const { handleRequest } = require('./routes.js');

module.exports = {
  handleRequest,
  async fetch(request, env) {
    return handleRequest(request, env);
  },
};
