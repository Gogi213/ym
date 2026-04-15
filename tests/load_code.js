const fs = require('node:fs');
const path = require('node:path');
const vm = require('node:vm');

const EXPORT_NAMES = [
  'buildAttachmentMetadata_',
  'buildAttachmentRequest_',
  'buildIngestStatusRequest_',
  'buildRunContext_',
  'buildResetPayload_',
  'buildSupabaseSelectRequest_',
  'buildCandidatesByRunDate_',
  'chunkItems_',
  'compactText_',
  'collectCandidateMessages_',
  'detectAttachmentType_',
  'extractSubjectReportDate_',
  'findMatchedTopic_',
  'formatRunDate_',
  'getMonthBackfillSearchQuery_',
  'getMessageSearchQuery_',
  'getBackfillSettings_',
  'listMonthRunDates_',
  'loadTopicRulesFromSpreadsheet_',
  'loadTopicRulesFromValues_',
  'markLatestMessagesByTopic_',
  'normalizeText_',
  'buildRunDateExistsQuery_',
  'resolveSettingValue_',
  'resolveTargetRunDate_',
  'run',
  'runMonthBackfill',
  'subjectMatchesTopics_',
  'tokenizeTopic_'
];

const codePath = path.join(__dirname, '..', 'Code.js');
const source = fs.readFileSync(codePath, 'utf8');
const exportCode = '\n;globalThis.__test_exports__ = {' +
  EXPORT_NAMES.map((name) => name + ':' + name).join(',') +
  '};';

const context = {
  console,
  Date,
  Intl,
  JSON,
  Math,
  String,
  Number,
  Boolean,
  RegExp
};

vm.runInNewContext(source + exportCode, context, { filename: 'Code.js' });

function cloneValue(value) {
  if (value === null || value === undefined) {
    return value;
  }

  if (typeof value === 'object') {
    return JSON.parse(JSON.stringify(value));
  }

  return value;
}

const wrappedExports = {};

for (const exportName of EXPORT_NAMES) {
  const exportedValue = context.__test_exports__[exportName];
  if (typeof exportedValue === 'function') {
    wrappedExports[exportName] = (...args) => cloneValue(exportedValue(...args));
  } else {
    wrappedExports[exportName] = cloneValue(exportedValue);
  }
}

module.exports = wrappedExports;
