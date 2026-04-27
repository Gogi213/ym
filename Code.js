const CONFIG_ = {
  sourceSpreadsheetId: '17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA',
  sourceSheetName: 'отчеты',
  sourceColumn: 1,
  sourceSecondaryColumn: 2,
  tursoDatabaseUrlProperty: 'TURSO_DATABASE_URL',
  tursoAuthTokenProperty: 'TURSO_AUTH_TOKEN',
  verboseLoggingProperty: 'VERBOSE_LOGGING',
  runDayOffset: -1,
  searchBatchSize: 100
};

function normalizeText_(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/[\u0000-\u001f]+/g, ' ')
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function tokenizeTopic_(text) {
  return normalizeText_(text)
    .split(' ')
    .filter(Boolean);
}

function compactText_(text) {
  return normalizeText_(text).replace(/\s+/g, '');
}

function loadTopicRulesFromValues_(values) {
  const rules = [];
  const seenMatchedTopics = {};

  for (let i = 0; i < values.length; i++) {
    const primaryRaw = String(values[i] && values[i][0] ? values[i][0] : '').trim();
    const secondaryRaw = String(values[i] && values[i][1] ? values[i][1] : '').trim();

    if (primaryRaw) {
      const primaryTokens = tokenizeTopic_(primaryRaw);
      if (primaryTokens.length && !seenMatchedTopics[primaryRaw]) {
        rules.push({
          raw: primaryRaw,
          matchedTopic: primaryRaw,
          primaryTopic: primaryRaw,
          topicRole: 'primary',
          tokens: primaryTokens
        });
        seenMatchedTopics[primaryRaw] = true;
      }
    }

    if (secondaryRaw) {
      const secondaryTokens = tokenizeTopic_(secondaryRaw);
      if (secondaryTokens.length && !seenMatchedTopics[secondaryRaw]) {
        rules.push({
          raw: secondaryRaw,
          matchedTopic: secondaryRaw,
          primaryTopic: primaryRaw || secondaryRaw,
          topicRole: 'secondary',
          tokens: secondaryTokens
        });
        seenMatchedTopics[secondaryRaw] = true;
      }
    }
  }

  return rules;
}

function extractSubjectBody_(subject) {
  const rawSubject = String(subject || '');
  const quotedMatch = rawSubject.match(/«([^»]+)»/) || rawSubject.match(/"([^"]+)"/);
  return quotedMatch ? quotedMatch[1] : '';
}

function extractSubjectReportDate_(subject) {
  const rawSubject = String(subject || '');
  const match = rawSubject.match(/за\s+(\d{2})\.(\d{2})\.(\d{4})/i);
  if (!match) {
    return null;
  }

  return match[3] + '-' + match[2] + '-' + match[1];
}

function findMatchedTopicRule_(subject, topicRules) {
  const normalizedMatchTarget = normalizeText_(extractSubjectBody_(subject) || subject);
  const compactMatchTarget = compactText_(extractSubjectBody_(subject) || subject);

  for (let i = 0; i < topicRules.length; i++) {
    const topicRule = topicRules[i];
    const normalizedTopic = normalizeText_(topicRule.raw);
    if (!normalizedTopic) {
      continue;
    }

    if (normalizedMatchTarget.indexOf(normalizedTopic) !== -1) {
      return topicRule;
    }

    const compactTopic = compactText_(topicRule.raw);
    if (compactTopic && compactMatchTarget.indexOf(compactTopic) !== -1) {
      return topicRule;
    }
  }

  return null;
}

function findMatchedTopic_(subject, topicRules) {
  const topicRule = findMatchedTopicRule_(subject, topicRules);
  return topicRule ? topicRule.raw : null;
}

function subjectMatchesTopics_(subject, topicRules) {
  return findMatchedTopicRule_(subject, topicRules) !== null;
}

function padNumber_(value) {
  return String(value).padStart(2, '0');
}

function formatRunDate_(date, timeZone) {
  const formatter = new Intl.DateTimeFormat('en-CA', {
    timeZone,
    year: 'numeric',
    month: '2-digit',
    day: '2-digit'
  });

  return formatter.format(date);
}

function resolveTargetRunDate_(date, timeZone, dayOffset) {
  const offset = Number(dayOffset || 0);
  const shiftedDate = new Date(date.getTime() + offset * 24 * 60 * 60 * 1000);
  return formatRunDate_(shiftedDate, timeZone);
}

function inferEffectiveRunDate_(subject, messageDate, timeZone) {
  const subjectReportDate = extractSubjectReportDate_(subject);
  if (subjectReportDate) {
    return subjectReportDate;
  }
  return resolveTargetRunDate_(messageDate, timeZone, CONFIG_.runDayOffset);
}

function listMonthRunDates_(targetRunDate) {
  const raw = String(targetRunDate || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw new Error('Invalid targetRunDate: ' + raw);
  }

  const year = Number(raw.slice(0, 4));
  const month = Number(raw.slice(5, 7));
  const day = Number(raw.slice(8, 10));
  const dates = [];

  for (let currentDay = 1; currentDay <= day; currentDay++) {
    dates.push(
      String(year).padStart(4, '0') + '-' +
      String(month).padStart(2, '0') + '-' +
      String(currentDay).padStart(2, '0')
    );
  }

  return dates;
}

function buildRunDateExistsQuery_(runDate) {
  return 'select=id&run_date=eq.' + encodeURIComponent(String(runDate || '')) + '&limit=1';
}

function getMessageSearchQuery_(dayOffset) {
  const lookbackDays = Math.max(2, Math.abs(Number(dayOffset || 0)) + 2);
  return 'newer_than:' + lookbackDays + 'd has:attachment';
}

function getMonthBackfillSearchQuery_(targetRunDate) {
  const raw = String(targetRunDate || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw new Error('Invalid targetRunDate: ' + raw);
  }

  const year = raw.slice(0, 4);
  const month = raw.slice(5, 7);
  const day = raw.slice(8, 10);
  const nextDay = String(Number(day) + 1).padStart(2, '0');

  return 'after:' + year + '/' + month + '/01' +
    ' before:' + year + '/' + month + '/' + nextDay +
    ' has:attachment';
}

function getRunDateSearchQuery_(runDate) {
  const raw = String(runDate || '').trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    throw new Error('Invalid runDate: ' + raw);
  }

  const year = Number(raw.slice(0, 4));
  const month = Number(raw.slice(5, 7));
  const day = Number(raw.slice(8, 10));
  const currentDate = new Date(Date.UTC(year, month - 1, day));
  const mailboxDate = new Date(currentDate.getTime() + 24 * 60 * 60 * 1000);
  const nextDate = new Date(mailboxDate.getTime() + 24 * 60 * 60 * 1000);
  const mailboxYear = String(mailboxDate.getUTCFullYear()).padStart(4, '0');
  const mailboxMonth = String(mailboxDate.getUTCMonth() + 1).padStart(2, '0');
  const mailboxDay = String(mailboxDate.getUTCDate()).padStart(2, '0');
  const nextYear = String(nextDate.getUTCFullYear()).padStart(4, '0');
  const nextMonth = String(nextDate.getUTCMonth() + 1).padStart(2, '0');
  const nextDay = String(nextDate.getUTCDate()).padStart(2, '0');

  return 'after:' + mailboxYear + '/' + mailboxMonth + '/' + mailboxDay +
    ' before:' + nextYear + '/' + nextMonth + '/' + nextDay +
    ' has:attachment';
}

function detectAttachmentType_(attachment) {
  const name = String(attachment && attachment.getName ? attachment.getName() : '').toLowerCase();
  const contentType = String(attachment && attachment.getContentType ? attachment.getContentType() : '').toLowerCase();

  if (
    name.endsWith('.xlsx') ||
    contentType.indexOf('openxmlformats-officedocument.spreadsheetml.sheet') !== -1
  ) {
    return 'xlsx';
  }

  if (
    name.endsWith('.csv') ||
    contentType.indexOf('text/csv') !== -1 ||
    contentType.indexOf('csv') !== -1
  ) {
    return 'csv';
  }

  return null;
}

function loadTopicRulesFromSpreadsheet_(spreadsheet) {
  const sheet = spreadsheet.getSheetByName(CONFIG_.sourceSheetName);
  if (!sheet) {
    throw new Error('Missing topic sheet: ' + CONFIG_.sourceSheetName);
  }

  const lastRow = sheet.getLastRow();
  if (lastRow < 2) {
    return [];
  }

  return loadTopicRulesFromValues_(
    sheet.getRange(2, CONFIG_.sourceColumn, lastRow - 1, CONFIG_.sourceSecondaryColumn).getDisplayValues()
  );
}

function listThreadsForQuery_(gmailApp, query) {
  const threads = [];

  for (let start = 0; ; start += CONFIG_.searchBatchSize) {
    const batch = gmailApp.search(query, start, CONFIG_.searchBatchSize);
    if (!batch.length) {
      break;
    }

    for (let i = 0; i < batch.length; i++) {
      threads.push(batch[i]);
    }
  }

  return threads;
}

function collectCandidateMessages_(threads, topicRules, runDate, timeZone) {
  const candidates = [];

  for (let i = 0; i < threads.length; i++) {
    const thread = threads[i];
    const messages = thread.getMessages();

    for (let j = 0; j < messages.length; j++) {
      const message = messages[j];
      const subject = String(message.getSubject() || '').trim();
      const subjectReportDate = extractSubjectReportDate_(subject);
      const effectiveRunDate = inferEffectiveRunDate_(subject, message.getDate(), timeZone);

      if (effectiveRunDate !== runDate) {
        continue;
      }

      const matchedTopicRule = findMatchedTopicRule_(subject, topicRules);
      if (!matchedTopicRule) {
        continue;
      }

      candidates.push({
        effectiveRunDate,
        matchedTopic: matchedTopicRule.matchedTopic || matchedTopicRule.raw,
        primaryTopic: matchedTopicRule.primaryTopic || matchedTopicRule.raw,
        topicRole: matchedTopicRule.topicRole || 'primary',
        message,
        messageDate: message.getDate(),
        messageId: message.getId(),
        subjectReportDate,
        subject,
        threadId: thread.getId()
      });
    }
  }

  return candidates;
}

function buildCandidatesByRunDate_(threads, topicRules, timeZone) {
  const grouped = {};

  for (let i = 0; i < threads.length; i++) {
    const thread = threads[i];
    const messages = thread.getMessages();

    for (let j = 0; j < messages.length; j++) {
      const message = messages[j];
      const subject = String(message.getSubject() || '').trim();
      const subjectReportDate = extractSubjectReportDate_(subject);
      const effectiveRunDate = inferEffectiveRunDate_(subject, message.getDate(), timeZone);
      const matchedTopicRule = findMatchedTopicRule_(subject, topicRules);

      if (!matchedTopicRule) {
        continue;
      }

      if (!grouped[effectiveRunDate]) {
        grouped[effectiveRunDate] = [];
      }

      grouped[effectiveRunDate].push({
        effectiveRunDate,
        matchedTopic: matchedTopicRule.matchedTopic || matchedTopicRule.raw,
        primaryTopic: matchedTopicRule.primaryTopic || matchedTopicRule.raw,
        topicRole: matchedTopicRule.topicRole || 'primary',
        message,
        messageDate: message.getDate(),
        messageId: message.getId(),
        subjectReportDate,
        subject,
        threadId: thread.getId()
      });
    }
  }

  return grouped;
}

function markLatestMessagesByTopic_(messages) {
  const latestByTopic = {};

  for (let i = 0; i < messages.length; i++) {
    const message = messages[i];
    const topic = message.matchedTopic;
    if (!topic) {
      continue;
    }

    const currentLatest = latestByTopic[topic];
    if (!currentLatest || new Date(message.messageDate) > new Date(currentLatest.messageDate)) {
      latestByTopic[topic] = message;
    }
  }

  return messages.map((message) => {
    return Object.assign({}, message, {
      isLatestForTopic: Boolean(
        message.matchedTopic &&
        latestByTopic[message.matchedTopic] &&
        latestByTopic[message.matchedTopic] === message
      )
    });
  });
}

function buildResetPayload_(runDate) {
  return {
    action: 'reset',
    run_date: runDate
  };
}


function buildAttachmentMetadata_(input) {
  return {
    action: 'ingest',
    run_date: input.runDate,
    primary_topic: input.primaryTopic,
    matched_topic: input.matchedTopic,
    topic_role: input.topicRole,
    message_subject: input.subject,
    message_date: input.messageDate instanceof Date
      ? input.messageDate.toISOString()
      : String(input.messageDate || ''),
    message_id: input.messageId,
    thread_id: input.threadId,
    attachment_name: input.attachmentName,
    attachment_type: input.attachmentType
  };
}

function buildAttachmentRequest_(settings, attachment, metadata) {
  return buildTursoPipelineRequest_(settings, buildTursoAttachmentRequests_(attachment, metadata));
}

function fetchRequest_(urlFetchApp, request) {
  return urlFetchApp.fetch(request.url, {
    method: request.method,
    headers: request.headers,
    muteHttpExceptions: request.muteHttpExceptions,
    contentType: request.contentType,
    payload: request.payload
  });
}

function sleepMs_(milliseconds) {
  if (typeof Utilities !== 'undefined' && Utilities.sleep) {
    Utilities.sleep(milliseconds);
  }
}

function isRetriableFetchError_(error) {
  const message = String(error && error.message ? error.message : error || '');
  return /Address unavailable/i.test(message);
}

function isTransientHttpStatus_(responseCode) {
  return responseCode === 502 || responseCode === 503 || responseCode === 504;
}

function fetchRequestWithRetry_(urlFetchApp, request, options) {
  const retryOptions = options || {};
  const maxAttempts = Math.max(1, Number(retryOptions.maxAttempts || 1));
  const retryableStatuses = retryOptions.retryableStatuses || [];

  for (let attempt = 0; attempt < maxAttempts; attempt++) {
    try {
      const response = fetchRequest_(urlFetchApp, request);
      const responseCode = Number(response.getResponseCode());
      if (retryableStatuses.indexOf(responseCode) !== -1 && attempt < maxAttempts - 1) {
        sleepMs_((attempt + 1) * 1500);
        continue;
      }
      return response;
    } catch (error) {
      if (isRetriableFetchError_(error) && attempt < maxAttempts - 1) {
        sleepMs_((attempt + 1) * 1500);
        continue;
      }
      throw error;
    }
  }

  throw new Error('Unreachable fetch retry state');
}

function parseJsonResponse_(response) {
  const responseCode = response.getResponseCode();
  const body = String(response.getContentText() || '');
  let json = null;

  try {
    json = body ? JSON.parse(body) : null;
  } catch (error) {
    json = null;
  }

  return { body, json, responseCode };
}

function assertSuccessfulResponse_(response, actionLabel) {
  const parsed = parseJsonResponse_(response);
  if (parsed.responseCode >= 200 && parsed.responseCode < 300) {
    return parsed;
  }
  throw new Error(
    actionLabel + ' failed with HTTP ' + parsed.responseCode + ': ' + parsed.body
  );
}

function assertSuccessfulIngestResponse_(settings, response, actionLabel) {
  return assertSuccessfulTursoResponse_(response, actionLabel);
}

function chunkItems_(items, chunkSize) {
  const chunks = [];
  const size = Math.max(1, Number(chunkSize || 1));
  for (let i = 0; i < items.length; i += size) {
    chunks.push(items.slice(i, i + size));
  }
  return chunks;
}

function uniqueValues_(items) {
  const seen = {};
  const values = [];
  for (let i = 0; i < items.length; i++) {
    const value = String(items[i] || '').trim();
    if (!value || seen[value]) {
      continue;
    }
    seen[value] = true;
    values.push(value);
  }
  return values;
}

function resolveSettingValue_(propertyValue, fallbackValue, propertyName) {
  const runtimeValue = String(propertyValue || '').trim();
  if (runtimeValue) {
    return runtimeValue;
  }

  const fallback = String(fallbackValue || '').trim();
  if (fallback) {
    return fallback;
  }

  throw new Error('Missing script property "' + propertyName + '"');
}

function logProgress_(phase, payload) {
  const message = JSON.stringify(Object.assign({ phase }, payload || {}), null, 2);

  if (typeof Logger !== 'undefined' && Logger.log) {
    Logger.log(message);
    return;
  }
  if (typeof console !== 'undefined' && console.log) {
    console.log(message);
  }
}

function elapsedMs_(startedAtMs) {
  return Date.now() - startedAtMs;
}

function getAppsScriptRuntime_() {
  if (
    typeof Gmail === 'undefined' ||
    typeof PropertiesService === 'undefined' ||
    typeof SpreadsheetApp === 'undefined' ||
    typeof Session === 'undefined' ||
    typeof UrlFetchApp === 'undefined'
  ) {
    throw new Error('Apps Script runtime globals are unavailable');
  }

  return {
    Gmail,
    PropertiesService,
    Session,
    SpreadsheetApp,
    UrlFetchApp
  };
}


function normalizeTursoPipelineUrl_(databaseUrl) {
  const raw = String(databaseUrl || '').trim();
  if (!raw) {
    return '';
  }

  const withoutPipeline = raw.replace(/\/v2\/pipeline$/i, '').replace(/\/+$/, '');
  if (/^libsql:\/\//i.test(withoutPipeline)) {
    return 'https://' + withoutPipeline.replace(/^libsql:\/\//i, '') + '/v2/pipeline';
  }
  if (/^https?:\/\//i.test(withoutPipeline)) {
    return withoutPipeline + '/v2/pipeline';
  }
  return 'https://' + withoutPipeline.replace(/^\/+/, '') + '/v2/pipeline';
}

function buildTursoValue_(value) {
  if (value === null || value === undefined) {
    return { type: 'null' };
  }
  if (typeof value === 'boolean') {
    return { type: 'integer', value: value ? '1' : '0' };
  }
  if (typeof value === 'number' && isFinite(value)) {
    return Number.isInteger(value)
      ? { type: 'integer', value: String(value) }
      : { type: 'float', value: String(value) };
  }
  return { type: 'text', value: String(value) };
}

function buildTursoExecuteRequest_(sql, args) {
  const stmt = { sql };
  if (Array.isArray(args) && args.length) {
    stmt.args = args.map(buildTursoValue_);
  }
  return { type: 'execute', stmt };
}

function buildTursoPipelineRequest_(settings, requests) {
  return {
    url: settings.pipelineUrl,
    method: 'post',
    contentType: 'application/json',
    headers: {
      Authorization: 'Bearer ' + settings.authToken
    },
    muteHttpExceptions: true,
    payload: JSON.stringify({ requests })
  };
}

function byteToHex_(value) {
  const normalized = Number(value);
  const byte = normalized < 0 ? normalized + 256 : normalized;
  return (byte + 256).toString(16).slice(-2);
}

function computeSha256HexFromBytes_(bytes) {
  if (typeof Utilities !== 'undefined' && Utilities.computeDigest && Utilities.DigestAlgorithm) {
    return Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, bytes)
      .map(byteToHex_)
      .join('');
  }
  if (typeof crypto !== 'undefined' && crypto.createHash) {
    return crypto.createHash('sha256').update(Buffer.from(bytes)).digest('hex');
  }
  throw new Error('No SHA-256 digest implementation available in this runtime');
}

function buildRawFileKey_(metadata, fileHash) {
  return [
    String(metadata.run_date || '').trim(),
    String(metadata.primary_topic || '').trim(),
    String(fileHash || '').trim()
  ].join('|');
}

function encodeBytesBase64_(bytes) {
  if (typeof Utilities !== 'undefined' && Utilities.base64Encode) {
    return Utilities.base64Encode(bytes);
  }
  if (typeof Buffer !== 'undefined') {
    return Buffer.from(bytes).toString('base64');
  }
  throw new Error('No base64 encoder available in this runtime');
}

function buildAttachmentBlobData_(attachment, metadata) {
  const blob = attachment.copyBlob();
  const bytes = blob.getBytes();
  const contentType = attachment.getContentType ? attachment.getContentType() : '';
  const fileHash = computeSha256HexFromBytes_(bytes);
  const rawFileKey = buildRawFileKey_(metadata, fileHash);
  return {
    bytes,
    contentType,
    fileHash,
    rawFileKey,
    fileId: rawFileKey
  };
}

function buildPreparedAttachmentInput_(attachment, metadata) {
  return {
    attachment,
    metadata,
    blobData: buildAttachmentBlobData_(attachment, metadata)
  };
}

function buildTursoResetRequests_(runDate) {
  return [
    buildTursoExecuteRequest_(
      'delete from ingest_file_payloads where file_id in (select id from ingest_files where run_date = ?)',
      [runDate]
    ),
    buildTursoExecuteRequest_(
      'delete from ingest_files where run_date = ?',
      [runDate]
    ),
    buildTursoExecuteRequest_(
      "insert into pipeline_runs (run_date, raw_revision, normalize_status, raw_files, raw_rows, normalized_files, normalized_rows, last_ingest_at, normalized_at, last_error, updated_at) values (?, 0, 'pending_normalize', 0, 0, 0, 0, current_timestamp, null, null, current_timestamp) on conflict(run_date) do nothing",
      [runDate]
    ),
    buildTursoExecuteRequest_(
      "update pipeline_runs set raw_revision = raw_revision + 1, normalize_status = 'pending_normalize', raw_files = 0, raw_rows = 0, normalized_files = 0, normalized_rows = 0, last_ingest_at = current_timestamp, normalized_at = null, last_error = null, updated_at = current_timestamp where run_date = ?",
      [runDate]
    ),
    { type: 'close' }
  ];
}

function buildTursoRunInitRequests_(runDate) {
  return [
    buildTursoExecuteRequest_(
      "insert into pipeline_runs (run_date, raw_revision, normalize_status, raw_files, raw_rows, normalized_files, normalized_rows, last_ingest_at, normalized_at, last_error, created_at, updated_at) values (?, 1, 'raw_only', 0, 0, 0, 0, current_timestamp, null, null, current_timestamp, current_timestamp) on conflict(run_date) do update set raw_revision = case when pipeline_runs.normalize_status in ('ready', 'normalize_error') then pipeline_runs.raw_revision + 1 else pipeline_runs.raw_revision end, normalize_status = 'raw_only', raw_rows = 0, normalized_files = 0, normalized_rows = 0, last_ingest_at = current_timestamp, normalized_at = null, last_error = null, updated_at = current_timestamp",
      [runDate]
    )
  ];
}

function buildTursoRunRefreshRequests_(runDate) {
  return [
    buildTursoExecuteRequest_(
      'update pipeline_runs set raw_files = (select count(*) from ingest_files where run_date = ?), last_ingest_at = current_timestamp, updated_at = current_timestamp where run_date = ?',
      [runDate, runDate]
    )
  ];
}

function buildTursoAttachmentStatements_(attachmentInput) {
  const metadata = attachmentInput.metadata;
  const blobData = attachmentInput.blobData || buildAttachmentBlobData_(attachmentInput.attachment, metadata);
  const bytes = blobData.bytes;
  const contentType = blobData.contentType;
  const fileBase64 = encodeBytesBase64_(bytes);
  const fileSizeBytes = Array.isArray(bytes) ? bytes.length : Number(bytes && bytes.length ? bytes.length : 0);
  const fileHash = blobData.fileHash;
  const rawFileKey = blobData.rawFileKey;
  const fileId = blobData.fileId;

  return [
    buildTursoExecuteRequest_(
      "insert into ingest_files (id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject, primary_topic, matched_topic, topic_role, attachment_name, attachment_type, status, header_json, row_count, error_text) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'raw_only', '[]', 0, null) on conflict(raw_file_key) do update set run_date = excluded.run_date, message_id = excluded.message_id, thread_id = excluded.thread_id, message_date = excluded.message_date, message_subject = excluded.message_subject, primary_topic = excluded.primary_topic, matched_topic = excluded.matched_topic, topic_role = excluded.topic_role, attachment_name = excluded.attachment_name, attachment_type = excluded.attachment_type, status = 'raw_only', header_json = '[]', row_count = 0, error_text = null, updated_at = current_timestamp",
      [
        fileId,
        rawFileKey,
        fileHash,
        metadata.run_date,
        metadata.message_id,
        metadata.thread_id,
        metadata.message_date,
        metadata.message_subject,
        metadata.primary_topic,
        metadata.matched_topic,
        metadata.topic_role,
        metadata.attachment_name,
        metadata.attachment_type
      ]
    ),
    buildTursoExecuteRequest_(
      'insert into ingest_file_payloads (file_id, content_type, file_size_bytes, file_base64) values (?, ?, ?, ?) on conflict(file_id) do update set content_type = excluded.content_type, file_size_bytes = excluded.file_size_bytes, file_base64 = excluded.file_base64',
      [fileId, contentType, fileSizeBytes, fileBase64]
    ),
  ];
}

function buildTursoAttachmentBatchRequest_(settings, batch, options) {
  const batchOptions = options || {};
  const requests = [];

  if (batchOptions.includeRunInit && batch.length) {
    requests.push.apply(requests, buildTursoRunInitRequests_(batch[0].metadata.run_date));
  }

  for (let index = 0; index < batch.length; index++) {
    requests.push.apply(
      requests,
      buildTursoAttachmentStatements_(batch[index])
    );
  }

  if (batch.length) {
    requests.push.apply(
      requests,
      buildTursoRunRefreshRequests_(batch[0].metadata.run_date)
    );
  }

  requests.push({ type: 'close' });
  return buildTursoPipelineRequest_(settings, requests);
}

function buildTursoAttachmentRequests_(attachment, metadata) {
  return JSON.parse(
    buildTursoAttachmentBatchRequest_(
      { pipelineUrl: '__internal__', authToken: '__internal__' },
      [buildPreparedAttachmentInput_(attachment, metadata)],
      { includeRunInit: true }
    ).payload
  ).requests;
}

function buildTursoExistingRawFileKeysRequest_(settings, rawFileKeys) {
  const placeholders = rawFileKeys.map(() => '?').join(', ');
  return buildTursoPipelineRequest_(
    settings,
    [
      buildTursoExecuteRequest_(
        `select f.raw_file_key from ingest_files f join ingest_file_payloads p on p.file_id = f.id where f.raw_file_key in (${placeholders})`,
        rawFileKeys
      ),
      { type: 'close' }
    ]
  );
}

function buildTursoStatusRequest_(settings, runDate) {
  return buildTursoPipelineRequest_(
    settings,
    [
      buildTursoExecuteRequest_(
        'select normalize_status from pipeline_runs where run_date = ? limit 1',
        [runDate]
      ),
      { type: 'close' }
    ]
  );
}

function readTursoCellValue_(cell) {
  if (cell && typeof cell === 'object') {
    if (Object.prototype.hasOwnProperty.call(cell, 'value')) {
      return cell.value;
    }
    if (Object.prototype.hasOwnProperty.call(cell, 'base64')) {
      return cell.base64;
    }
  }
  return cell;
}

function extractTursoRows_(parsed) {
  const result = parsed && parsed.json && Array.isArray(parsed.json.results)
    ? parsed.json.results[0]
    : null;
  const executeResult = result && result.response && result.response.result ? result.response.result : null;
  const cols = Array.isArray(executeResult && executeResult.cols)
    ? executeResult.cols.map((column) => typeof column === 'string' ? column : column && column.name)
    : [];
  const rows = Array.isArray(executeResult && executeResult.rows) ? executeResult.rows : [];

  return rows.map((row) => {
    if (Array.isArray(row)) {
      const mapped = {};
      for (let index = 0; index < row.length; index++) {
        mapped[String(cols[index] || index)] = readTursoCellValue_(row[index]);
      }
      return mapped;
    }
    if (row && typeof row === 'object') {
      const mapped = {};
      const keys = Object.keys(row);
      for (let index = 0; index < keys.length; index++) {
        mapped[keys[index]] = readTursoCellValue_(row[keys[index]]);
      }
      return mapped;
    }
    return row;
  });
}

function assertSuccessfulTursoResponse_(response, actionLabel) {
  const parsed = parseJsonResponse_(response);

  if (parsed.responseCode < 200 || parsed.responseCode >= 300) {
    throw new Error(
      actionLabel + ' failed with HTTP ' + parsed.responseCode + ': ' + parsed.body
    );
  }

  const results = parsed.json && Array.isArray(parsed.json.results) ? parsed.json.results : [];
  for (let index = 0; index < results.length; index++) {
    if (results[index] && results[index].type === 'ok') {
      continue;
    }

    const errorPayload = results[index] && (results[index].error || results[index]);
    throw new Error(
      actionLabel + ' failed: ' + JSON.stringify(errorPayload || parsed.json)
    );
  }

  return parsed;
}

function postTursoReset_(urlFetchApp, settings, runDate) {
  return assertSuccessfulTursoResponse_(
    fetchRequestWithRetry_(
      urlFetchApp,
      buildTursoPipelineRequest_(settings, buildTursoResetRequests_(runDate)),
      {
        maxAttempts: 3,
        retryableStatuses: [502, 503, 504]
      }
    ),
    'Reset request'
  );
}

function fetchTursoRunDateExists_(urlFetchApp, settings, runDate) {
  const response = assertSuccessfulTursoResponse_(
    fetchRequestWithRetry_(urlFetchApp, buildTursoStatusRequest_(settings, runDate), {
      maxAttempts: 3,
      retryableStatuses: [502, 503, 504]
    }),
    'Run date existence check'
  );
  const rows = extractTursoRows_(response);
  if (!rows.length) {
    return false;
  }
  return true;
}

function fetchExistingTursoRunDates_(urlFetchApp, settings, startRunDate, endRunDate) {
  const found = {};
  const response = assertSuccessfulTursoResponse_(
    fetchRequestWithRetry_(
      urlFetchApp,
      buildTursoExistingRunDatesRequest_(settings, startRunDate, endRunDate),
      {
        maxAttempts: 3,
        retryableStatuses: [502, 503, 504]
      }
    ),
    'Existing run dates lookup'
  );
  const rows = extractTursoRows_(response);
  for (let index = 0; index < rows.length; index++) {
    const runDate = String(rows[index].run_date || '').trim();
    if (runDate) {
      found[runDate] = true;
    }
  }
  return found;
}

function fetchExistingTursoRawFileKeys_(urlFetchApp, settings, rawFileKeys) {
  const found = {};
  const uniqueKeys = uniqueValues_(rawFileKeys || []);
  if (!uniqueKeys.length) {
    return found;
  }

  const keyBatches = chunkItems_(uniqueKeys, 100);
  for (let index = 0; index < keyBatches.length; index++) {
    const response = assertSuccessfulTursoResponse_(
      fetchRequestWithRetry_(
        urlFetchApp,
        buildTursoExistingRawFileKeysRequest_(settings, keyBatches[index]),
        {
          maxAttempts: 3,
          retryableStatuses: [502, 503, 504]
        }
      ),
      'Existing raw file lookup'
    );
    const rows = extractTursoRows_(response);
    for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
      const rawFileKey = String(rows[rowIndex].raw_file_key || '').trim();
      if (rawFileKey) {
        found[rawFileKey] = true;
      }
    }
  }

  return found;
}


function getScriptSettings_(propertiesService) {
  const scriptProperties = propertiesService.getScriptProperties();
  return {
    mode: 'turso',
    pipelineUrl: normalizeTursoPipelineUrl_(
      resolveSettingValue_(
        scriptProperties.getProperty(CONFIG_.tursoDatabaseUrlProperty),
        '',
        CONFIG_.tursoDatabaseUrlProperty
      )
    ),
    authToken: resolveSettingValue_(
      scriptProperties.getProperty(CONFIG_.tursoAuthTokenProperty),
      '',
      CONFIG_.tursoAuthTokenProperty
    )
  };
}

function getBackfillSettings_(propertiesService) {
  return Object.assign({ skipExistingEnabled: true }, getScriptSettings_(propertiesService));
}

function postReset_(urlFetchApp, settings, runDate) {
  return postTursoReset_(urlFetchApp, settings, runDate);
}

function fetchRunDateExists_(urlFetchApp, settings, runDate) {
  return fetchTursoRunDateExists_(urlFetchApp, settings, runDate);
}

function fetchExistingRunDates_(urlFetchApp, settings, startRunDate, endRunDate) {
  return fetchExistingTursoRunDates_(urlFetchApp, settings, startRunDate, endRunDate);
}

function buildRunContext_(runtime) {
  const timeZone = runtime.Session.getScriptTimeZone();
  const spreadsheet = runtime.SpreadsheetApp.openById(CONFIG_.sourceSpreadsheetId);
  const topicRules = loadTopicRulesFromSpreadsheet_(spreadsheet);
  const settings = getScriptSettings_(runtime.PropertiesService);
  const scriptProperties = runtime.PropertiesService.getScriptProperties();
  const verboseLogging = /^(1|true|yes|on)$/i.test(
    String(scriptProperties.getProperty(CONFIG_.verboseLoggingProperty) || '').trim()
  );

  if (!topicRules.length) {
    throw new Error('No topic rules found in sheet "' + CONFIG_.sourceSheetName + '"');
  }

  return {
    settings,
    timeZone,
    topicRules,
    verboseLogging
  };
}


function isBandwidthQuotaExceededError_(error) {
  const message = String(error && error.message ? error.message : error || '');
  return /Bandwidth quota exceeded/i.test(message);
}

function estimateBase64Bytes_(byteLength) {
  const normalized = Math.max(0, Number(byteLength || 0));
  if (!normalized) {
    return 0;
  }
  return Math.ceil(normalized / 3) * 4;
}

function estimateAttachmentInputBytes_(attachmentInput) {
  const bytes = attachmentInput && attachmentInput.blobData ? attachmentInput.blobData.bytes : null;
  const byteLength = Array.isArray(bytes) ? bytes.length : Number(bytes && bytes.length ? bytes.length : 0);
  return estimateBase64Bytes_(byteLength);
}

function estimateAttachmentBatchBytes_(batch) {
  let total = 0;
  for (let index = 0; index < batch.length; index++) {
    total += estimateAttachmentInputBytes_(batch[index]);
  }
  return total;
}

function buildSingleAttachmentBandwidthError_(error, attachmentInput, estimatedBytes) {
  const message = String(error && error.message ? error.message : error || '');
  const metadata = attachmentInput && attachmentInput.metadata ? attachmentInput.metadata : {};
  const blobData = attachmentInput && attachmentInput.blobData ? attachmentInput.blobData : {};
  return new Error(
    message +
    ' Single attachment still exceeds transfer limits: ' +
    String(metadata.attachment_name || '(unknown)') +
    ' [runDate=' + String(metadata.run_date || '') +
    ', rawFileKey=' + String(blobData.rawFileKey || '') +
    ', estimatedBase64Bytes=' + String(estimatedBytes || 0) + ']'
  );
}

function buildTursoExistingRunDatesRequest_(settings, startRunDate, endRunDate) {
  return buildTursoPipelineRequest_(
    settings,
    [
      buildTursoExecuteRequest_(
        'select run_date from pipeline_runs where run_date >= ? and run_date <= ? order by run_date asc',
        [startRunDate, endRunDate]
      ),
      { type: 'close' }
    ]
  );
}

function uploadAttachmentBatchWithAdaptiveSplit_(runtime, settings, batch, options) {
  const uploadOptions = options || {};
  const splitDepth = Math.max(0, Number(uploadOptions.splitDepth || 0));
  const estimatedBytes = estimateAttachmentBatchBytes_(batch);

  try {
    const response = fetchRequestWithRetry_(
      runtime.UrlFetchApp,
      buildTursoAttachmentBatchRequest_(settings, batch, {
        includeRunInit: Boolean(uploadOptions.includeRunInit)
      }),
      {
        maxAttempts: 3,
        retryableStatuses: [502, 503, 504]
      }
    );
    assertSuccessfulIngestResponse_(settings, response, 'Attachment ingest');
    if (uploadOptions.onSuccess) {
      uploadOptions.onSuccess({
        batch,
        estimatedBytes,
        splitDepth
      });
    }
    return;
  } catch (error) {
    if (!isBandwidthQuotaExceededError_(error)) {
      throw error;
    }
    if (batch.length <= 1) {
      throw buildSingleAttachmentBandwidthError_(error, batch[0], estimatedBytes);
    }

    const splitIndex = Math.ceil(batch.length / 2);
    const leftBatch = batch.slice(0, splitIndex);
    const rightBatch = batch.slice(splitIndex);
    if (uploadOptions.logProgress) {
      uploadOptions.logProgress('upload_batch_split', {
        runDate: uploadOptions.runDate,
        batchSize: batch.length,
        leftBatchSize: leftBatch.length,
        rightBatchSize: rightBatch.length,
        estimatedBytes,
        splitDepth,
        elapsedMs: elapsedMs_(uploadOptions.startedAtMs)
      });
    }

    uploadAttachmentBatchWithAdaptiveSplit_(runtime, settings, leftBatch, Object.assign({}, uploadOptions, {
      includeRunInit: Boolean(uploadOptions.includeRunInit),
      splitDepth: splitDepth + 1
    }));
    uploadAttachmentBatchWithAdaptiveSplit_(runtime, settings, rightBatch, Object.assign({}, uploadOptions, {
      includeRunInit: false,
      splitDepth: splitDepth + 1
    }));
  }
}

function getGmailHeaderValue_(headers, headerName) {
  const targetName = String(headerName || '').toLowerCase();
  const headerItems = Array.isArray(headers) ? headers : [];
  for (let index = 0; index < headerItems.length; index++) {
    const header = headerItems[index];
    if (String(header && header.name ? header.name : '').toLowerCase() === targetName) {
      return String(header && header.value ? header.value : '');
    }
  }
  return '';
}

function parseGmailMessageDate_(gmailMessage) {
  const internalDate = Number(gmailMessage && gmailMessage.internalDate ? gmailMessage.internalDate : 0);
  if (internalDate > 0) {
    return new Date(internalDate);
  }
  const dateHeader = getGmailHeaderValue_(
    gmailMessage && gmailMessage.payload ? gmailMessage.payload.headers : [],
    'Date'
  );
  const parsedDate = dateHeader ? new Date(dateHeader) : null;
  if (parsedDate && !isNaN(parsedDate.getTime())) {
    return parsedDate;
  }
  return new Date(0);
}

function decodeBase64WebSafeToBytes_(data) {
  const raw = String(data || '').trim();
  if (!raw) {
    return [];
  }
  if (typeof Utilities !== 'undefined' && Utilities.base64DecodeWebSafe) {
    return Utilities.base64DecodeWebSafe(raw);
  }
  if (typeof Buffer !== 'undefined') {
    return Array.from(Buffer.from(raw, 'base64url'));
  }
  throw new Error('No base64 web-safe decoder available in this runtime');
}

function listGmailMessagesForQuery_(gmailService, query) {
  const messages = [];
  let pageToken = null;

  do {
    const response = gmailService.Users.Messages.list('me', {
      q: query,
      maxResults: CONFIG_.searchBatchSize,
      pageToken: pageToken || undefined
    }) || {};
    const batch = Array.isArray(response.messages) ? response.messages : [];
    for (let index = 0; index < batch.length; index++) {
      messages.push(batch[index]);
    }
    pageToken = response.nextPageToken || null;
  } while (pageToken);

  return messages;
}

function collectGmailCandidates_(gmailService, query, topicRules, runDate, timeZone) {
  const messageRefs = listGmailMessagesForQuery_(gmailService, query);
  const candidates = [];

  for (let index = 0; index < messageRefs.length; index++) {
    const gmailMessage = gmailService.Users.Messages.get('me', messageRefs[index].id, {
      format: 'full'
    });
    const subject = String(
      getGmailHeaderValue_(gmailMessage && gmailMessage.payload ? gmailMessage.payload.headers : [], 'Subject') || ''
    ).trim();
    const messageDate = parseGmailMessageDate_(gmailMessage);
    const subjectReportDate = extractSubjectReportDate_(subject);
    const effectiveRunDate = inferEffectiveRunDate_(subject, messageDate, timeZone);
    if (effectiveRunDate !== runDate) {
      continue;
    }

    const matchedTopicRule = findMatchedTopicRule_(subject, topicRules);
    if (!matchedTopicRule) {
      continue;
    }

    candidates.push({
      effectiveRunDate,
      matchedTopic: matchedTopicRule.matchedTopic || matchedTopicRule.raw,
      primaryTopic: matchedTopicRule.primaryTopic || matchedTopicRule.raw,
      topicRole: matchedTopicRule.topicRole || 'primary',
      messageDate,
      messageId: gmailMessage.id,
      threadId: gmailMessage.threadId || '',
      subjectReportDate,
      subject,
      gmailMessage
    });
  }

  return {
    candidates,
    threadsScanned: messageRefs.length
  };
}

function collectGmailAttachmentParts_(payload, collectedParts) {
  const target = collectedParts || [];
  if (!payload || typeof payload !== 'object') {
    return target;
  }

  const filename = String(payload.filename || '').trim();
  const body = payload.body || {};
  if (filename && (body.attachmentId || body.data)) {
    target.push(payload);
  }

  const parts = Array.isArray(payload.parts) ? payload.parts : [];
  for (let index = 0; index < parts.length; index++) {
    collectGmailAttachmentParts_(parts[index], target);
  }

  return target;
}

function buildGmailApiAttachment_(runtime, messageId, part) {
  const body = part && part.body ? part.body : {};
  let encodedData = String(body.data || '').trim();
  if (!encodedData && body.attachmentId) {
    const attachmentResponse = runtime.Gmail.Users.Messages.Attachments.get('me', messageId, body.attachmentId) || {};
    encodedData = String(attachmentResponse.data || '').trim();
  }
  const bytes = decodeBase64WebSafeToBytes_(encodedData);
  const name = String(part && part.filename ? part.filename : '');
  const mimeType = String(part && part.mimeType ? part.mimeType : '');

  return {
    getName() {
      return name;
    },
    getContentType() {
      return mimeType;
    },
    copyBlob() {
      return {
        getBytes() {
          return bytes.slice();
        }
      };
    }
  };
}

function getCandidateAttachments_(runtime, candidate) {
  if (candidate && Array.isArray(candidate.attachments)) {
    return candidate.attachments.slice();
  }
  if (candidate && candidate.message && typeof candidate.message.getAttachments === 'function') {
    return candidate.message.getAttachments({
      includeAttachments: true,
      includeInlineImages: false
    });
  }

  const parts = collectGmailAttachmentParts_(candidate && candidate.gmailMessage ? candidate.gmailMessage.payload : null, []);
  const attachments = [];
  for (let index = 0; index < parts.length; index++) {
    attachments.push(buildGmailApiAttachment_(runtime, candidate.messageId, parts[index]));
  }
  return attachments;
}

function runForDate_(runtime, runDate, startedAtMs, runContext, options) {
  const context = runContext || buildRunContext_(runtime);
  const timeZone = context.timeZone;
  const topicRules = context.topicRules;
  const settings = context.settings;
  const runOptions = options || {};
  const query = runOptions.query || getMessageSearchQuery_(CONFIG_.runDayOffset);
  const runStartedPayload = {
    runDate,
    query,
    topicRules: topicRules.length,
    elapsedMs: elapsedMs_(startedAtMs)
  };
  if (context.verboseLogging) {
    runStartedPayload.topicsPreview = topicRules.map((rule) => rule.raw).slice(0, 20);
  }
  logProgress_('run_started', runStartedPayload);

  const discoveryResult = runOptions.preloadedCandidates
    ? null
    : collectGmailCandidates_(runtime.Gmail, query, topicRules, runDate, timeZone);
  const threadsScanned = runOptions.preloadedThreadsCount != null
    ? Number(runOptions.preloadedThreadsCount)
    : Number(discoveryResult && discoveryResult.threadsScanned ? discoveryResult.threadsScanned : 0);
  logProgress_('threads_loaded', {
    runDate,
    threadsScanned,
    elapsedMs: elapsedMs_(startedAtMs)
  });

  const allCandidates = runOptions.preloadedCandidates
    ? runOptions.preloadedCandidates.slice()
    : discoveryResult.candidates.slice();
  const candidates = allCandidates.slice();

  const stats = {
    runDate,
    topicRules: topicRules.length,
    threadsScanned,
    matchedMessagesBeforeLatestFilter: allCandidates.length,
    matchedMessages: candidates.length,
    attachmentsSeen: 0,
    attachmentsSent: 0,
    duplicateAttachmentsSkipped: 0,
    attachmentsSkippedExisting: 0,
    uploadBatches: 0,
    resetResponse: null
  };

  const candidatesSelectedPayload = {
    runDate,
    matchedMessagesBeforeLatestFilter: allCandidates.length,
    matchedMessages: candidates.length,
    elapsedMs: elapsedMs_(startedAtMs)
  };
  if (context.verboseLogging) {
    candidatesSelectedPayload.matchedTopicsBeforeLatestFilter = uniqueValues_(
      allCandidates.map((candidate) => candidate.matchedTopic)
    );
    candidatesSelectedPayload.matchedTopics = uniqueValues_(
      candidates.map((candidate) => candidate.matchedTopic)
    );
    candidatesSelectedPayload.candidateSubjects = candidates.map((candidate) => candidate.subject);
  }
  logProgress_('candidates_selected', candidatesSelectedPayload);

  const attachmentInputs = [];
  const unsupportedAttachments = [];

  for (let i = 0; i < candidates.length; i++) {
    const candidate = candidates[i];
    const attachments = getCandidateAttachments_(runtime, candidate);

    for (let j = 0; j < attachments.length; j++) {
      const attachment = attachments[j];
      const attachmentType = detectAttachmentType_(attachment);
      if (!attachmentType) {
        unsupportedAttachments.push({
          matchedTopic: candidate.matchedTopic,
          subject: candidate.subject,
          attachmentName: attachment.getName(),
          contentType: attachment.getContentType ? attachment.getContentType() : ''
        });
        continue;
      }

      stats.attachmentsSeen++;
      attachmentInputs.push(buildPreparedAttachmentInput_(
        attachment,
        buildAttachmentMetadata_({
          runDate,
          primaryTopic: candidate.primaryTopic,
          matchedTopic: candidate.matchedTopic,
          topicRole: candidate.topicRole,
          subject: candidate.subject,
          messageDate: candidate.messageDate,
          messageId: candidate.messageId,
          threadId: candidate.threadId,
          attachmentName: attachment.getName(),
          attachmentType
        })
      ));
    }
  }

  const attachmentsCollectedPayload = {
    runDate,
    attachmentsSeen: stats.attachmentsSeen,
    elapsedMs: elapsedMs_(startedAtMs)
  };
  if (context.verboseLogging && unsupportedAttachments.length) {
    attachmentsCollectedPayload.unsupportedAttachments = unsupportedAttachments;
  }
  logProgress_('attachments_collected', attachmentsCollectedPayload);

  const uniqueAttachmentInputs = [];
  const seenRawFileKeys = {};
  for (let index = 0; index < attachmentInputs.length; index++) {
    const attachmentInput = attachmentInputs[index];
    const rawFileKey = attachmentInput.blobData && attachmentInput.blobData.rawFileKey
      ? attachmentInput.blobData.rawFileKey
      : '';
    if (rawFileKey && seenRawFileKeys[rawFileKey]) {
      stats.duplicateAttachmentsSkipped++;
      continue;
    }
    if (rawFileKey) {
      seenRawFileKeys[rawFileKey] = true;
    }
    uniqueAttachmentInputs.push(attachmentInput);
  }

  const existingRawFileKeys = fetchExistingTursoRawFileKeys_(
    runtime.UrlFetchApp,
    settings,
    uniqueAttachmentInputs.map((attachmentInput) => attachmentInput.blobData.rawFileKey)
  );
  const uploadInputs = [];
  for (let index = 0; index < uniqueAttachmentInputs.length; index++) {
    const attachmentInput = uniqueAttachmentInputs[index];
    if (existingRawFileKeys[attachmentInput.blobData.rawFileKey]) {
      stats.attachmentsSkippedExisting++;
      continue;
    }
    uploadInputs.push(attachmentInput);
  }

  if (stats.duplicateAttachmentsSkipped || stats.attachmentsSkippedExisting) {
    logProgress_('attachments_filtered', {
      runDate,
      duplicateAttachmentsSkipped: stats.duplicateAttachmentsSkipped,
      attachmentsSkippedExisting: stats.attachmentsSkippedExisting,
      attachmentsReadyForUpload: uploadInputs.length,
      elapsedMs: elapsedMs_(startedAtMs)
    });
  }

  const requestBatches = chunkItems_(uploadInputs, 6);
  for (let batchIndex = 0; batchIndex < requestBatches.length; batchIndex++) {
    const batch = requestBatches[batchIndex];
    uploadAttachmentBatchWithAdaptiveSplit_(runtime, settings, batch, {
      includeRunInit: batchIndex === 0,
      runDate,
      startedAtMs,
      splitDepth: 0,
      logProgress: logProgress_,
      onSuccess(batchResult) {
        stats.uploadBatches++;
        stats.attachmentsSent += batchResult.batch.length;
        logProgress_('upload_batch_complete', {
          runDate,
          batchIndex: batchIndex + 1,
          batchCount: requestBatches.length,
          batchSize: batchResult.batch.length,
          attachmentsSent: stats.attachmentsSent,
          estimatedBytes: batchResult.estimatedBytes,
          splitDepth: batchResult.splitDepth,
          elapsedMs: elapsedMs_(startedAtMs)
        });
      }
    });
  }

  logProgress_('run_finished', Object.assign({}, stats, {
    totalElapsedMs: elapsedMs_(startedAtMs)
  }));

  return stats;
}

function finalizeMonthBackfillSummary_(summary) {
  const result = Object.assign({
    processedDates: [],
    successfulDates: [],
    noCandidateDates: [],
    skippedExistingDates: [],
    failedDates: [],
    dateResults: []
  }, summary || {});

  result.processedCount = result.processedDates.length;
  result.successfulCount = result.successfulDates.length;
  result.noCandidateCount = result.noCandidateDates.length;
  result.skippedCount = result.skippedExistingDates.length;
  result.failedCount = result.failedDates.length;
  return result;
}

function run() {
  return runMonthBackfill();
}

function runMonthBackfill() {
  const runtime = getAppsScriptRuntime_();
  const runContext = buildRunContext_(runtime);
  const targetRunDate = formatRunDate_(new Date(), runContext.timeZone);
  const runDates = listMonthRunDates_(targetRunDate);
  const backfillSettings = getBackfillSettings_(runtime.PropertiesService);
  const startedAtMs = Date.now();
  const summary = {
    targetRunDate,
    totalDates: runDates.length,
    threadsScanned: 0,
    processedDates: [],
    successfulDates: [],
    noCandidateDates: [],
    skippedExistingDates: [],
    failedDates: [],
    dateResults: []
  };
  const existingRunDates = backfillSettings.skipExistingEnabled
    ? fetchExistingRunDates_(runtime.UrlFetchApp, backfillSettings, runDates[0], targetRunDate)
    : {};
  const missingRunDates = runDates.filter((runDate) => !existingRunDates[runDate]);

  logProgress_('month_backfill_started', {
    targetRunDate,
    runDates,
    missingRunDates,
    queryMode: 'per_day_missing_only',
    threadsScanned: summary.threadsScanned,
    skipExistingEnabled: backfillSettings.skipExistingEnabled,
    elapsedMs: elapsedMs_(startedAtMs)
  });

  for (let i = 0; i < runDates.length; i++) {
    const runDate = runDates[i];
    if (backfillSettings.skipExistingEnabled && existingRunDates[runDate]) {
      summary.skippedExistingDates.push(runDate);
      summary.dateResults.push({
        runDate,
        status: 'skipped_existing'
      });
      logProgress_('month_backfill_skipped_existing', {
        runDate,
        elapsedMs: elapsedMs_(startedAtMs)
      });
      continue;
    }

    try {
      const runQuery = getRunDateSearchQuery_(runDate);
      const runStats = runForDate_(runtime, runDate, startedAtMs, runContext, {
        query: runQuery
      });
      summary.threadsScanned += Number(runStats.threadsScanned || 0);
      if (!runStats.matchedMessagesBeforeLatestFilter) {
        summary.processedDates.push(runDate);
        summary.noCandidateDates.push(runDate);
        summary.dateResults.push({
          runDate,
          status: 'no_candidates'
        });
        logProgress_('month_backfill_no_candidates', {
          runDate,
          elapsedMs: elapsedMs_(startedAtMs)
        });
        continue;
      }
      summary.processedDates.push(runDate);
      summary.successfulDates.push(runDate);
      summary.dateResults.push({
        runDate,
        status: 'processed',
        matchedMessages: runStats.matchedMessages,
        attachmentsSeen: runStats.attachmentsSeen,
        attachmentsSent: runStats.attachmentsSent,
        attachmentsSkippedExisting: runStats.attachmentsSkippedExisting,
        duplicateAttachmentsSkipped: runStats.duplicateAttachmentsSkipped,
        uploadBatches: runStats.uploadBatches
      });
    } catch (error) {
      summary.failedDates.push({
        runDate,
        error: error && error.message ? error.message : String(error)
      });
      summary.dateResults.push({
        runDate,
        status: 'failed',
        error: error && error.message ? error.message : String(error)
      });
      logProgress_('month_backfill_failed', {
        runDate,
        error: error && error.message ? error.message : String(error),
        elapsedMs: elapsedMs_(startedAtMs)
      });
    }
  }

  logProgress_('month_backfill_finished', Object.assign({}, finalizeMonthBackfillSummary_(summary), {
    totalElapsedMs: elapsedMs_(startedAtMs)
  }));

  return finalizeMonthBackfillSummary_(summary);
}
