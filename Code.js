const CONFIG_ = {
  sourceSpreadsheetId: '17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA',
  sourceSheetName: 'отчеты',
  sourceColumn: 1,
  supabaseFunctionUrlProperty: 'SUPABASE_FUNCTION_URL',
  supabaseIngestTokenProperty: 'SUPABASE_INGEST_TOKEN',
  supabaseRestUrlProperty: 'SUPABASE_REST_URL',
  supabaseServiceRoleKeyProperty: 'SUPABASE_SERVICE_ROLE_KEY',
  verboseLoggingProperty: 'VERBOSE_LOGGING',
  supabaseFunctionUrl: 'https://jchvqvuudclgodsrhctb.supabase.co/functions/v1/mail-ingest',
  supabaseIngestToken: '4EYKvpGLVNIyvyGiHxZtiCE6i9fPOH1kGlkJRAp6bZKjZEmgGeREg/JPPEAhs0ft',
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

  for (let i = 0; i < values.length; i++) {
    const raw = String(values[i] && values[i][0] ? values[i][0] : '').trim();
    if (!raw) {
      continue;
    }

    const tokens = tokenizeTopic_(raw);
    if (!tokens.length) {
      continue;
    }

    rules.push({ raw, tokens });
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

function findMatchedTopic_(subject, topicRules) {
  const normalizedMatchTarget = normalizeText_(extractSubjectBody_(subject) || subject);
  const compactMatchTarget = compactText_(extractSubjectBody_(subject) || subject);

  for (let i = 0; i < topicRules.length; i++) {
    const topicRule = topicRules[i];
    const normalizedTopic = normalizeText_(topicRule.raw);
    if (!normalizedTopic) {
      continue;
    }

    if (normalizedMatchTarget.indexOf(normalizedTopic) !== -1) {
      return topicRule.raw;
    }

    const compactTopic = compactText_(topicRule.raw);
    if (compactTopic && compactMatchTarget.indexOf(compactTopic) !== -1) {
      return topicRule.raw;
    }
  }

  return null;
}

function subjectMatchesTopics_(subject, topicRules) {
  return findMatchedTopic_(subject, topicRules) !== null;
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
    sheet.getRange(2, CONFIG_.sourceColumn, lastRow - 1, 1).getDisplayValues()
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
      const effectiveRunDate = subjectReportDate || formatRunDate_(message.getDate(), timeZone);

      if (effectiveRunDate !== runDate) {
        continue;
      }

      const matchedTopic = findMatchedTopic_(subject, topicRules);
      if (!matchedTopic) {
        continue;
      }

      candidates.push({
        effectiveRunDate,
        matchedTopic,
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
      const effectiveRunDate = subjectReportDate || formatRunDate_(message.getDate(), timeZone);
      const matchedTopic = findMatchedTopic_(subject, topicRules);

      if (!matchedTopic) {
        continue;
      }

      if (!grouped[effectiveRunDate]) {
        grouped[effectiveRunDate] = [];
      }

      grouped[effectiveRunDate].push({
        effectiveRunDate,
        matchedTopic,
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
    matched_topic: input.matchedTopic,
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
  return {
    url: settings.functionUrl,
    method: 'post',
    headers: {
      'x-ingest-token': settings.ingestToken
    },
    muteHttpExceptions: true,
    payload: {
      meta: JSON.stringify(metadata),
      file: attachment.copyBlob().setName(metadata.attachment_name)
    }
  };
}

function buildSupabaseSelectRequest_(settings, relationName, queryString) {
  return {
    url: settings.restUrl + '/' + relationName + '?' + queryString,
    method: 'get',
    headers: {
      apikey: settings.serviceRoleKey,
      Authorization: 'Bearer ' + settings.serviceRoleKey
    },
    muteHttpExceptions: true
  };
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

function getScriptSettings_(propertiesService) {
  const scriptProperties = propertiesService.getScriptProperties();
  const functionUrl = resolveSettingValue_(
    scriptProperties.getProperty(CONFIG_.supabaseFunctionUrlProperty),
    CONFIG_.supabaseFunctionUrl,
    CONFIG_.supabaseFunctionUrlProperty
  );
  const ingestToken = resolveSettingValue_(
    scriptProperties.getProperty(CONFIG_.supabaseIngestTokenProperty),
    CONFIG_.supabaseIngestToken,
    CONFIG_.supabaseIngestTokenProperty
  );

  return { functionUrl, ingestToken };
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

function getBackfillSettings_(propertiesService) {
  const scriptProperties = propertiesService.getScriptProperties();
  const functionUrl = resolveSettingValue_(
    scriptProperties.getProperty(CONFIG_.supabaseFunctionUrlProperty),
    CONFIG_.supabaseFunctionUrl,
    CONFIG_.supabaseFunctionUrlProperty
  );
  const restUrl = resolveSettingValue_(
    scriptProperties.getProperty(CONFIG_.supabaseRestUrlProperty),
    functionUrl.replace(/\/functions\/v1\/[^/]+$/, '/rest/v1'),
    CONFIG_.supabaseRestUrlProperty
  );
  const serviceRoleKey = String(
    scriptProperties.getProperty(CONFIG_.supabaseServiceRoleKeyProperty) || ''
  ).trim();

  return {
    restUrl,
    serviceRoleKey,
    skipExistingEnabled: Boolean(serviceRoleKey)
  };
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

function postReset_(urlFetchApp, settings, runDate) {
  return assertSuccessfulResponse_(
    urlFetchApp.fetch(settings.functionUrl, {
      method: 'post',
      contentType: 'application/json',
      headers: {
        'x-ingest-token': settings.ingestToken
      },
      muteHttpExceptions: true,
      payload: JSON.stringify(buildResetPayload_(runDate))
    }),
    'Reset request'
  );
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
    typeof GmailApp === 'undefined' ||
    typeof PropertiesService === 'undefined' ||
    typeof SpreadsheetApp === 'undefined' ||
    typeof Session === 'undefined' ||
    typeof UrlFetchApp === 'undefined'
  ) {
    throw new Error('Apps Script runtime globals are unavailable');
  }

  return {
    GmailApp,
    PropertiesService,
    Session,
    SpreadsheetApp,
    UrlFetchApp
  };
}

function fetchRunDateExists_(urlFetchApp, settings, runDate) {
  const response = assertSuccessfulResponse_(
    urlFetchApp.fetch(
      buildSupabaseSelectRequest_(
        settings,
        'ingest_files',
        buildRunDateExistsQuery_(runDate)
      )
    ),
    'Run date existence check'
  );

  return Array.isArray(response.json) && response.json.length > 0;
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

  const threads = runOptions.preloadedCandidates
    ? null
    : listThreadsForQuery_(runtime.GmailApp, query);
  const threadsScanned = runOptions.preloadedThreadsCount != null
    ? Number(runOptions.preloadedThreadsCount)
    : threads.length;
  logProgress_('threads_loaded', {
    runDate,
    threadsScanned,
    elapsedMs: elapsedMs_(startedAtMs)
  });

  const allCandidates = runOptions.preloadedCandidates
    ? runOptions.preloadedCandidates.slice()
    : collectCandidateMessages_(threads, topicRules, runDate, timeZone);
  const candidates = markLatestMessagesByTopic_(allCandidates)
    .filter((candidate) => candidate.isLatestForTopic);

  const stats = {
    runDate,
    topicRules: topicRules.length,
    threadsScanned,
    matchedMessagesBeforeLatestFilter: allCandidates.length,
    matchedMessages: candidates.length,
    attachmentsSeen: 0,
    attachmentsSent: 0,
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

  const resetResponse = postReset_(runtime.UrlFetchApp, settings, runDate);
  stats.resetResponse = resetResponse.json || resetResponse.body;
  logProgress_('reset_complete', {
    runDate,
    resetResponse: stats.resetResponse,
    elapsedMs: elapsedMs_(startedAtMs)
  });

  const attachmentRequests = [];
  const unsupportedAttachments = [];

  for (let i = 0; i < candidates.length; i++) {
    const candidate = candidates[i];
    const attachments = candidate.message.getAttachments({
      includeAttachments: true,
      includeInlineImages: false
    });

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
      attachmentRequests.push(
        buildAttachmentRequest_(
          settings,
          attachment,
          buildAttachmentMetadata_({
            runDate,
            matchedTopic: candidate.matchedTopic,
            subject: candidate.subject,
            messageDate: candidate.messageDate,
            messageId: candidate.messageId,
            threadId: candidate.threadId,
            attachmentName: attachment.getName(),
            attachmentType
          })
        )
      );
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

  const requestBatches = chunkItems_(attachmentRequests, 10);
  for (let batchIndex = 0; batchIndex < requestBatches.length; batchIndex++) {
    const batch = requestBatches[batchIndex];
    const responses = runtime.UrlFetchApp.fetchAll(batch);
    stats.uploadBatches++;

    for (let responseIndex = 0; responseIndex < responses.length; responseIndex++) {
      assertSuccessfulResponse_(responses[responseIndex], 'Attachment ingest');
      stats.attachmentsSent++;
    }

    logProgress_('upload_batch_complete', {
      runDate,
      batchIndex: batchIndex + 1,
      batchCount: requestBatches.length,
      batchSize: batch.length,
      attachmentsSent: stats.attachmentsSent,
      elapsedMs: elapsedMs_(startedAtMs)
    });
  }

  logProgress_('run_finished', Object.assign({}, stats, {
    totalElapsedMs: elapsedMs_(startedAtMs)
  }));

  return stats;
}

function run() {
  const runtime = getAppsScriptRuntime_();
  const startedAt = new Date();
  const runContext = buildRunContext_(runtime);
  const runDate = resolveTargetRunDate_(startedAt, runContext.timeZone, CONFIG_.runDayOffset);
  return runForDate_(runtime, runDate, Date.now(), runContext);
}

function runMonthBackfill() {
  const runtime = getAppsScriptRuntime_();
  const runContext = buildRunContext_(runtime);
  const targetRunDate = formatRunDate_(new Date(), runContext.timeZone);
  const runDates = listMonthRunDates_(targetRunDate);
  const backfillSettings = getBackfillSettings_(runtime.PropertiesService);
  const startedAtMs = Date.now();
  const query = getMonthBackfillSearchQuery_(targetRunDate);
  const threads = listThreadsForQuery_(runtime.GmailApp, query);
  const candidatesByRunDate = buildCandidatesByRunDate_(
    threads,
    runContext.topicRules,
    runContext.timeZone
  );
  const summary = {
    targetRunDate,
    totalDates: runDates.length,
    threadsScanned: threads.length,
    processedDates: [],
    skippedExistingDates: [],
    failedDates: []
  };

  logProgress_('month_backfill_started', {
    targetRunDate,
    runDates,
    query,
    threadsScanned: threads.length,
    skipExistingEnabled: backfillSettings.skipExistingEnabled,
    elapsedMs: elapsedMs_(startedAtMs)
  });

  if (!backfillSettings.skipExistingEnabled) {
    logProgress_('month_backfill_skip_check_disabled', {
      reason: 'SUPABASE_SERVICE_ROLE_KEY is not configured',
      elapsedMs: elapsedMs_(startedAtMs)
    });
  }

  for (let i = 0; i < runDates.length; i++) {
    const runDate = runDates[i];
    if (
      backfillSettings.skipExistingEnabled &&
      fetchRunDateExists_(runtime.UrlFetchApp, backfillSettings, runDate)
    ) {
      summary.skippedExistingDates.push(runDate);
      logProgress_('month_backfill_skipped_existing', {
        runDate,
        elapsedMs: elapsedMs_(startedAtMs)
      });
      continue;
    }

    try {
      const dayCandidates = candidatesByRunDate[runDate] || [];
      if (!dayCandidates.length) {
        summary.processedDates.push(runDate);
        logProgress_('month_backfill_no_candidates', {
          runDate,
          elapsedMs: elapsedMs_(startedAtMs)
        });
        continue;
      }

      runForDate_(runtime, runDate, startedAtMs, runContext, {
        query,
        preloadedThreadsCount: threads.length,
        preloadedCandidates: dayCandidates
      });
      summary.processedDates.push(runDate);
    } catch (error) {
      summary.failedDates.push({
        runDate,
        error: error && error.message ? error.message : String(error)
      });
      logProgress_('month_backfill_failed', {
        runDate,
        error: error && error.message ? error.message : String(error),
        elapsedMs: elapsedMs_(startedAtMs)
      });
    }
  }

  logProgress_('month_backfill_finished', Object.assign({}, summary, {
    totalElapsedMs: elapsedMs_(startedAtMs)
  }));

  return summary;
}
