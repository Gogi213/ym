
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
