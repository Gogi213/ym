
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
    stats.uploadBatches++;

    for (let responseIndex = 0; responseIndex < batch.length; responseIndex++) {
      const response = fetchRequestWithRetry_(runtime.UrlFetchApp, batch[responseIndex], {
        maxAttempts: 3,
        retryableStatuses: [502, 503, 504]
      });
      assertSuccessfulResponse_(response, 'Attachment ingest');
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
