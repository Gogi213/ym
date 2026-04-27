
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
