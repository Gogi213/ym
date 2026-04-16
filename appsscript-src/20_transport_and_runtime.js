
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

function buildIngestStatusRequest_(settings, runDate) {
  return {
    url: String(settings.statusUrl || '').replace(/\/+$/, '') + '/' + encodeURIComponent(String(runDate || '')),
    method: 'get',
    headers: {
      'x-ingest-token': settings.ingestToken
    },
    muteHttpExceptions: true
  };
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

function normalizeIngestStatusBaseUrl_(statusUrl, ingestBaseUrl) {
  const explicitStatusUrl = String(statusUrl || '').trim().replace(/\/+$/, '');
  if (explicitStatusUrl) {
    return /\/pipeline-runs$/i.test(explicitStatusUrl)
      ? explicitStatusUrl
      : explicitStatusUrl + '/pipeline-runs';
  }

  const explicitIngestBaseUrl = String(ingestBaseUrl || '').trim().replace(/\/+$/, '');
  return explicitIngestBaseUrl ? explicitIngestBaseUrl + '/pipeline-runs' : '';
}

function isTransientHttpStatus_(responseCode) {
  return responseCode === 502 || responseCode === 503 || responseCode === 504;
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
  const ingestBaseUrl = String(scriptProperties.getProperty(CONFIG_.ingestBaseUrlProperty) || '').trim();
  const functionUrl = ingestBaseUrl
    ? ingestBaseUrl.replace(/\/+$/, '') + '/ingest'
    : resolveSettingValue_(
        scriptProperties.getProperty(CONFIG_.supabaseFunctionUrlProperty),
        CONFIG_.supabaseFunctionUrl,
        CONFIG_.supabaseFunctionUrlProperty
      );
  const resetUrl = ingestBaseUrl
    ? ingestBaseUrl.replace(/\/+$/, '') + '/reset'
    : functionUrl;
  const ingestToken = resolveSettingValue_(
    scriptProperties.getProperty(CONFIG_.ingestTokenProperty),
    scriptProperties.getProperty(CONFIG_.supabaseIngestTokenProperty),
    CONFIG_.ingestTokenProperty
  );

  return { functionUrl, resetUrl, ingestToken };
}

function getBackfillSettings_(propertiesService) {
  const scriptProperties = propertiesService.getScriptProperties();
  const ingestBaseUrl = String(scriptProperties.getProperty(CONFIG_.ingestBaseUrlProperty) || '').trim();
  const ingestStatusUrl = String(scriptProperties.getProperty(CONFIG_.ingestStatusUrlProperty) || '').trim();
  const functionUrl = ingestBaseUrl
    ? ingestBaseUrl.replace(/\/+$/, '') + '/ingest'
    : resolveSettingValue_(
        scriptProperties.getProperty(CONFIG_.supabaseFunctionUrlProperty),
        CONFIG_.supabaseFunctionUrl,
        CONFIG_.supabaseFunctionUrlProperty
      );
  const restUrl = ingestBaseUrl
    ? ''
    : resolveSettingValue_(
        scriptProperties.getProperty(CONFIG_.supabaseRestUrlProperty),
        functionUrl.replace(/\/functions\/v1\/[^/]+$/, '/rest/v1'),
        CONFIG_.supabaseRestUrlProperty
      );
  const serviceRoleKey = String(
    scriptProperties.getProperty(CONFIG_.supabaseServiceRoleKeyProperty) || ''
  ).trim();
  const ingestToken = String(
    scriptProperties.getProperty(CONFIG_.ingestTokenProperty)
      || scriptProperties.getProperty(CONFIG_.supabaseIngestTokenProperty)
      || ''
  ).trim();
  const statusUrl = normalizeIngestStatusBaseUrl_(ingestStatusUrl, ingestBaseUrl);

  return {
    statusUrl,
    ingestToken,
    restUrl,
    serviceRoleKey,
    skipExistingEnabled: Boolean(statusUrl || serviceRoleKey)
  };
}

function postReset_(urlFetchApp, settings, runDate) {
  return assertSuccessfulResponse_(
    urlFetchApp.fetch(settings.resetUrl, {
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

function fetchRunDateExists_(urlFetchApp, settings, runDate) {
  if (settings.statusUrl) {
    const request = buildIngestStatusRequest_(settings, runDate);
    for (let attempt = 0; attempt < 3; attempt++) {
      const response = fetchRequest_(urlFetchApp, request);
      const parsed = parseJsonResponse_(response);

      if (parsed.responseCode >= 200 && parsed.responseCode < 300) {
        return Boolean(parsed.json && parsed.json.exists);
      }

      if (isTransientHttpStatus_(parsed.responseCode) && attempt < 2) {
        sleepMs_((attempt + 1) * 1500);
        continue;
      }

      throw new Error(
        'Run date existence check failed with HTTP ' + parsed.responseCode + ': ' + parsed.body
      );
    }
  }

  const request = buildSupabaseSelectRequest_(
    settings,
    'ingest_files',
    buildRunDateExistsQuery_(runDate)
  );
  const response = assertSuccessfulResponse_(
    fetchRequest_(urlFetchApp, request),
    'Run date existence check'
  );

  return Array.isArray(response.json) && response.json.length > 0;
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
