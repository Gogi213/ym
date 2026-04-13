
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
    '',
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
