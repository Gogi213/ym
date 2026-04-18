
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
  return Object.assign({ skipExistingEnabled: false }, getScriptSettings_(propertiesService));
}

function postReset_(urlFetchApp, settings, runDate) {
  return postTursoReset_(urlFetchApp, settings, runDate);
}

function fetchRunDateExists_(urlFetchApp, settings, runDate) {
  return fetchTursoRunDateExists_(urlFetchApp, settings, runDate);
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
