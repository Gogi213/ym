const CONFIG_ = {
  sourceSpreadsheetId: '17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA',
  sourceSheetName: 'отчеты',
  sourceColumn: 1,
  sourceSecondaryColumn: 2,
  ingestBaseUrlProperty: 'INGEST_BASE_URL',
  ingestTokenProperty: 'INGEST_TOKEN',
  ingestStatusUrlProperty: 'INGEST_STATUS_URL',
  supabaseFunctionUrlProperty: 'SUPABASE_FUNCTION_URL',
  supabaseIngestTokenProperty: 'SUPABASE_INGEST_TOKEN',
  supabaseRestUrlProperty: 'SUPABASE_REST_URL',
  supabaseServiceRoleKeyProperty: 'SUPABASE_SERVICE_ROLE_KEY',
  verboseLoggingProperty: 'VERBOSE_LOGGING',
  supabaseFunctionUrl: 'https://jchvqvuudclgodsrhctb.supabase.co/functions/v1/mail-ingest',
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

