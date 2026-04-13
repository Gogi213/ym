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
      const effectiveRunDate = subjectReportDate || formatRunDate_(message.getDate(), timeZone);

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
      const effectiveRunDate = subjectReportDate || formatRunDate_(message.getDate(), timeZone);
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
