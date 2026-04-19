const test = require('node:test');
const assert = require('node:assert/strict');
const ingest = require('./load_code.js');

test('exports core ingest helpers', () => {
  assert.equal(typeof ingest.normalizeText_, 'function');
  assert.equal(typeof ingest.buildResetPayload_, 'function');
  assert.equal(typeof ingest.buildAttachmentMetadata_, 'function');
});

test('normalizeText_ lowercases, removes punctuation, and normalizes yo', () => {
  assert.equal(
    ingest.normalizeText_('  Ёжик, UTM-report!!! \n'),
    'ежик utm report'
  );
});

test('loadTopicRulesFromValues_ tokenizes non-empty topic rows', () => {
  const rules = ingest.loadTopicRulesFromValues_([
    [' Weekly Report ', 'Weekly Report Conversions'],
    [''],
    ['utm source']
  ]);

  assert.deepEqual(rules, [
    {
      raw: 'Weekly Report',
      matchedTopic: 'Weekly Report',
      primaryTopic: 'Weekly Report',
      topicRole: 'primary',
      tokens: ['weekly', 'report']
    },
    {
      raw: 'Weekly Report Conversions',
      matchedTopic: 'Weekly Report Conversions',
      primaryTopic: 'Weekly Report',
      topicRole: 'secondary',
      tokens: ['weekly', 'report', 'conversions']
    },
    {
      raw: 'utm source',
      matchedTopic: 'utm source',
      primaryTopic: 'utm source',
      topicRole: 'primary',
      tokens: ['utm', 'source']
    }
  ]);
});

test('loadTopicRulesFromSpreadsheet_ reads topics starting from row 2 and ignores A1 header', () => {
  const values = [
    ['Тема письма', 'Конверсии'],
    ['Abbott / Heptral / 2026 / Solta', 'Abbott / Heptral / 2026 / Solta / конверсии'],
    [''],
    ['TW // Назонекс Аллерджи // Solta', '']
  ];

  const sheet = {
    getLastRow() {
      return values.length;
    },
    getRange(row, column, numRows, numCols) {
      assert.equal(row, 2);
      assert.equal(column, 1);
      assert.equal(numRows, 3);
      assert.equal(numCols, 2);
      return {
        getDisplayValues() {
          return values.slice(1);
        }
      };
    }
  };

  const spreadsheet = {
    getSheetByName(name) {
      assert.equal(name, 'отчеты');
      return sheet;
    }
  };

  assert.deepEqual(
    ingest.loadTopicRulesFromSpreadsheet_(spreadsheet),
    [
      {
        raw: 'Abbott / Heptral / 2026 / Solta',
        matchedTopic: 'Abbott / Heptral / 2026 / Solta',
        primaryTopic: 'Abbott / Heptral / 2026 / Solta',
        topicRole: 'primary',
        tokens: ['abbott', 'heptral', '2026', 'solta']
      },
      {
        raw: 'Abbott / Heptral / 2026 / Solta / конверсии',
        matchedTopic: 'Abbott / Heptral / 2026 / Solta / конверсии',
        primaryTopic: 'Abbott / Heptral / 2026 / Solta',
        topicRole: 'secondary',
        tokens: ['abbott', 'heptral', '2026', 'solta', 'конверсии']
      },
      {
        raw: 'TW // Назонекс Аллерджи // Solta',
        matchedTopic: 'TW // Назонекс Аллерджи // Solta',
        primaryTopic: 'TW // Назонекс Аллерджи // Solta',
        topicRole: 'primary',
        tokens: ['tw', 'назонекс', 'аллерджи', 'solta']
      }
    ]
  );
});

test('subjectMatchesTopics_ matches by partial body substring, not scattered tokens', () => {
  const rules = [
    { raw: 'Femina / 2026 / Solta', tokens: ['femina', '2026', 'solta'] }
  ];

  assert.equal(
    ingest.subjectMatchesTopics_('Client One weekly report 2026-04-07', rules),
    false
  );
  assert.equal(
    ingest.subjectMatchesTopics_(
      'Отчёт «Abbott / Femina / 2026 / Solta / Banners» за 05.04.2026',
      rules
    ),
    true
  );
  assert.equal(
    ingest.subjectMatchesTopics_(
      'Отчёт «Abbott / Solta / 2026 / Femina / Banners» за 05.04.2026',
      rules
    ),
    false
  );
});

test('findMatchedTopic_ matches against subject body and ignores generic prefix text', () => {
  const rules = [
    { raw: 'Отчёт', tokens: ['отчет'] },
    { raw: 'Abbott / Femina / 2026 / Solta / Banners', tokens: ['abbott'] }
  ];

  assert.equal(
    ingest.findMatchedTopic_(
      'Отчёт «Abbott / Femina / 2026 / Solta / Banners» за 05.04.2026',
      rules
    ),
    'Abbott / Femina / 2026 / Solta / Banners'
  );
});

test('findMatchedTopic_ ignores decorative underscores in topic rules', () => {
  const rules = [
    { raw: '_SenSoy_', tokens: ['sensoy'] }
  ];

  assert.equal(
    ingest.findMatchedTopic_(
      'Отчёт «SenSoy» за 10.04.2026',
      rules
    ),
    '_SenSoy_'
  );
});

test('findMatchedTopic_ matches compact form when subject spacing differs from rule', () => {
  const rules = [
    { raw: '_SenSoy_', tokens: ['sensoy'] }
  ];

  assert.equal(
    ingest.findMatchedTopic_(
      'Отчёт «Sen Soy» за 11.04.2026',
      rules
    ),
    '_SenSoy_'
  );
});

test('extractSubjectReportDate_ parses ru date from subject', () => {
  assert.equal(
    ingest.extractSubjectReportDate_('Отчёт «_SenSoy_» за 11.04.2026'),
    '2026-04-11'
  );
});

test('collectCandidateMessages_ prefers subject report date over message date', () => {
  const message = {
    getDate() {
      return new Date('2026-04-12T01:00:00Z');
    },
    getSubject() {
      return 'Отчёт «_SenSoy_» за 11.04.2026';
    },
    getId() {
      return 'msg-1';
    }
  };
  const thread = {
    getMessages() {
      return [message];
    },
    getId() {
      return 'thr-1';
    }
  };

  const result = ingest.collectCandidateMessages_(
    [thread],
    [{ raw: '_SenSoy_', tokens: ['sensoy'] }],
    '2026-04-11',
    'Asia/Tbilisi'
  );

  assert.equal(result.length, 1);
  assert.equal(result[0].matchedTopic, '_SenSoy_');
  assert.equal(result[0].primaryTopic, '_SenSoy_');
  assert.equal(result[0].topicRole, 'primary');
});

test('buildCandidatesByRunDate_ groups matched messages by effective run date', () => {
  const messages = [
    {
      getDate() {
        return new Date('2026-04-12T01:00:00Z');
      },
      getSubject() {
        return 'Отчёт «_SenSoy_» за 11.04.2026';
      },
      getId() {
        return 'msg-1';
      }
    },
    {
      getDate() {
        return new Date('2026-04-12T02:00:00Z');
      },
      getSubject() {
        return 'Отчёт «TW // Назонекс Аллерджи // Solta» за 10.04.2026';
      },
      getId() {
        return 'msg-2';
      }
    }
  ];
  const thread = {
    getMessages() {
      return messages;
    },
    getId() {
      return 'thr-1';
    }
  };

  const grouped = ingest.buildCandidatesByRunDate_(
    [thread],
    [
      { raw: '_SenSoy_', tokens: ['sensoy'] },
      { raw: 'TW // Назонекс Аллерджи // Solta', tokens: ['tw', 'назонекс', 'аллерджи', 'solta'] }
    ],
    'Asia/Tbilisi'
  );

  assert.deepEqual(Object.keys(grouped).sort(), ['2026-04-10', '2026-04-11']);
  assert.equal(grouped['2026-04-11'].length, 1);
  assert.equal(grouped['2026-04-11'][0].matchedTopic, '_SenSoy_');
  assert.equal(grouped['2026-04-11'][0].primaryTopic, '_SenSoy_');
  assert.equal(grouped['2026-04-10'].length, 1);
  assert.equal(grouped['2026-04-10'][0].matchedTopic, 'TW // Назонекс Аллерджи // Solta');
  assert.equal(grouped['2026-04-10'][0].primaryTopic, 'TW // Назонекс Аллерджи // Solta');
});

test('formatRunDate_ formats date in target timezone', () => {
  assert.equal(
    ingest.formatRunDate_(new Date('2026-04-06T20:30:00Z'), 'Asia/Tbilisi'),
    '2026-04-07'
  );
});

test('resolveTargetRunDate_ applies day offset to target export date', () => {
  assert.equal(
    ingest.resolveTargetRunDate_(
      new Date('2026-04-07T08:15:00Z'),
      'Asia/Tbilisi',
      -1
    ),
    '2026-04-06'
  );
  assert.equal(
    ingest.resolveTargetRunDate_(
      new Date('2026-04-07T08:15:00Z'),
      'Asia/Tbilisi',
      0
    ),
    '2026-04-07'
  );
});

test('listMonthRunDates_ returns all dates from month start through target date', () => {
  assert.deepEqual(
    ingest.listMonthRunDates_('2026-04-12'),
    [
      '2026-04-01',
      '2026-04-02',
      '2026-04-03',
      '2026-04-04',
      '2026-04-05',
      '2026-04-06',
      '2026-04-07',
      '2026-04-08',
      '2026-04-09',
      '2026-04-10',
      '2026-04-11',
      '2026-04-12'
    ]
  );
});

test('buildRunDateExistsQuery_ shapes REST query for existing ingest day check', () => {
  assert.equal(
    ingest.buildRunDateExistsQuery_('2026-04-11'),
    'select=id&run_date=eq.2026-04-11&limit=1'
  );
});

test('getMessageSearchQuery_ uses a wider recent window than the target export day', () => {
  assert.equal(
    ingest.getMessageSearchQuery_(-1),
    'newer_than:3d has:attachment'
  );
  assert.equal(
    ingest.getMessageSearchQuery_(0),
    'newer_than:2d has:attachment'
  );
});

test('getMonthBackfillSearchQuery_ covers the full month-to-date window', () => {
  assert.equal(
    ingest.getMonthBackfillSearchQuery_('2026-04-12'),
    'after:2026/04/01 before:2026/04/13 has:attachment'
  );
  assert.equal(
    ingest.getMonthBackfillSearchQuery_('2026-04-01'),
    'after:2026/04/01 before:2026/04/02 has:attachment'
  );
});

test('detectAttachmentType_ keeps only xlsx and csv candidates', () => {
  assert.equal(
    ingest.detectAttachmentType_({
      getName: () => 'client.xlsx',
      getContentType: () => 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    }),
    'xlsx'
  );
  assert.equal(
    ingest.detectAttachmentType_({
      getName: () => 'client.csv',
      getContentType: () => 'text/csv'
    }),
    'csv'
  );
  assert.equal(
    ingest.detectAttachmentType_({
      getName: () => 'client.pdf',
      getContentType: () => 'application/pdf'
    }),
    null
  );
});

test('buildResetPayload_ shapes the once-per-run reset request', () => {
  assert.deepEqual(ingest.buildResetPayload_('2026-04-06'), {
    action: 'reset',
    run_date: '2026-04-06'
  });
});

test('buildAttachmentMetadata_ shapes multipart metadata for one attachment upload', () => {
  assert.deepEqual(
    ingest.buildAttachmentMetadata_({
      runDate: '2026-04-06',
      primaryTopic: 'weekly report',
      matchedTopic: 'weekly report',
      topicRole: 'primary',
      subject: 'Weekly report for Solta',
      messageDate: new Date('2026-04-06T09:30:00Z'),
      messageId: 'msg-1',
      threadId: 'thr-1',
      attachmentName: 'client.xlsx',
      attachmentType: 'xlsx'
    }),
    {
      action: 'ingest',
      run_date: '2026-04-06',
      primary_topic: 'weekly report',
      matched_topic: 'weekly report',
      topic_role: 'primary',
      message_subject: 'Weekly report for Solta',
      message_date: '2026-04-06T09:30:00.000Z',
      message_id: 'msg-1',
      thread_id: 'thr-1',
      attachment_name: 'client.xlsx',
      attachment_type: 'xlsx'
    }
  );
});

test('markLatestMessagesByTopic_ keeps only the latest message for each topic', () => {
  const result = ingest.markLatestMessagesByTopic_([
    { matchedTopic: 'topic-a', messageDate: new Date('2026-04-06T08:00:00Z'), id: 1 },
    { matchedTopic: 'topic-a', messageDate: new Date('2026-04-06T09:00:00Z'), id: 2 },
    { matchedTopic: 'topic-b', messageDate: new Date('2026-04-06T07:00:00Z'), id: 3 }
  ]);

  assert.deepEqual(
    result.map((item) => ({ id: item.id, isLatestForTopic: item.isLatestForTopic })),
    [
      { id: 1, isLatestForTopic: false },
      { id: 2, isLatestForTopic: true },
      { id: 3, isLatestForTopic: true }
    ]
  );
});

test('buildAttachmentRequest_ shapes one UrlFetchApp request for multipart upload', () => {
  const blob = {
    getBytes() {
      return [65, 66, 67];
    }
  };
  const attachment = {
    getContentType() {
      return 'text/csv';
    },
    copyBlob() {
      return blob;
    }
  };

  const request = ingest.buildAttachmentRequest_(
    {
      mode: 'turso',
      pipelineUrl: 'https://example.turso.io/v2/pipeline',
      authToken: 'secret-token'
    },
    attachment,
    {
      run_date: '2026-04-06',
      primary_topic: 'weekly report',
      matched_topic: 'weekly report',
      topic_role: 'primary',
      message_subject: 'Weekly report for Solta',
      message_date: '2026-04-06T09:30:00.000Z',
      message_id: 'msg-1',
      thread_id: 'thr-1',
      attachment_name: 'client.xlsx',
      attachment_type: 'csv',
      action: 'ingest'
    }
  );

  assert.equal(request.url, 'https://example.turso.io/v2/pipeline');
  assert.equal(request.method, 'post');
  assert.equal(request.contentType, 'application/json');
  assert.equal(request.headers.Authorization, 'Bearer secret-token');
  assert.match(request.payload, /insert into ingest_files/i);
  assert.match(request.payload, /insert into ingest_file_payloads/i);
  assert.match(request.payload, /'raw_only'/i);
  assert.match(request.payload, /raw_file_key/i);
  assert.match(request.payload, /file_hash/i);
  assert.match(request.payload, /on conflict/i);
  const parsedPayload = JSON.parse(request.payload);
  const ingestInsertSql = parsedPayload.requests[1].stmt.sql;
  assert.doesNotMatch(ingestInsertSql, /raw_revision/i);
});

test('buildTursoPipelineRequest_ shapes SQL over HTTP request for Turso', () => {
  const request = ingest.buildTursoPipelineRequest_(
    {
      pipelineUrl: 'https://example.turso.io/v2/pipeline',
      authToken: 'secret-token'
    },
    [
      { type: 'execute', stmt: { sql: 'select 1' } },
      { type: 'close' }
    ]
  );

  assert.equal(request.url, 'https://example.turso.io/v2/pipeline');
  assert.equal(request.method, 'post');
  assert.equal(request.contentType, 'application/json');
  assert.equal(request.headers.Authorization, 'Bearer secret-token');
  assert.deepEqual(JSON.parse(request.payload), {
    requests: [
      { type: 'execute', stmt: { sql: 'select 1' } },
      { type: 'close' }
    ]
  });
});

test('fetchRunDateExists_ passes status request as fetch(url, params) in Apps Script shape', () => {
  const calls = [];
  const urlFetchApp = {
    fetch(url, params) {
      calls.push({ url, params });
      return {
        getResponseCode() {
          return 200;
        },
        getContentText() {
          return JSON.stringify({
            results: [
              {
                type: 'ok',
                response: {
                  type: 'execute',
                  result: {
                    cols: ['normalize_status'],
                    rows: [[{ type: 'text', value: 'ready' }]]
                  }
                }
              },
              { type: 'ok', response: { type: 'close' } }
            ]
          });
        }
      };
    }
  };

  const exists = ingest.fetchRunDateExists_(
    urlFetchApp,
    {
      mode: 'turso',
      pipelineUrl: 'https://example.turso.io/v2/pipeline',
      authToken: 'secret-token'
    },
    '2026-04-14'
  );

  assert.equal(exists, true);
  assert.equal(calls.length, 1);
  assert.equal(calls[0].url, 'https://example.turso.io/v2/pipeline');
  assert.equal(calls[0].params.method, 'post');
  assert.equal(calls[0].params.headers.Authorization, 'Bearer secret-token');
  assert.equal(calls[0].params.muteHttpExceptions, true);
});

test('fetchRunDateExists_ treats non-ready existing day as not yet complete', () => {
  const urlFetchApp = {
    fetch() {
      return {
        getResponseCode() {
          return 200;
        },
        getContentText() {
          return JSON.stringify({
            results: [
              {
                type: 'ok',
                response: {
                  type: 'execute',
                  result: {
                    cols: ['normalize_status'],
                    rows: [[{ type: 'text', value: 'pending_normalize' }]]
                  }
                }
              },
              { type: 'ok', response: { type: 'close' } }
            ]
          });
        }
      };
    }
  };

  const exists = ingest.fetchRunDateExists_(
    urlFetchApp,
    {
      mode: 'turso',
      pipelineUrl: 'https://example.turso.io/v2/pipeline',
      authToken: 'secret-token'
    },
    '2026-04-14'
  );

  assert.equal(exists, false);
});

test('fetchRequestWithRetry_ retries Address unavailable transport exception', () => {
  let attempts = 0;
  const urlFetchApp = {
    fetch(url, params) {
      attempts += 1;
      if (attempts === 1) {
        throw new Error('Address unavailable: https://example.com/ingest');
      }
      return {
        getResponseCode() {
          return 200;
        },
        getContentText() {
          return JSON.stringify({ ok: true });
        }
      };
    }
  };

  const response = ingest.fetchRequestWithRetry_(
    urlFetchApp,
    {
      url: 'https://example.com/ingest',
      method: 'post',
      headers: { 'x-ingest-token': 'secret-token' },
      muteHttpExceptions: true,
      payload: { foo: 'bar' }
    },
    {
      maxAttempts: 3,
      retryableStatuses: [502, 503, 504]
    }
  );

  assert.deepEqual(response, {});
  assert.equal(attempts, 2);
});

test('fetchRequestWithRetry_ retries transient 503 response and returns success', () => {
  let attempts = 0;
  const urlFetchApp = {
    fetch() {
      attempts += 1;
      if (attempts === 1) {
        return {
          getResponseCode() {
            return 503;
          },
          getContentText() {
            return '';
          }
        };
      }

      return {
        getResponseCode() {
          return 200;
        },
        getContentText() {
          return JSON.stringify({ ok: true });
        }
      };
    }
  };

  const response = ingest.fetchRequestWithRetry_(
    urlFetchApp,
    {
      url: 'https://example.com/ingest',
      method: 'post',
      headers: { 'x-ingest-token': 'secret-token' },
      muteHttpExceptions: true
    },
    {
      maxAttempts: 3,
      retryableStatuses: [502, 503, 504]
    }
  );

  assert.deepEqual(response, {});
  assert.equal(attempts, 2);
});

test('fetchRunDateExists_ retries transient 502 from Turso HTTP endpoint', () => {
  let attempts = 0;
  const urlFetchApp = {
    fetch(url, params) {
      attempts += 1;
      if (attempts === 1) {
        return {
          getResponseCode() {
            return 502;
          },
          getContentText() {
            return '<html>bad gateway</html>';
          }
        };
      }

      return {
        getResponseCode() {
          return 200;
        },
        getContentText() {
          return JSON.stringify({
            results: [
              {
                type: 'ok',
                response: {
                  type: 'execute',
                  result: {
                    cols: ['normalize_status'],
                    rows: [[{ type: 'text', value: 'ready' }]]
                  }
                }
              },
              { type: 'ok', response: { type: 'close' } }
            ]
          });
        }
      };
    }
  };

  const exists = ingest.fetchRunDateExists_(
    urlFetchApp,
    {
      mode: 'turso',
      pipelineUrl: 'https://example.turso.io/v2/pipeline',
      authToken: 'secret-token'
    },
    '2026-04-14'
  );

  assert.equal(exists, true);
  assert.equal(attempts, 2);
});

test('fetchRunDateExists_ raises after repeated transient 503 from Turso HTTP endpoint', () => {
  let attempts = 0;
  const urlFetchApp = {
    fetch() {
      attempts += 1;
      return {
        getResponseCode() {
          return 503;
        },
        getContentText() {
          return '';
        }
      };
    }
  };

  assert.throws(
    () => ingest.fetchRunDateExists_(
      urlFetchApp,
      {
        mode: 'turso',
        pipelineUrl: 'https://example.turso.io/v2/pipeline',
        authToken: 'secret-token'
      },
      '2026-04-15'
    ),
    /Run date existence check failed with HTTP 503/
  );
  assert.equal(attempts, 3);
});

test('buildRunContext_ loads timezone, topics, and ingest settings once', () => {
  const values = [
    ['Тема письма', 'Конверсии'],
    ['TW // Назонекс Аллерджи // Solta', 'TW // Назонекс Аллерджи // Solta // conversions'],
    ['_SenSoy_', '']
  ];
  const scriptProperties = {
    getProperty(name) {
      if (name === 'TURSO_DATABASE_URL') {
        return 'libsql://example.turso.io';
      }
      if (name === 'TURSO_AUTH_TOKEN') {
        return 'secret-token';
      }
      return '';
    }
  };
  const runtime = {
    Session: {
      getScriptTimeZone() {
        return 'Asia/Tbilisi';
      }
    },
    SpreadsheetApp: {
      openById(id) {
        assert.equal(id, '17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA');
        return {
          getSheetByName(name) {
            assert.equal(name, 'отчеты');
            return {
              getLastRow() {
                return values.length;
              },
              getRange(row, column, numRows, numCols) {
                assert.equal(row, 2);
                assert.equal(column, 1);
                assert.equal(numRows, 2);
                assert.equal(numCols, 2);
                return {
                  getDisplayValues() {
                    return values.slice(1);
                  }
                };
              }
            };
          }
        };
      }
    },
    PropertiesService: {
      getScriptProperties() {
        return scriptProperties;
      }
    }
  };

  assert.deepEqual(ingest.buildRunContext_(runtime), {
    timeZone: 'Asia/Tbilisi',
    topicRules: [
      {
        raw: 'TW // Назонекс Аллерджи // Solta',
        matchedTopic: 'TW // Назонекс Аллерджи // Solta',
        primaryTopic: 'TW // Назонекс Аллерджи // Solta',
        topicRole: 'primary',
        tokens: ['tw', 'назонекс', 'аллерджи', 'solta']
      },
      {
        raw: 'TW // Назонекс Аллерджи // Solta // conversions',
        matchedTopic: 'TW // Назонекс Аллерджи // Solta // conversions',
        primaryTopic: 'TW // Назонекс Аллерджи // Solta',
        topicRole: 'secondary',
        tokens: ['tw', 'назонекс', 'аллерджи', 'solta', 'conversions']
      },
      {
        raw: '_SenSoy_',
        matchedTopic: '_SenSoy_',
        primaryTopic: '_SenSoy_',
        topicRole: 'primary',
        tokens: ['sensoy']
      }
    ],
    settings: {
      mode: 'turso',
      pipelineUrl: 'https://example.turso.io/v2/pipeline',
      authToken: 'secret-token'
    },
    verboseLogging: false
  });
});

test('buildRunContext_ prefers direct Turso settings when script properties are configured', () => {
  const values = [
    ['Тема письма', 'Конверсии'],
    ['_SenSoy_', '']
  ];
  const scriptProperties = {
    getProperty(name) {
      if (name === 'TURSO_DATABASE_URL') {
        return 'libsql://example.turso.io';
      }
      if (name === 'TURSO_AUTH_TOKEN') {
        return 'secret-token';
      }
      return '';
    }
  };
  const runtime = {
    Session: {
      getScriptTimeZone() {
        return 'Asia/Tbilisi';
      }
    },
    SpreadsheetApp: {
      openById() {
        return {
          getSheetByName() {
            return {
              getLastRow() {
                return values.length;
              },
              getRange() {
                return {
                  getDisplayValues() {
                    return values.slice(1);
                  }
                };
              }
            };
          }
        };
      }
    },
    PropertiesService: {
      getScriptProperties() {
        return scriptProperties;
      }
    }
  };

  const context = ingest.buildRunContext_(runtime);
  assert.deepEqual(context.settings, {
    mode: 'turso',
    pipelineUrl: 'https://example.turso.io/v2/pipeline',
    authToken: 'secret-token'
  });
});

test('getBackfillSettings_ uses direct Turso settings for status checks', () => {
  const scriptProperties = {
    getProperty(name) {
      if (name === 'TURSO_DATABASE_URL') {
        return 'libsql://example.turso.io';
      }
      if (name === 'TURSO_AUTH_TOKEN') {
        return 'secret-token';
      }
      return '';
    }
  };

  assert.deepEqual(
    ingest.getBackfillSettings_({
      getScriptProperties() {
        return scriptProperties;
      }
    }),
    {
      mode: 'turso',
      pipelineUrl: 'https://example.turso.io/v2/pipeline',
      authToken: 'secret-token',
      skipExistingEnabled: false
    }
  );
});

test('runForDate_ writes raw attachments directly to Turso without default reset', () => {
  const fetchCalls = [];
  const attachmentBlob = {
    getBytes() {
      return [65, 66, 67];
    }
  };
  const attachment = {
    getName() {
      return 'report.csv';
    },
    getContentType() {
      return 'text/csv';
    },
    copyBlob() {
      return attachmentBlob;
    }
  };
  const runtime = {
    UrlFetchApp: {
      fetch(url, params) {
        fetchCalls.push({ url, params });
        return {
          getResponseCode() {
            return 200;
          },
          getContentText() {
            return JSON.stringify({
              results: [
                {
                  type: 'ok',
                  response: {
                    type: 'execute',
                    result: {
                      cols: [],
                      rows: [],
                      affected_row_count: 1,
                      rows_read: 0,
                      rows_written: 1
                    }
                  }
                },
                {
                  type: 'ok',
                  response: {
                    type: 'close'
                  }
                }
              ]
            });
          }
        };
      }
    }
  };
  const runContext = {
    timeZone: 'Asia/Tbilisi',
    topicRules: [{ raw: '_SenSoy_' }],
    verboseLogging: false,
    settings: {
      mode: 'turso',
      pipelineUrl: 'https://example.turso.io/v2/pipeline',
      authToken: 'secret-token'
    }
  };

  const result = ingest.runForDate_(
    runtime,
    '2026-04-14',
    0,
    runContext,
    {
      query: 'newer_than:3d has:attachment',
      preloadedThreadsCount: 1,
      preloadedCandidates: [
        {
          primaryTopic: '_SenSoy_',
          matchedTopic: '_SenSoy_',
          topicRole: 'primary',
          subject: 'Отчёт «_SenSoy_» за 14.04.2026',
          messageDate: new Date('2026-04-14T09:00:00Z'),
          messageId: 'msg-1',
          threadId: 'thr-1',
          message: {
            getAttachments() {
              return [attachment];
            }
          }
        }
      ]
    }
  );

  assert.equal(result.attachmentsSeen, 1);
  assert.equal(result.attachmentsSent, 1);
  assert.equal(fetchCalls.length, 1);
  assert.equal(fetchCalls[0].url, 'https://example.turso.io/v2/pipeline');
  assert.equal(fetchCalls[0].params.headers.Authorization, 'Bearer secret-token');
  assert.equal(fetchCalls[0].params.headers['x-ingest-token'], undefined);
  const ingestPayload = JSON.parse(fetchCalls[0].params.payload);
  assert.match(
    ingestPayload.requests.map((request) => request.stmt && request.stmt.sql).join('\n'),
    /insert into ingest_files/i
  );
  assert.match(
    ingestPayload.requests.map((request) => request.stmt && request.stmt.sql).join('\n'),
    /insert into ingest_file_payloads/i
  );
  assert.doesNotMatch(
    ingestPayload.requests.map((request) => request.stmt && request.stmt.sql).join('\n'),
    /delete from ingest_file_payloads|delete from ingest_rows|delete from ingest_files/i
  );
});

test('runForDate_ uploads attachments from all matching messages in the window, not only the latest one', () => {
  const fetchCalls = [];
  const attachmentA = {
    getName() {
      return 'older.csv';
    },
    getContentType() {
      return 'text/csv';
    },
    copyBlob() {
      return {
        getBytes() {
          return [65, 65, 65];
        }
      };
    }
  };
  const attachmentB = {
    getName() {
      return 'newer.csv';
    },
    getContentType() {
      return 'text/csv';
    },
    copyBlob() {
      return {
        getBytes() {
          return [66, 66, 66];
        }
      };
    }
  };
  const runtime = {
    UrlFetchApp: {
      fetch(url, params) {
        fetchCalls.push({ url, params });
        return {
          getResponseCode() {
            return 200;
          },
          getContentText() {
            return JSON.stringify({
              results: [
                {
                  type: 'ok',
                  response: {
                    type: 'execute',
                    result: {
                      cols: [],
                      rows: [],
                      affected_row_count: 1,
                      rows_read: 0,
                      rows_written: 1
                    }
                  }
                },
                { type: 'ok', response: { type: 'close' } }
              ]
            });
          }
        };
      }
    }
  };
  const runContext = {
    timeZone: 'Asia/Tbilisi',
    topicRules: [{ raw: '_SenSoy_' }],
    verboseLogging: false,
    settings: {
      mode: 'turso',
      pipelineUrl: 'https://example.turso.io/v2/pipeline',
      authToken: 'secret-token'
    }
  };

  const result = ingest.runForDate_(
    runtime,
    '2026-04-14',
    0,
    runContext,
    {
      query: 'newer_than:3d has:attachment',
      preloadedThreadsCount: 1,
      preloadedCandidates: [
        {
          primaryTopic: '_SenSoy_',
          matchedTopic: '_SenSoy_',
          topicRole: 'primary',
          subject: 'Отчёт «_SenSoy_» за 14.04.2026',
          messageDate: new Date('2026-04-14T09:00:00Z'),
          messageId: 'msg-older',
          threadId: 'thr-1',
          message: {
            getAttachments() {
              return [attachmentA];
            }
          }
        },
        {
          primaryTopic: '_SenSoy_',
          matchedTopic: '_SenSoy_',
          topicRole: 'primary',
          subject: 'Отчёт «_SenSoy_» за 14.04.2026 повтор',
          messageDate: new Date('2026-04-14T10:00:00Z'),
          messageId: 'msg-newer',
          threadId: 'thr-1',
          message: {
            getAttachments() {
              return [attachmentB];
            }
          }
        }
      ]
    }
  );

  assert.equal(result.matchedMessagesBeforeLatestFilter, 2);
  assert.equal(result.matchedMessages, 2);
  assert.equal(result.attachmentsSeen, 2);
  assert.equal(result.attachmentsSent, 2);
  assert.equal(fetchCalls.length, 1);

  const payload = JSON.parse(fetchCalls[0].params.payload);
  const allSql = payload.requests.map((item) => item.stmt && item.stmt.sql).filter(Boolean).join('\n');
  assert.equal(
    payload.requests.filter((item) => item.stmt && /insert into ingest_files/i.test(item.stmt.sql)).length,
    2
  );
  assert.equal(
    payload.requests.filter((item) => item.stmt && /insert into ingest_file_payloads/i.test(item.stmt.sql)).length,
    2
  );
  assert.equal(
    payload.requests.filter((item) => item.stmt && /insert into pipeline_runs/i.test(item.stmt.sql)).length,
    1
  );
  assert.equal(
    payload.requests.filter((item) => item.stmt && /update pipeline_runs set raw_files/i.test(item.stmt.sql)).length,
    1
  );
  assert.doesNotMatch(allSql, /delete from ingest_file_payloads|delete from ingest_rows|delete from ingest_files/i);
});

test('buildAttachmentRequest_ uses stable content-based identity and upsert semantics', () => {
  const attachment = {
    getContentType() {
      return 'text/csv';
    },
    copyBlob() {
      return {
        getBytes() {
          return [65, 66, 67];
        }
      };
    }
  };

  const request = ingest.buildAttachmentRequest_(
    {
      mode: 'turso',
      pipelineUrl: 'https://example.turso.io/v2/pipeline',
      authToken: 'secret-token'
    },
    attachment,
    {
      run_date: '2026-04-06',
      primary_topic: 'weekly report',
      matched_topic: 'weekly report',
      topic_role: 'primary',
      message_subject: 'Weekly report for Solta',
      message_date: '2026-04-06T09:30:00.000Z',
      message_id: 'msg-1',
      thread_id: 'thr-1',
      attachment_name: 'client.xlsx',
      attachment_type: 'csv',
      action: 'ingest'
    }
  );

  const payload = JSON.parse(request.payload);
  const allSql = payload.requests.map((item) => item.stmt && item.stmt.sql).filter(Boolean).join('\n');
  const insertStmt = payload.requests[1].stmt;
  const args = insertStmt.args || [];

  assert.match(allSql, /raw_file_key/i);
  assert.match(allSql, /file_hash/i);
  assert.match(allSql, /on conflict/i);
  assert.match(allSql, /raw_only/i);
  assert.equal(args[0].value, args[1].value);
  assert.match(args[2].value, /^[a-f0-9]{64}$/i);
});

test('chunkItems_ splits work into stable batches', () => {
  assert.deepEqual(
    ingest.chunkItems_([1, 2, 3, 4, 5], 2),
    [[1, 2], [3, 4], [5]]
  );
});

test('resolveSettingValue_ falls back to config when script property is empty', () => {
  assert.equal(
    ingest.resolveSettingValue_('', 'fallback-value', 'PROP_NAME'),
    'fallback-value'
  );
});

test('resolveSettingValue_ prefers script property over fallback', () => {
  assert.equal(
    ingest.resolveSettingValue_('runtime-value', 'fallback-value', 'PROP_NAME'),
    'runtime-value'
  );
});

test('buildRunContext_ requires TURSO_DATABASE_URL from script properties', () => {
  const runtime = {
    Session: {
      getScriptTimeZone() {
        return 'Asia/Tbilisi';
      }
    },
    SpreadsheetApp: {
      openById() {
        return {
          getSheetByName() {
            return {
              getLastRow() {
                return 2;
              },
              getRange() {
                return {
                  getDisplayValues() {
                    return [['_SenSoy_']];
                  }
                };
              }
            };
          }
        };
      }
    },
    PropertiesService: {
      getScriptProperties() {
        return {
          getProperty(name) {
            return '';
          }
        };
      }
    }
  };

  assert.throws(
    () => ingest.buildRunContext_(runtime),
    /Missing script property "TURSO_DATABASE_URL"/
  );
});
