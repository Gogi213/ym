
function normalizeTursoPipelineUrl_(databaseUrl) {
  const raw = String(databaseUrl || '').trim();
  if (!raw) {
    return '';
  }

  const withoutPipeline = raw.replace(/\/v2\/pipeline$/i, '').replace(/\/+$/, '');
  if (/^libsql:\/\//i.test(withoutPipeline)) {
    return 'https://' + withoutPipeline.replace(/^libsql:\/\//i, '') + '/v2/pipeline';
  }
  if (/^https?:\/\//i.test(withoutPipeline)) {
    return withoutPipeline + '/v2/pipeline';
  }
  return 'https://' + withoutPipeline.replace(/^\/+/, '') + '/v2/pipeline';
}

function buildTursoValue_(value) {
  if (value === null || value === undefined) {
    return { type: 'null' };
  }
  if (typeof value === 'boolean') {
    return { type: 'integer', value: value ? '1' : '0' };
  }
  if (typeof value === 'number' && isFinite(value)) {
    return Number.isInteger(value)
      ? { type: 'integer', value: String(value) }
      : { type: 'float', value: String(value) };
  }
  return { type: 'text', value: String(value) };
}

function buildTursoExecuteRequest_(sql, args) {
  const stmt = { sql };
  if (Array.isArray(args) && args.length) {
    stmt.args = args.map(buildTursoValue_);
  }
  return { type: 'execute', stmt };
}

function buildTursoPipelineRequest_(settings, requests) {
  return {
    url: settings.pipelineUrl,
    method: 'post',
    contentType: 'application/json',
    headers: {
      Authorization: 'Bearer ' + settings.authToken
    },
    muteHttpExceptions: true,
    payload: JSON.stringify({ requests })
  };
}

function byteToHex_(value) {
  const normalized = Number(value);
  const byte = normalized < 0 ? normalized + 256 : normalized;
  return (byte + 256).toString(16).slice(-2);
}

function computeSha256HexFromBytes_(bytes) {
  if (typeof Utilities !== 'undefined' && Utilities.computeDigest && Utilities.DigestAlgorithm) {
    return Utilities.computeDigest(Utilities.DigestAlgorithm.SHA_256, bytes)
      .map(byteToHex_)
      .join('');
  }
  if (typeof crypto !== 'undefined' && crypto.createHash) {
    return crypto.createHash('sha256').update(Buffer.from(bytes)).digest('hex');
  }
  throw new Error('No SHA-256 digest implementation available in this runtime');
}

function buildRawFileKey_(metadata, fileHash) {
  return [
    String(metadata.run_date || '').trim(),
    String(metadata.primary_topic || '').trim(),
    String(fileHash || '').trim()
  ].join('|');
}

function encodeBytesBase64_(bytes) {
  if (typeof Utilities !== 'undefined' && Utilities.base64Encode) {
    return Utilities.base64Encode(bytes);
  }
  if (typeof Buffer !== 'undefined') {
    return Buffer.from(bytes).toString('base64');
  }
  throw new Error('No base64 encoder available in this runtime');
}

function buildTursoResetRequests_(runDate) {
  return [
    buildTursoExecuteRequest_(
      'delete from ingest_file_payloads where file_id in (select id from ingest_files where run_date = ?)',
      [runDate]
    ),
    buildTursoExecuteRequest_(
      'delete from ingest_rows where file_id in (select id from ingest_files where run_date = ?)',
      [runDate]
    ),
    buildTursoExecuteRequest_(
      'delete from ingest_files where run_date = ?',
      [runDate]
    ),
    buildTursoExecuteRequest_(
      "insert into pipeline_runs (run_date, raw_revision, normalize_status, raw_files, raw_rows, normalized_files, normalized_rows, last_ingest_at, normalized_at, last_error, updated_at) values (?, 0, 'pending_normalize', 0, 0, 0, 0, current_timestamp, null, null, current_timestamp) on conflict(run_date) do nothing",
      [runDate]
    ),
    buildTursoExecuteRequest_(
      "update pipeline_runs set raw_revision = raw_revision + 1, normalize_status = 'pending_normalize', raw_files = 0, raw_rows = 0, normalized_files = 0, normalized_rows = 0, last_ingest_at = current_timestamp, normalized_at = null, last_error = null, updated_at = current_timestamp where run_date = ?",
      [runDate]
    ),
    { type: 'close' }
  ];
}

function buildTursoAttachmentRequests_(attachment, metadata) {
  const blob = attachment.copyBlob();
  const bytes = blob.getBytes();
  const contentType = attachment.getContentType ? attachment.getContentType() : '';
  const fileBase64 = encodeBytesBase64_(bytes);
  const fileSizeBytes = Array.isArray(bytes) ? bytes.length : Number(bytes && bytes.length ? bytes.length : 0);
  const fileHash = computeSha256HexFromBytes_(bytes);
  const rawFileKey = buildRawFileKey_(metadata, fileHash);
  const fileId = rawFileKey;

  return [
    buildTursoExecuteRequest_(
      "insert into pipeline_runs (run_date, raw_revision, normalize_status, raw_files, raw_rows, normalized_files, normalized_rows, last_ingest_at, normalized_at, last_error, created_at, updated_at) values (?, 1, 'raw_only', 0, 0, 0, 0, current_timestamp, null, null, current_timestamp, current_timestamp) on conflict(run_date) do update set raw_revision = case when pipeline_runs.normalize_status in ('ready', 'normalize_error') then pipeline_runs.raw_revision + 1 else pipeline_runs.raw_revision end, normalize_status = 'raw_only', raw_rows = 0, normalized_files = 0, normalized_rows = 0, last_ingest_at = current_timestamp, normalized_at = null, last_error = null, updated_at = current_timestamp",
      [metadata.run_date]
    ),
    buildTursoExecuteRequest_(
      "insert into ingest_files (id, raw_file_key, file_hash, run_date, message_id, thread_id, message_date, message_subject, primary_topic, matched_topic, topic_role, attachment_name, attachment_type, status, header_json, row_count, error_text) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'raw_only', '[]', 0, null) on conflict(raw_file_key) do update set run_date = excluded.run_date, message_id = excluded.message_id, thread_id = excluded.thread_id, message_date = excluded.message_date, message_subject = excluded.message_subject, primary_topic = excluded.primary_topic, matched_topic = excluded.matched_topic, topic_role = excluded.topic_role, attachment_name = excluded.attachment_name, attachment_type = excluded.attachment_type, status = 'raw_only', header_json = '[]', row_count = 0, error_text = null, updated_at = current_timestamp",
      [
        fileId,
        rawFileKey,
        fileHash,
        metadata.run_date,
        metadata.message_id,
        metadata.thread_id,
        metadata.message_date,
        metadata.message_subject,
        metadata.primary_topic,
        metadata.matched_topic,
        metadata.topic_role,
        metadata.attachment_name,
        metadata.attachment_type
      ]
    ),
    buildTursoExecuteRequest_(
      'insert into ingest_file_payloads (file_id, content_type, file_size_bytes, file_base64) values (?, ?, ?, ?) on conflict(file_id) do update set content_type = excluded.content_type, file_size_bytes = excluded.file_size_bytes, file_base64 = excluded.file_base64',
      [fileId, contentType, fileSizeBytes, fileBase64]
    ),
    buildTursoExecuteRequest_(
      'update pipeline_runs set raw_files = (select count(*) from ingest_files where run_date = ?), last_ingest_at = current_timestamp, updated_at = current_timestamp where run_date = ?',
      [metadata.run_date, metadata.run_date]
    ),
    { type: 'close' }
  ];
}

function buildTursoStatusRequest_(settings, runDate) {
  return buildTursoPipelineRequest_(
    settings,
    [
      buildTursoExecuteRequest_(
        'select normalize_status from pipeline_runs where run_date = ? limit 1',
        [runDate]
      ),
      { type: 'close' }
    ]
  );
}

function readTursoCellValue_(cell) {
  if (cell && typeof cell === 'object') {
    if (Object.prototype.hasOwnProperty.call(cell, 'value')) {
      return cell.value;
    }
    if (Object.prototype.hasOwnProperty.call(cell, 'base64')) {
      return cell.base64;
    }
  }
  return cell;
}

function extractTursoRows_(parsed) {
  const result = parsed && parsed.json && Array.isArray(parsed.json.results)
    ? parsed.json.results[0]
    : null;
  const executeResult = result && result.response && result.response.result ? result.response.result : null;
  const cols = Array.isArray(executeResult && executeResult.cols)
    ? executeResult.cols.map((column) => typeof column === 'string' ? column : column && column.name)
    : [];
  const rows = Array.isArray(executeResult && executeResult.rows) ? executeResult.rows : [];

  return rows.map((row) => {
    if (Array.isArray(row)) {
      const mapped = {};
      for (let index = 0; index < row.length; index++) {
        mapped[String(cols[index] || index)] = readTursoCellValue_(row[index]);
      }
      return mapped;
    }
    if (row && typeof row === 'object') {
      const mapped = {};
      const keys = Object.keys(row);
      for (let index = 0; index < keys.length; index++) {
        mapped[keys[index]] = readTursoCellValue_(row[keys[index]]);
      }
      return mapped;
    }
    return row;
  });
}

function assertSuccessfulTursoResponse_(response, actionLabel) {
  const parsed = parseJsonResponse_(response);

  if (parsed.responseCode < 200 || parsed.responseCode >= 300) {
    throw new Error(
      actionLabel + ' failed with HTTP ' + parsed.responseCode + ': ' + parsed.body
    );
  }

  const results = parsed.json && Array.isArray(parsed.json.results) ? parsed.json.results : [];
  for (let index = 0; index < results.length; index++) {
    if (results[index] && results[index].type === 'ok') {
      continue;
    }

    const errorPayload = results[index] && (results[index].error || results[index]);
    throw new Error(
      actionLabel + ' failed: ' + JSON.stringify(errorPayload || parsed.json)
    );
  }

  return parsed;
}

function postTursoReset_(urlFetchApp, settings, runDate) {
  return assertSuccessfulTursoResponse_(
    fetchRequestWithRetry_(
      urlFetchApp,
      buildTursoPipelineRequest_(settings, buildTursoResetRequests_(runDate)),
      {
        maxAttempts: 3,
        retryableStatuses: [502, 503, 504]
      }
    ),
    'Reset request'
  );
}

function fetchTursoRunDateExists_(urlFetchApp, settings, runDate) {
  const response = assertSuccessfulTursoResponse_(
    fetchRequestWithRetry_(urlFetchApp, buildTursoStatusRequest_(settings, runDate), {
      maxAttempts: 3,
      retryableStatuses: [502, 503, 504]
    }),
    'Run date existence check'
  );
  const rows = extractTursoRows_(response);
  if (!rows.length) {
    return false;
  }
  const normalizeStatus = String(rows[0].normalize_status || '').trim();
  return normalizeStatus ? normalizeStatus === 'ready' : true;
}
