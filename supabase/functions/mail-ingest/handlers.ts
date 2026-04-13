import {
  IngestMeta,
  ParsedTable,
  ParseDebug,
  buildRowObject,
  buildSkipErrorText,
  isValidRunDate,
  jsonResponse,
  normalizeAttachmentType,
} from './shared.ts'
import { parseAttachment } from './parse.ts'
import {
  getSupabaseAdmin,
  insertFilePayloadRecord,
  insertFileRecord,
  markPipelineRunAfterReset,
  refreshPipelineRunAfterIngest,
} from './supabase.ts'

type AttachmentParseResult = {
  table: ParsedTable | null
  debug: ParseDebug
  parseFailureMessage: string | null
}

async function parseFile(
  attachmentType: 'xlsx' | 'csv',
  file: File,
): Promise<{ bytes: Uint8Array; parsed: AttachmentParseResult }> {
  const bytes = new Uint8Array(await file.arrayBuffer())
  let parsedResult: { table: ParsedTable | null; debug: ParseDebug } | null = null
  let parseFailureMessage: string | null = null

  try {
    parsedResult = parseAttachment(attachmentType, bytes)
  } catch (error) {
    parseFailureMessage =
      error instanceof Error ? error.message : 'Unknown parser error'
  }

  return {
    bytes,
    parsed: {
      table: parsedResult ? parsedResult.table : null,
      debug: parsedResult
        ? parsedResult.debug
        : {
            type: attachmentType,
            summary: { error: 'parse_failed' },
          },
      parseFailureMessage,
    },
  }
}

function resolveTableOutcome(parsed: AttachmentParseResult) {
  const table = parsed.table
  const status = table
    ? 'ingested'
    : parsed.parseFailureMessage
      ? 'error'
      : 'skipped'
  const header = table ? table.header : []
  const rowCount = table ? table.rows.length : 0
  const errorText = table
    ? null
    : parsed.parseFailureMessage || buildSkipErrorText('no_valid_table_block', parsed.debug)

  return {
    table,
    status,
    header,
    rowCount,
    errorText,
  }
}

function validateIngestMeta(meta: IngestMeta) {
  return (
    meta.action === 'ingest' &&
    isValidRunDate(meta.run_date) &&
    !!String(meta.primary_topic || '').trim() &&
    ['primary', 'secondary'].includes(String(meta.topic_role || ''))
  )
}

export async function handleReset(req: Request) {
  const payload = await req.json()
  const runDate = payload?.run_date

  if (payload?.action !== 'reset' || !isValidRunDate(runDate)) {
    return jsonResponse({ ok: false, error: 'Invalid reset payload' }, 400)
  }

  const supabase = getSupabaseAdmin()
  const { error } = await supabase.from('ingest_files').delete().eq('run_date', runDate)

  if (error) {
    throw error
  }

  await markPipelineRunAfterReset(supabase, runDate)

  return jsonResponse({
    ok: true,
    action: 'reset',
    run_date: runDate,
  })
}

export async function handleIngest(req: Request) {
  const form = await req.formData()
  const metaRaw = form.get('meta')
  const file = form.get('file')

  if (typeof metaRaw !== 'string' || !(file instanceof File)) {
    return jsonResponse({ ok: false, error: 'Missing multipart meta or file' }, 400)
  }

  let meta: IngestMeta
  try {
    meta = JSON.parse(metaRaw)
  } catch (_error) {
    return jsonResponse({ ok: false, error: 'Invalid meta JSON' }, 400)
  }

  if (!validateIngestMeta(meta)) {
    return jsonResponse({ ok: false, error: 'Invalid ingest payload' }, 400)
  }

  const attachmentType = normalizeAttachmentType(meta.attachment_type, file.name, file.type)
  if (!attachmentType) {
    return jsonResponse({ ok: false, error: 'Unsupported attachment type' }, 415)
  }

  const supabase = getSupabaseAdmin()
  let fileId: string | null = null

  try {
    const { bytes, parsed } = await parseFile(attachmentType, file)
    const outcome = resolveTableOutcome(parsed)

    fileId = await insertFileRecord(
      supabase,
      meta,
      attachmentType,
      outcome.status,
      outcome.header,
      outcome.rowCount,
      outcome.errorText,
    )
    await insertFilePayloadRecord(supabase, fileId, file.type, bytes)

    if (outcome.table && outcome.table.rows.length) {
      const rowPayload = outcome.table.rows.map((row, index) => ({
        file_id: fileId,
        run_date: meta.run_date,
        row_index: index + 1,
        row_json: buildRowObject(outcome.table!.header, row),
      }))

      const { error: rowsError } = await supabase.from('ingest_rows').insert(rowPayload)
      if (rowsError) {
        await supabase
          .from('ingest_files')
          .update({ status: 'error', error_text: rowsError.message })
          .eq('id', fileId)
        throw rowsError
      }
    }

    await refreshPipelineRunAfterIngest(supabase, meta.run_date)

    return jsonResponse({
      ok: true,
      status: outcome.status,
      file_id: fileId,
      rows: outcome.rowCount,
      debug: parsed.debug,
    })
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown ingest error'

    if (fileId) {
      await supabase
        .from('ingest_files')
        .update({ status: 'error', error_text: message })
        .eq('id', fileId)
      await refreshPipelineRunAfterIngest(supabase, meta.run_date)
    } else {
      await insertFileRecord(
        supabase,
        meta,
        attachmentType,
        'error',
        [],
        0,
        message,
      )
      await refreshPipelineRunAfterIngest(supabase, meta.run_date)
    }

    return jsonResponse({
      ok: false,
      error: message,
    }, 500)
  }
}
