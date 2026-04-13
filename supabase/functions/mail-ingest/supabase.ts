import { createClient } from 'npm:@supabase/supabase-js@2'

import { IngestMeta } from './shared.ts'

export function getSupabaseAdmin() {
  const url = Deno.env.get('SUPABASE_URL') || ''
  const serviceRoleKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || ''

  if (!url || !serviceRoleKey) {
    throw new Error('Missing Supabase server environment variables')
  }

  return createClient(url, serviceRoleKey)
}

export async function markPipelineRunAfterReset(
  supabase: ReturnType<typeof getSupabaseAdmin>,
  runDate: string,
) {
  const { data: existing, error: fetchError } = await supabase
    .from('pipeline_runs')
    .select('raw_revision')
    .eq('run_date', runDate)
    .maybeSingle()

  if (fetchError) {
    throw fetchError
  }

  const nextRevision = Number(existing?.raw_revision || 0) + 1
  const { error } = await supabase.from('pipeline_runs').upsert(
    {
      run_date: runDate,
      raw_revision: nextRevision,
      normalize_status: 'pending_normalize',
      raw_files: 0,
      raw_rows: 0,
      normalized_files: 0,
      normalized_rows: 0,
      last_ingest_at: new Date().toISOString(),
      normalized_at: null,
      last_error: null,
      updated_at: new Date().toISOString(),
    },
    { onConflict: 'run_date' },
  )

  if (error) {
    throw error
  }
}

export async function refreshPipelineRunAfterIngest(
  supabase: ReturnType<typeof getSupabaseAdmin>,
  runDate: string,
) {
  const { data: files, error: filesError } = await supabase
    .from('ingest_files')
    .select('status,row_count')
    .eq('run_date', runDate)

  if (filesError) {
    throw filesError
  }

  const totalFiles = Array.isArray(files) ? files.length : 0
  let ingestedFiles = 0
  let rawRows = 0

  for (const file of files || []) {
    if (file.status === 'ingested') {
      ingestedFiles += 1
      rawRows += Number(file.row_count || 0)
    }
  }

  const { error } = await supabase
    .from('pipeline_runs')
    .update({
      raw_files: totalFiles,
      raw_rows: rawRows,
      normalized_files: 0,
      normalized_rows: 0,
      normalize_status: ingestedFiles > 0 ? 'pending_normalize' : 'raw_only',
      last_ingest_at: new Date().toISOString(),
      normalized_at: null,
      last_error: null,
      updated_at: new Date().toISOString(),
    })
    .eq('run_date', runDate)

  if (error) {
    throw error
  }
}

export async function insertFileRecord(
  supabase: ReturnType<typeof getSupabaseAdmin>,
  meta: IngestMeta,
  attachmentType: 'xlsx' | 'csv',
  status: 'ingested' | 'skipped' | 'error',
  header: string[],
  rowCount: number,
  errorText: string | null,
) {
  const payload = {
    run_date: meta.run_date,
    message_id: meta.message_id,
    thread_id: meta.thread_id,
    message_date: meta.message_date,
    message_subject: meta.message_subject,
    primary_topic: meta.primary_topic,
    matched_topic: meta.matched_topic,
    topic_role: meta.topic_role,
    attachment_name: meta.attachment_name,
    attachment_type: attachmentType,
    status,
    header_json: header,
    row_count: rowCount,
    error_text: errorText,
  }

  const { data, error } = await supabase
    .from('ingest_files')
    .insert(payload)
    .select('id')
    .single()

  if (error) {
    throw error
  }

  return data.id as string
}

function encodeBase64Bytes(bytes: Uint8Array) {
  let binary = ''
  const chunkSize = 0x8000

  for (let i = 0; i < bytes.length; i += chunkSize) {
    const chunk = bytes.subarray(i, i + chunkSize)
    binary += String.fromCharCode(...chunk)
  }

  return btoa(binary)
}

export async function insertFilePayloadRecord(
  supabase: ReturnType<typeof getSupabaseAdmin>,
  fileId: string,
  fileContentType: string,
  bytes: Uint8Array,
) {
  const payload = {
    file_id: fileId,
    content_type: fileContentType || null,
    file_size_bytes: bytes.length,
    file_base64: encodeBase64Bytes(bytes),
  }

  const { error } = await supabase.from('ingest_file_payloads').insert(payload)

  if (error) {
    throw error
  }
}
