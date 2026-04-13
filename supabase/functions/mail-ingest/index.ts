import { createClient } from 'npm:@supabase/supabase-js@2'
import { strFromU8, unzipSync } from 'npm:fflate'

const UTM_HEADERS: Record<string, true> = {
  utm_source: true,
  utm_campaign: true,
  utm_content: true,
  utm_term: true,
}

type IngestMeta = {
  action: 'ingest'
  run_date: string
  primary_topic: string
  matched_topic: string
  topic_role: 'primary' | 'secondary'
  message_subject: string
  message_date: string
  message_id: string
  thread_id: string
  attachment_name: string
  attachment_type: 'xlsx' | 'csv'
}

type TableBlock = {
  header: string[]
  headerRowIndex: number
  dataRowIndex: number | null
}

type ParsedTable = {
  header: string[]
  rows: string[][]
}

type ParseDebug = {
  type: 'csv' | 'xlsx'
  summary: unknown
}

const JSON_HEADERS = { 'Content-Type': 'application/json' }

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: JSON_HEADERS,
  })
}

function normalizeHeaderCell(value: unknown) {
  return String(value ?? '')
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/[\s-]+/g, '_')
    .replace(/[^a-z0-9_]+/g, '')
    .replace(/^_+|_+$/g, '')
}

function rowHasUtmHeader(cells: unknown[]) {
  for (const cell of cells) {
    if (UTM_HEADERS[normalizeHeaderCell(cell)]) {
      return true
    }
  }

  return false
}

function countNonEmptyCells(cells: unknown[]) {
  let count = 0

  for (const cell of cells) {
    if (String(cell ?? '').trim()) {
      count += 1
    }
  }

  return count
}

function findTableBlockInRows(rows: string[][]): TableBlock | null {
  for (let i = 0; i < rows.length; i += 1) {
    const header = rows[i] || []
    if (countNonEmptyCells(header) < 2 || !rowHasUtmHeader(header)) {
      continue
    }

    for (let j = i + 1; j < rows.length; j += 1) {
      const dataRow = rows[j] || []
      if (countNonEmptyCells(dataRow) >= 2) {
        return {
          header,
          headerRowIndex: i,
          dataRowIndex: j,
        }
      }
    }

    return {
      header,
      headerRowIndex: i,
      dataRowIndex: null,
    }
  }

  return null
}

function stripBom(text: string) {
  return String(text || '').replace(/^\uFEFF/, '')
}

function detectDelimiter(text: string) {
  const lines = String(text || '')
    .split(/\r\n|\n|\r/)
    .filter(Boolean)
    .slice(0, 5)
  const delimiters = [',', ';', '\t', '|']
  let bestDelimiter = ','
  let bestScore = -1

  for (const delimiter of delimiters) {
    let score = 0

    for (const line of lines) {
      score += line.split(delimiter).length - 1
    }

    if (score > bestScore) {
      bestScore = score
      bestDelimiter = delimiter
    }
  }

  return bestDelimiter
}

function parseDelimitedLine(line: string, delimiter: string) {
  const cells: string[] = []
  let current = ''
  let inQuotes = false

  for (let i = 0; i < line.length; i += 1) {
    const char = line[i]

    if (char === '"') {
      if (inQuotes && line[i + 1] === '"') {
        current += '"'
        i += 1
      } else {
        inQuotes = !inQuotes
      }
      continue
    }

    if (char === delimiter && !inQuotes) {
      cells.push(current)
      current = ''
      continue
    }

    current += char
  }

  cells.push(current)
  return cells
}

function parseCsvText(text: string) {
  const strippedText = stripBom(text)
  const delimiter = detectDelimiter(strippedText)
  const lines = strippedText.split(/\r\n|\n|\r/)
  const rows: string[][] = []

  for (const line of lines) {
    rows.push(parseDelimitedLine(line, delimiter))
  }

  return rows
}

function decodeXmlEntities(text: string) {
  return String(text || '')
    .replace(/&#10;/g, '\n')
    .replace(/&#13;/g, '\r')
    .replace(/&#9;/g, '\t')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&')
}

function parseWorkbookRels(xml: string) {
  const rels: Record<string, string> = {}
  const pattern = /<Relationship\b[^>]*\bId="([^"]+)"[^>]*\bTarget="([^"]+)"[^>]*\/?>/gi
  let match: RegExpExecArray | null

  while ((match = pattern.exec(xml || '')) !== null) {
    rels[match[1]] = match[2]
  }

  return rels
}

function resolveXlsxPath(target: string) {
  let normalized = String(target || '').replace(/^\/+/, '')

  if (normalized.indexOf('xl/') === 0) {
    return normalized
  }

  normalized = normalized.replace(/^(\.\.\/)+/, '')
  return `xl/${normalized}`
}

function parseSheetPaths(workbookXml: string, relMap: Record<string, string>) {
  const paths: string[] = []
  const pattern = /<sheet\b[^>]*\br:id="([^"]+)"[^>]*\/?>/gi
  let match: RegExpExecArray | null

  while ((match = pattern.exec(workbookXml || '')) !== null) {
    const relTarget = relMap[match[1]]
    if (relTarget) {
      paths.push(resolveXlsxPath(relTarget))
    }
  }

  return paths
}

function parseSharedStrings(xml: string) {
  const sharedStrings: string[] = []
  const items = String(xml || '').match(/<si\b[\s\S]*?<\/si>/gi) || []

  for (const item of items) {
    const texts: string[] = []
    const pattern = /<t(?:\s[^>]*)?>([\s\S]*?)<\/t>/gi
    let match: RegExpExecArray | null

    while ((match = pattern.exec(item)) !== null) {
      texts.push(decodeXmlEntities(match[1]))
    }

    sharedStrings.push(texts.join(''))
  }

  return sharedStrings
}

function extractXlsxRowCells(rowXml: string, sharedStrings: string[]) {
  const cells: string[] = []
  const pattern = /<c\b([^>]*)>([\s\S]*?)<\/c>/gi
  let match: RegExpExecArray | null

  while ((match = pattern.exec(rowXml || '')) !== null) {
    const attrs = match[1] || ''
    const body = match[2] || ''
    const typeMatch = attrs.match(/\bt="([^"]+)"/i)
    const cellType = typeMatch ? typeMatch[1] : ''

    if (cellType === 'inlineStr') {
      const texts: string[] = []
      const textPattern = /<t(?:\s[^>]*)?>([\s\S]*?)<\/t>/gi
      let textMatch: RegExpExecArray | null

      while ((textMatch = textPattern.exec(body)) !== null) {
        texts.push(decodeXmlEntities(textMatch[1]))
      }

      cells.push(texts.join(''))
      continue
    }

    const valueMatch = body.match(/<v>([\s\S]*?)<\/v>/i)
    if (!valueMatch) {
      continue
    }

    const rawValue = decodeXmlEntities(valueMatch[1])
    if (cellType === 's') {
      cells.push(sharedStrings[Number(rawValue)] || '')
    } else {
      cells.push(rawValue)
    }
  }

  return cells
}

function extractRowsFromSheetXml(sheetXml: string, sharedStrings: string[]) {
  const rowXmlList = String(sheetXml || '').match(/<row\b[\s\S]*?<\/row>/gi) || []
  const rows: string[][] = []

  for (const rowXml of rowXmlList) {
    rows.push(extractXlsxRowCells(rowXml, sharedStrings))
  }

  return rows
}

function buildRowObject(header: string[], row: string[]) {
  const result: Record<string, string> = {}
  const maxLength = Math.max(header.length, row.length)

  for (let i = 0; i < maxLength; i += 1) {
    const key = String(header[i] || '').trim()
    if (!key) {
      continue
    }

    result[key] = String(row[i] || '')
  }

  return result
}

function isSummaryRow(row: string[]) {
  for (const cell of row) {
    const value = String(cell || '').trim()
    if (!value) {
      continue
    }

    const normalized = value.toLowerCase().replace(/ё/g, 'е').replace(/\s+/g, ' ').trim()
    return normalized.startsWith('итого') || normalized.startsWith('total')
  }

  return false
}

function extractDataRows(rows: string[][], tableBlock: TableBlock) {
  if (tableBlock.dataRowIndex === null) {
    return []
  }

  const dataRows: string[][] = []
  let skippingSummary = true

  for (let i = tableBlock.dataRowIndex; i < rows.length; i += 1) {
    const row = rows[i] || []
    if (countNonEmptyCells(row) === 0) {
      break
    }

    if (skippingSummary && isSummaryRow(row)) {
      continue
    }

    skippingSummary = false
    dataRows.push(row)
  }

  return dataRows
}

function previewCell(value: unknown) {
  return String(value ?? '').replace(/\s+/g, ' ').trim().slice(0, 80)
}

function summarizeRows(rows: string[][], maxRows = 8, maxCols = 8) {
  return rows.slice(0, maxRows).map((row, index) => ({
    row_index: index + 1,
    non_empty_cells: countNonEmptyCells(row),
    has_utm_header: rowHasUtmHeader(row),
    cells: row.slice(0, maxCols).map(previewCell),
  }))
}

function buildSkipErrorText(reason: string, debug: ParseDebug) {
  return JSON.stringify({
    reason,
    debug,
  }).slice(0, 6000)
}

function parseCsvTable(text: string): { table: ParsedTable | null; debug: ParseDebug } {
  const rows = parseCsvText(text)
  const tableBlock = findTableBlockInRows(rows)
  if (!tableBlock) {
    return {
      table: null,
      debug: {
        type: 'csv',
        summary: {
          preview_rows: summarizeRows(rows),
        },
      },
    }
  }

  return {
    table: {
      header: tableBlock.header,
      rows: extractDataRows(rows, tableBlock),
    },
    debug: {
      type: 'csv',
      summary: {
        header_row_index: tableBlock.headerRowIndex + 1,
        data_row_index:
          tableBlock.dataRowIndex === null ? null : tableBlock.dataRowIndex + 1,
      },
    },
  }
}

function parseXlsxTable(bytes: Uint8Array): { table: ParsedTable | null; debug: ParseDebug } {
  const files = unzipSync(bytes, {
    filter: (file) => file.originalSize < 10_000_000,
  })

  if (!files['xl/workbook.xml']) {
    return {
      table: null,
      debug: {
        type: 'xlsx',
        summary: {
          error: 'missing_workbook_xml',
        },
      },
    }
  }

  const workbookXml = strFromU8(files['xl/workbook.xml'])
  const relsXml = files['xl/_rels/workbook.xml.rels']
    ? strFromU8(files['xl/_rels/workbook.xml.rels'])
    : ''
  const sharedStringsXml = files['xl/sharedStrings.xml']
    ? strFromU8(files['xl/sharedStrings.xml'])
    : ''
  const relMap = parseWorkbookRels(relsXml)
  const sheetPaths = parseSheetPaths(workbookXml, relMap)
  const sharedStrings = parseSharedStrings(sharedStringsXml)
  const sheetPreviews: unknown[] = []

  for (const sheetPath of sheetPaths) {
    const sheetBytes = files[sheetPath]
    if (!sheetBytes) {
      continue
    }

    const rows = extractRowsFromSheetXml(strFromU8(sheetBytes), sharedStrings)
    const tableBlock = findTableBlockInRows(rows)
    sheetPreviews.push({
      sheet_path: sheetPath,
      preview_rows: summarizeRows(rows),
    })
    if (!tableBlock) {
      continue
    }

    return {
      table: {
        header: tableBlock.header,
        rows: extractDataRows(rows, tableBlock),
      },
      debug: {
        type: 'xlsx',
        summary: {
          matched_sheet_path: sheetPath,
          header_row_index: tableBlock.headerRowIndex + 1,
          data_row_index:
            tableBlock.dataRowIndex === null ? null : tableBlock.dataRowIndex + 1,
        },
      },
    }
  }

  return {
    table: null,
    debug: {
      type: 'xlsx',
      summary: {
        sheet_previews: sheetPreviews,
      },
    },
  }
}

function normalizeAttachmentType(metaType: unknown, filename: string, contentType: string) {
  const type = String(metaType || '').toLowerCase()
  const lowerName = String(filename || '').toLowerCase()
  const lowerContentType = String(contentType || '').toLowerCase()

  if (
    type === 'xlsx' ||
    lowerName.endsWith('.xlsx') ||
    lowerContentType.includes('openxmlformats-officedocument.spreadsheetml.sheet')
  ) {
    return 'xlsx'
  }

  if (
    type === 'csv' ||
    lowerName.endsWith('.csv') ||
    lowerContentType.includes('text/csv') ||
    lowerContentType.includes('csv')
  ) {
    return 'csv'
  }

  return null
}

function isValidRunDate(value: unknown) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''))
}

function getSupabaseAdmin() {
  const url = Deno.env.get('SUPABASE_URL') || ''
  const serviceRoleKey = Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') || ''

  if (!url || !serviceRoleKey) {
    throw new Error('Missing Supabase server environment variables')
  }

  return createClient(url, serviceRoleKey)
}

function assertAuthorized(req: Request) {
  const expectedToken = Deno.env.get('INGEST_TOKEN') || ''
  const actualToken = req.headers.get('x-ingest-token') || ''

  if (!expectedToken || actualToken !== expectedToken) {
    throw new Error('Unauthorized')
  }
}

async function insertFileRecord(
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

async function insertFilePayloadRecord(
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

async function handleReset(req: Request) {
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

  return jsonResponse({
    ok: true,
    action: 'reset',
    run_date: runDate,
  })
}

async function handleIngest(req: Request) {
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

  if (
    meta.action !== 'ingest' ||
    !isValidRunDate(meta.run_date) ||
    !String(meta.primary_topic || '').trim() ||
    !['primary', 'secondary'].includes(String(meta.topic_role || ''))
  ) {
    return jsonResponse({ ok: false, error: 'Invalid ingest payload' }, 400)
  }

  const attachmentType = normalizeAttachmentType(meta.attachment_type, file.name, file.type)
  if (!attachmentType) {
    return jsonResponse({ ok: false, error: 'Unsupported attachment type' }, 415)
  }

  const supabase = getSupabaseAdmin()
  let fileId: string | null = null

  try {
    const bytes = new Uint8Array(await file.arrayBuffer())
    let parsed: { table: ParsedTable | null; debug: ParseDebug } | null = null
    let parseFailureMessage: string | null = null

    try {
      parsed =
        attachmentType === 'csv'
          ? parseCsvTable(strFromU8(bytes))
          : parseXlsxTable(bytes)
    } catch (error) {
      parseFailureMessage =
        error instanceof Error ? error.message : 'Unknown parser error'
    }

    const table = parsed ? parsed.table : null
    const status = table
      ? 'ingested'
      : parseFailureMessage
        ? 'error'
        : 'skipped'
    const header = table ? table.header : []
    const rowCount = table ? table.rows.length : 0
    const errorText = table
      ? null
      : parseFailureMessage || buildSkipErrorText('no_valid_table_block', parsed!.debug)

    fileId = await insertFileRecord(
      supabase,
      meta,
      attachmentType,
      status,
      header,
      rowCount,
      errorText,
    )
    await insertFilePayloadRecord(supabase, fileId, file.type, bytes)

    if (table && table.rows.length) {
      const rowPayload = table.rows.map((row, index) => ({
        file_id: fileId,
        run_date: meta.run_date,
        row_index: index + 1,
        row_json: buildRowObject(table.header, row),
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

    return jsonResponse({
      ok: true,
      status,
      file_id: fileId,
      rows: rowCount,
      debug: parsed ? parsed.debug : undefined,
    })
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unknown ingest error'

    if (fileId) {
      await supabase
        .from('ingest_files')
        .update({ status: 'error', error_text: message })
        .eq('id', fileId)
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
    }

    return jsonResponse({
      ok: false,
      error: message,
    }, 500)
  }
}

Deno.serve(async (req: Request) => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: JSON_HEADERS })
  }

  if (req.method !== 'POST') {
    return jsonResponse({ ok: false, error: 'Method not allowed' }, 405)
  }

  try {
    assertAuthorized(req)
    const contentType = req.headers.get('content-type') || ''

    if (contentType.includes('application/json')) {
      return await handleReset(req)
    }

    if (contentType.includes('multipart/form-data')) {
      return await handleIngest(req)
    }

    return jsonResponse({ ok: false, error: 'Unsupported content type' }, 415)
  } catch (error) {
    const message = error instanceof Error ? error.message : 'Unexpected error'
    const status = message === 'Unauthorized' ? 401 : 500
    return jsonResponse({ ok: false, error: message }, status)
  }
})
