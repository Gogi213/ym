export const UTM_HEADERS: Record<string, true> = {
  utm_source: true,
  utm_campaign: true,
  utm_content: true,
  utm_term: true,
}

export type IngestMeta = {
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

export type TableBlock = {
  header: string[]
  headerRowIndex: number
  dataRowIndex: number | null
}

export type ParsedTable = {
  header: string[]
  rows: string[][]
}

export type ParseDebug = {
  type: 'csv' | 'xlsx'
  summary: unknown
}

export const JSON_HEADERS = { 'Content-Type': 'application/json' }

export function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: JSON_HEADERS,
  })
}

export function normalizeHeaderCell(value: unknown) {
  return String(value ?? '')
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/[\s-]+/g, '_')
    .replace(/[^a-z0-9_]+/g, '')
    .replace(/^_+|_+$/g, '')
}

export function rowHasUtmHeader(cells: unknown[]) {
  for (const cell of cells) {
    if (UTM_HEADERS[normalizeHeaderCell(cell)]) {
      return true
    }
  }

  return false
}

export function countNonEmptyCells(cells: unknown[]) {
  let count = 0

  for (const cell of cells) {
    if (String(cell ?? '').trim()) {
      count += 1
    }
  }

  return count
}

export function findTableBlockInRows(rows: string[][]): TableBlock | null {
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

export function buildRowObject(header: string[], row: string[]) {
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

export function buildSkipErrorText(reason: string, debug: ParseDebug) {
  return JSON.stringify({
    reason,
    debug,
  }).slice(0, 6000)
}

export function normalizeAttachmentType(metaType: unknown, filename: string, contentType: string) {
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

export function isValidRunDate(value: unknown) {
  return /^\d{4}-\d{2}-\d{2}$/.test(String(value || ''))
}
