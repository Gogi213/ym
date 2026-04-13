import { strFromU8, unzipSync } from 'npm:fflate'

import {
  ParsedTable,
  ParseDebug,
  TableBlock,
  countNonEmptyCells,
  findTableBlockInRows,
  rowHasUtmHeader,
} from './shared.ts'

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

export function parseAttachment(
  attachmentType: 'xlsx' | 'csv',
  bytes: Uint8Array,
): { table: ParsedTable | null; debug: ParseDebug } {
  return attachmentType === 'csv'
    ? parseCsvTable(strFromU8(bytes))
    : parseXlsxTable(bytes)
}
