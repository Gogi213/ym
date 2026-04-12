Ниже — нормальное ТЗ и сразу код без лишней херни.

Код ниже опирается на стандартные сервисы Apps Script: `GmailApp` ищет письма по Gmail query, `SpreadsheetApp.openById()` открывает таблицу по ID, `DriveApp` работает с папками и файлами, а `Utilities` умеет работать с zip/blob-данными. Важная оговорка: требование “не складывать на Drive запускающего аккаунта” **гарантированно** выполняется только если целевая папка находится в **Shared Drive**, потому что в shared drives файлы принадлежат организации, а не конкретному пользователю. Если это обычная расшаренная папка в My Drive, файл всё равно создаётся от имени аккаунта, из-под которого бежит скрипт. ([Google for Developers][1])

## ТЗ

1. Скрипт запускается из почты `ya-stats@solta.io`.
2. Скрипт забирает письма **только** от `ya-stats@solta.io`.
3. Темы для отбора берутся из Google Sheets:

   * файл: `17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA`
   * лист: `"отчеты"`
   * столбец: `A`
4. Совпадение темы — **неполное**:

   * строка из ячейки разбивается на слова
   * письмо подходит, если **все слова** из ячейки встречаются в теме письма
   * в теме письма могут быть дополнительные слова
5. Из подходящих писем надо проверять **каждое вложение-таблицу**.
6. Вложение считается “нашим клиентом”, если в таблице есть строка-шапка, где встречается **хотя бы один** из заголовков:

   * `utm_source`
   * `utm_campaign`
   * `utm_content`
   * `utm_term`
   * регистр любой
7. Над таблицей может быть мусорный текст типа “тыры пыры сделано с utm меткой” — его игнорируем. Проверяем именно строки таблицы, первые несколько строк, пока не найдём реальную шапку.
8. Если UTM-заголовков нет — вложение игнорируется.
9. Подходящие вложения сохраняются в папку:

   * `1f38YjgK214y-KNH8oSqoQm8EWbZSfywn`
10. Для каждого письма создаётся отдельная подпапка по **теме письма**.
11. Внутрь этой папки сохраняются только валидные вложения.
12. Чтобы не жрать одно и то же по кругу, обработанные треды помечаются Gmail label’ом.

## Что поддерживает код

* `csv`
* `tsv`
* `txt` с табличным содержимым
* `xlsx` без временной конвертации в Google Sheets
* `html`-таблицы
* псевдо-`xls`, если внутри на самом деле html-таблица
* `zip`, если внутри лежат такие файлы

Старый бинарный `xls` сознательно не парсится — это самый ублюдский формат, и ради скорости/надёжности я его здесь не тащу.

---

```javascript
const CONFIG = {
  SENDER: 'ya-stats@solta.io',
  SOURCE_SPREADSHEET_ID: '17izchH29LyxuTCNWJ0SThSXmuubMnNFCjtPJiWtcxFA',
  SOURCE_SHEET_NAME: 'отчеты',
  SOURCE_COLUMN: 1, // A
  TARGET_FOLDER_ID: '1f38YjgK214y-KNH8oSqoQm8EWbZSfywn',
  PROCESSED_LABEL: 'solta/utm_processed',
  THREAD_BATCH: 50,
  HEADER_SCAN_ROWS: 20
};

const UTM_HEADERS = {
  utm_source: true,
  utm_campaign: true,
  utm_content: true,
  utm_term: true
};

function run() {
  const topics = loadTopics_();
  if (!topics.length) {
    throw new Error('В листе "отчеты" нет тем в столбце A');
  }

  const rootFolder = DriveApp.getFolderById(CONFIG.TARGET_FOLDER_ID);
  const label = GmailApp.getUserLabelByName(CONFIG.PROCESSED_LABEL) || GmailApp.createLabel(CONFIG.PROCESSED_LABEL);
  const folderCache = Object.create(null);

  const stats = {
    threads: 0,
    messages: 0,
    checkedAttachments: 0,
    savedAttachments: 0,
    createdFolders: 0,
    skippedThreadsWithErrors: 0
  };

  const query = 'from:' + CONFIG.SENDER + ' has:attachment -label:' + CONFIG.PROCESSED_LABEL;

  while (true) {
    const threads = GmailApp.search(query, 0, CONFIG.THREAD_BATCH);
    if (!threads.length) break;

    stats.threads += threads.length;

    for (let i = 0; i < threads.length; i++) {
      const thread = threads[i];
      try {
        processThread_(thread, topics, rootFolder, folderCache, stats);
        thread.addLabel(label);
      } catch (err) {
        stats.skippedThreadsWithErrors++;
        console.error('THREAD ERROR: ' + thread.getFirstMessageSubject() + ' :: ' + err.message);
      }
    }
  }

  console.log(JSON.stringify(stats, null, 2));
}

function processThread_(thread, topics, rootFolder, folderCache, stats) {
  const messages = thread.getMessages();
  stats.messages += messages.length;

  for (let i = 0; i < messages.length; i++) {
    const message = messages[i];
    const subject = (message.getSubject() || '').trim();
    if (!subject) continue;
    if (!subjectMatchesTopics_(subject, topics)) continue;

    const attachments = message.getAttachments({
      includeAttachments: true,
      includeInlineImages: false
    });

    if (!attachments.length) continue;

    let subjectFolder = null;

    for (let j = 0; j < attachments.length; j++) {
      const att = attachments[j];
      const candidateBlobs = explodeIfZip_(att);

      for (let k = 0; k < candidateBlobs.length; k++) {
        const blob = candidateBlobs[k];
        stats.checkedAttachments++;

        if (!isClientTableBlob_(blob)) continue;

        if (!subjectFolder) {
          subjectFolder = getOrCreateChildFolder_(rootFolder, subject, folderCache, stats);
        }

        saveBlob_(blob, subjectFolder, message.getDate());
        stats.savedAttachments++;
      }
    }
  }
}

function loadTopics_() {
  const ss = SpreadsheetApp.openById(CONFIG.SOURCE_SPREADSHEET_ID);
  const sheet = ss.getSheetByName(CONFIG.SOURCE_SHEET_NAME);
  if (!sheet) throw new Error('Не найден лист: ' + CONFIG.SOURCE_SHEET_NAME);

  const lastRow = sheet.getLastRow();
  if (lastRow < 1) return [];

  const values = sheet.getRange(1, CONFIG.SOURCE_COLUMN, lastRow, 1).getDisplayValues();
  const topics = [];

  for (let i = 0; i < values.length; i++) {
    const raw = String(values[i][0] || '').trim();
    if (!raw) continue;

    const tokens = tokenizeSubject_(raw);
    if (!tokens.length) continue;

    topics.push({
      raw: raw,
      tokens: tokens
    });
  }

  return topics;
}

function subjectMatchesTopics_(subject, topics) {
  const normalizedSubject = normalizeText_(subject);

  for (let i = 0; i < topics.length; i++) {
    const tokens = topics[i].tokens;
    let ok = true;

    for (let j = 0; j < tokens.length; j++) {
      if (normalizedSubject.indexOf(tokens[j]) === -1) {
        ok = false;
        break;
      }
    }

    if (ok) return true;
  }

  return false;
}

function tokenizeSubject_(text) {
  return normalizeText_(text)
    .split(' ')
    .map(function (s) { return s.trim(); })
    .filter(Boolean);
}

function normalizeText_(text) {
  return String(text || '')
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/[\u0000-\u001f]+/g, ' ')
    .replace(/[^\p{L}\p{N}_]+/gu, ' ')
    .replace(/\s+/g, ' ')
    .trim();
}

function normalizeHeaderCell_(value) {
  return String(value || '')
    .toLowerCase()
    .replace(/ё/g, 'е')
    .replace(/&nbsp;/gi, ' ')
    .replace(/[\r\n\t]+/g, ' ')
    .replace(/[\s\-]+/g, '_')
    .replace(/[^a-z0-9_]+/g, '')
    .replace(/^_+|_+$/g, '');
}

function rowHasUtmHeader_(cells) {
  for (let i = 0; i < cells.length; i++) {
    const cell = normalizeHeaderCell_(cells[i]);
    if (UTM_HEADERS[cell]) return true;
  }
  return false;
}

function isClientTableBlob_(blob) {
  const kind = detectBlobKind_(blob);
  if (!kind) return false;

  try {
    if (kind === 'csv') return csvHasUtmHeader_(blob);
    if (kind === 'xlsx') return xlsxHasUtmHeader_(blob);
    if (kind === 'html') return htmlHasUtmHeader_(blob);
  } catch (err) {
    console.error('BLOB PARSE ERROR: ' + (blob.getName ? blob.getName() : 'unknown') + ' :: ' + err.message);
  }

  return false;
}

function detectBlobKind_(blob) {
  const name = String(blob.getName ? blob.getName() : '').toLowerCase();
  const contentType = String(blob.getContentType ? blob.getContentType() : '').toLowerCase();

  if (name.endsWith('.xlsx') || contentType.indexOf('openxmlformats-officedocument.spreadsheetml.sheet') !== -1) {
    return 'xlsx';
  }

  if (
    name.endsWith('.csv') ||
    name.endsWith('.tsv') ||
    name.endsWith('.txt') ||
    contentType.indexOf('text/csv') !== -1 ||
    contentType.indexOf('text/plain') !== -1
  ) {
    return 'csv';
  }

  if (
    name.endsWith('.html') ||
    name.endsWith('.htm') ||
    contentType.indexOf('text/html') !== -1
  ) {
    return 'html';
  }

  if (name.endsWith('.xls')) {
    const text = safeGetString_(blob);
    if (/<table[\s>]/i.test(text)) return 'html';
  }

  return null;
}

function explodeIfZip_(blob) {
  const name = String(blob.getName ? blob.getName() : '').toLowerCase();
  const contentType = String(blob.getContentType ? blob.getContentType() : '').toLowerCase();

  if (name.endsWith('.zip') || contentType.indexOf('zip') !== -1) {
    try {
      return Utilities.unzip(blob.copyBlob());
    } catch (err) {
      console.error('ZIP ERROR: ' + name + ' :: ' + err.message);
      return [];
    }
  }

  return [blob];
}

function csvHasUtmHeader_(blob) {
  let text = safeGetString_(blob);
  if (!text) return false;

  text = stripBom_(text);
  const delimiter = detectDelimiter_(text);

  let rows;
  try {
    rows = Utilities.parseCsv(text, delimiter);
  } catch (err) {
    return false;
  }

  const limit = Math.min(rows.length, CONFIG.HEADER_SCAN_ROWS);
  for (let i = 0; i < limit; i++) {
    if (rowHasUtmHeader_(rows[i])) return true;
  }

  return false;
}

function htmlHasUtmHeader_(blob) {
  const html = safeGetString_(blob);
  if (!html) return false;

  const rows = html.match(/<tr\b[\s\S]*?<\/tr>/gi) || [];
  const limit = Math.min(rows.length, CONFIG.HEADER_SCAN_ROWS);

  for (let i = 0; i < limit; i++) {
    const rowHtml = rows[i];
    const cells = [];
    const re = /<t[dh]\b[^>]*>([\s\S]*?)<\/t[dh]>/gi;
    let m;

    while ((m = re.exec(rowHtml)) !== null) {
      cells.push(stripTags_(decodeHtmlEntities_(m[1])));
    }

    if (cells.length && rowHasUtmHeader_(cells)) return true;
  }

  return false;
}

function xlsxHasUtmHeader_(blob) {
  const parts = Utilities.unzip(blob.copyBlob());
  const files = Object.create(null);

  for (let i = 0; i < parts.length; i++) {
    files[parts[i].getName()] = parts[i];
  }

  if (!files['xl/workbook.xml']) return false;

  const workbookXml = safeGetString_(files['xl/workbook.xml']);
  const relsXml = files['xl/_rels/workbook.xml.rels'] ? safeGetString_(files['xl/_rels/workbook.xml.rels']) : '';
  const sharedStringsXml = files['xl/sharedStrings.xml'] ? safeGetString_(files['xl/sharedStrings.xml']) : '';

  const relMap = parseWorkbookRels_(relsXml);
  const sheetPaths = parseSheetPaths_(workbookXml, relMap);
  const sharedStrings = parseSharedStrings_(sharedStringsXml);

  for (let i = 0; i < sheetPaths.length; i++) {
    const path = sheetPaths[i];
    if (!files[path]) continue;

    const sheetXml = safeGetString_(files[path]);
    if (sheetXmlHasUtmHeader_(sheetXml, sharedStrings)) return true;
  }

  return false;
}

function parseWorkbookRels_(xml) {
  const map = Object.create(null);
  if (!xml) return map;

  const re = /<Relationship\b[^>]*\bId="([^"]+)"[^>]*\bTarget="([^"]+)"[^>]*\/?>/gi;
  let m;

  while ((m = re.exec(xml)) !== null) {
    map[m[1]] = m[2];
  }

  return map;
}

function parseSheetPaths_(workbookXml, relMap) {
  const paths = [];
  if (!workbookXml) return paths;

  const re = /<sheet\b[^>]*\br:id="([^"]+)"[^>]*\/?>/gi;
  let m;

  while ((m = re.exec(workbookXml)) !== null) {
    const relId = m[1];
    const target = relMap[relId];
    if (!target) continue;
    paths.push(resolveXlsxPath_(target));
  }

  return paths;
}

function resolveXlsxPath_(target) {
  let t = String(target || '').replace(/^\/+/, '');
  if (t.indexOf('xl/') === 0) return t;
  if (t.indexOf('../') === 0) t = t.replace(/^(\.\.\/)+/, '');
  return 'xl/' + t;
}

function parseSharedStrings_(xml) {
  const result = [];
  if (!xml) return result;

  const siList = xml.match(/<si\b[\s\S]*?<\/si>/gi) || [];
  for (let i = 0; i < siList.length; i++) {
    const si = siList[i];
    const texts = [];
    const re = /<t(?:\s[^>]*)?>([\s\S]*?)<\/t>/gi;
    let m;

    while ((m = re.exec(si)) !== null) {
      texts.push(decodeXmlEntities_(m[1]));
    }

    result.push(texts.join(''));
  }

  return result;
}

function sheetXmlHasUtmHeader_(xml, sharedStrings) {
  if (!xml) return false;

  const rows = xml.match(/<row\b[\s\S]*?<\/row>/gi) || [];
  const limit = Math.min(rows.length, CONFIG.HEADER_SCAN_ROWS);

  for (let i = 0; i < limit; i++) {
    const cells = extractXlsxRowCells_(rows[i], sharedStrings);
    if (cells.length && rowHasUtmHeader_(cells)) return true;
  }

  return false;
}

function extractXlsxRowCells_(rowXml, sharedStrings) {
  const cells = [];
  const re = /<c\b([^>]*)>([\s\S]*?)<\/c>/gi;
  let m;

  while ((m = re.exec(rowXml)) !== null) {
    const attrs = m[1] || '';
    const body = m[2] || '';
    const typeMatch = attrs.match(/\bt="([^"]+)"/i);
    const type = typeMatch ? typeMatch[1] : '';

    if (type === 'inlineStr') {
      const texts = [];
      const tRe = /<t(?:\s[^>]*)?>([\s\S]*?)<\/t>/gi;
      let t;
      while ((t = tRe.exec(body)) !== null) {
        texts.push(decodeXmlEntities_(t[1]));
      }
      cells.push(texts.join(''));
      continue;
    }

    const vMatch = body.match(/<v>([\s\S]*?)<\/v>/i);
    if (!vMatch) continue;

    const rawValue = decodeXmlEntities_(vMatch[1]);

    if (type === 's') {
      const idx = Number(rawValue);
      cells.push(isNaN(idx) ? '' : (sharedStrings[idx] || ''));
    } else {
      cells.push(rawValue);
    }
  }

  return cells;
}

function detectDelimiter_(text) {
  const lines = text.split(/\r\n|\n|\r/).filter(Boolean).slice(0, 5);
  if (!lines.length) return ',';

  const candidates = [',', ';', '\t', '|'];
  let best = ',';
  let bestScore = -1;

  for (let i = 0; i < candidates.length; i++) {
    const delim = candidates[i];
    let score = 0;

    for (let j = 0; j < lines.length; j++) {
      score += lines[j].split(delim).length - 1;
    }

    if (score > bestScore) {
      bestScore = score;
      best = delim;
    }
  }

  return best;
}

function getOrCreateChildFolder_(rootFolder, subject, folderCache, stats) {
  const name = safeFolderName_(subject);

  if (folderCache[name]) return folderCache[name];

  const it = rootFolder.getFoldersByName(name);
  let folder;

  if (it.hasNext()) {
    folder = it.next();
  } else {
    folder = rootFolder.createFolder(name);
    stats.createdFolders++;
  }

  folderCache[name] = folder;
  return folder;
}

function saveBlob_(blob, folder, messageDate) {
  const ts = Utilities.formatDate(messageDate || new Date(), Session.getScriptTimeZone(), 'yyyyMMdd_HHmmss');
  const originalName = safeFileName_(blob.getName ? blob.getName() : 'file');
  const finalName = ts + '__' + originalName;
  folder.createFile(blob.copyBlob().setName(finalName));
}

function safeFolderName_(name) {
  return String(name || 'без темы')
    .replace(/[\\\/:*?"<>|#%]+/g, ' ')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 180) || 'без темы';
}

function safeFileName_(name) {
  return String(name || 'file')
    .replace(/[\\\/:*?"<>|#%]+/g, '_')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, 180) || 'file';
}

function safeGetString_(blob) {
  try {
    return blob.getDataAsString();
  } catch (err) {
    return '';
  }
}

function stripBom_(text) {
  return String(text || '').replace(/^\uFEFF/, '');
}

function stripTags_(html) {
  return String(html || '').replace(/<[^>]+>/g, ' ').replace(/\s+/g, ' ').trim();
}

function decodeXmlEntities_(s) {
  return String(s || '')
    .replace(/&#10;/g, '\n')
    .replace(/&#13;/g, '\r')
    .replace(/&#9;/g, '\t')
    .replace(/&quot;/g, '"')
    .replace(/&apos;/g, "'")
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&');
}

function decodeHtmlEntities_(s) {
  return String(s || '')
    .replace(/&nbsp;/gi, ' ')
    .replace(/&#160;/g, ' ')
    .replace(/&#39;/g, "'")
    .replace(/&quot;/g, '"')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&amp;/g, '&');
}

function installHourlyTrigger() {
  ScriptApp.newTrigger('run')
    .timeBased()
    .everyHours(1)
    .create();
}
```

## Как запускать

1. Создаёшь **standalone Apps Script** под `ya-stats@solta.io`.
2. Вставляешь код.
3. Первый запуск — `run()`.
4. Даёшь доступы.
5. Если нужен автозапуск — один раз запускаешь `installHourlyTrigger()`.

## Что ещё важно

* Скрипт метит обработанные треды лейблом `solta/utm_processed`.
* Если захочешь прогнать всё заново — просто сними этот label у нужных тредов.
* Если целевая папка **не Shared Drive**, последнее требование про “не на Drive аккаунта запуска” технически не гарантируется. Для железобетонного варианта папку надо держать именно в Shared Drive. ([Google for Developers][2])

Если вставишь это как есть, оно уже поедет.

[1]: https://developers.google.com/apps-script/reference/gmail/gmail-app "Class GmailApp  |  Apps Script  |  Google for Developers"
[2]: https://developers.google.com/workspace/drive/api/guides/about-shareddrives "Shared drives overview  |  Google Drive  |  Google for Developers"
