import { assertAuthorized } from './auth.ts'
import { handleIngest, handleReset } from './handlers.ts'
import { JSON_HEADERS, jsonResponse } from './shared.ts'

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
