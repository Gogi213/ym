export function assertAuthorized(req: Request) {
  const expectedToken = Deno.env.get('INGEST_TOKEN') || ''
  const actualToken = req.headers.get('x-ingest-token') || ''

  if (!expectedToken || actualToken !== expectedToken) {
    throw new Error('Unauthorized')
  }
}
