import Papa from 'https://cdn.skypack.dev/papaparse'

export async function onRequestPost(context: any) {
  const { SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY } = context.env
  const body = await context.request.text()

  const parsed = Papa.parse(body, {
    header: true,
    skipEmptyLines: true,
  })

  const data = parsed.data as {
    sku_code: string
    batch_no: string
    barcode: string
    date_in: string
    warranty_months: string
  }[]

  const insertedSKUs = new Set()
  const insertedBatches = new Set()

  const results = []

  for (const row of data) {
    const res = await fetch(`${SUPABASE_URL}/rest/v1/rpc/create_inventory_with_batch_and_sku`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        apikey: SUPABASE_SERVICE_ROLE_KEY,
        Authorization: `Bearer ${SUPABASE_SERVICE_ROLE_KEY}`,
      },
      body: JSON.stringify({
        sku_code: row.sku_code,
        batch_no: row.batch_no,
        barcode: row.barcode,
        date_in: row.date_in,
        warranty_months: parseInt(row.warranty_months),
      }),
    })

    const json = await res.json()
    results.push({ status: res.status, data: json })
  }

  return new Response(JSON.stringify({ results }), {
    headers: { 'Content-Type': 'application/json' },
  })
}
