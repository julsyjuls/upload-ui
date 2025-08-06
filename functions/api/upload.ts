export async function onRequestPost(context) {
  try {
    const body = await context.request.json();
    const rows = body.rows;

    const SUPABASE_URL = context.env.SUPABASE_URL;
    const SUPABASE_KEY = context.env.SUPABASE_SERVICE_ROLE_KEY;

    async function getSkuId(sku_code) {
      const url = `${SUPABASE_URL}/rest/v1/skus?sku_code=eq.${encodeURIComponent(sku_code)}&select=id&limit=1`;
      const res = await fetch(url, {
        headers: {
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`,
        },
      });
      const data = await res.json();
      return data[0]?.id || null;
    }

    const transformedRows = [];
    const skippedRows = [];

    for (const row of rows) {
      const { sku_code, batch_no, barcode, date_in, warranty_months } = row;

      const sku_id = await getSkuId(sku_code);

      if (!sku_id) {
        skippedRows.push({
          sku_code,
          batch_no,
          reason: "Missing SKU",
        });
        continue;
      }

      transformedRows.push({
        sku_id,
        batch_no, // just text
        barcode,
        date_in,
        warranty_months: parseInt(warranty_months),
      });
    }

    if (transformedRows.length === 0) {
      return new Response(
        JSON.stringify({ error: "No valid rows to insert", skipped_rows: skippedRows }),
        { status: 400, headers: { "Content-Type": "application/json" } }
      );
    }

    const response = await fetch(`${SUPABASE_URL}/rest/v1/inventory`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
        Prefer: "return=representation",
      },
      body: JSON.stringify(transformedRows),
    });

    const result = await response.json();

    if (!response.ok) {
      return new Response(JSON.stringify({ error: "Insert failed", detail: result }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }

    return new Response(
      JSON.stringify({
        message: "Upload successful",
        inserted: result.length,
        skipped_rows: skippedRows,
      }),
      { status: 200, headers: { "Content-Type": "application/json" } }
    );
  } catch (err) {
    return new Response(JSON.stringify({ error: err.message }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
}
