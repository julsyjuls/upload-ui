export async function onRequestPost(context) {
  try {
    const body = await context.request.json();
    const rows = body.rows;

    const SUPABASE_URL = context.env.SUPABASE_URL;
    const SUPABASE_KEY = context.env.SUPABASE_SERVICE_ROLE_KEY;

    // Helper to fetch ID from a table
    async function getIdByValue(table, keyField, valueField, value) {
      const url = `${SUPABASE_URL}/rest/v1/${table}?${valueField}=eq.${encodeURIComponent(value)}&select=${keyField}&limit=1`;
      const res = await fetch(url, {
        headers: {
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`,
        },
      });
      const data = await res.json();
      return data[0]?.[keyField] || null;
    }

    const transformedRows = [];

    for (const row of rows) {
      const { sku_code, batch_no, barcode, date_in, warranty_months } = row;

      const sku_id = await getIdByValue("skus", "id", "sku_code", sku_code);
      const batch_id = await getIdByValue("batches", "id", "batch_no", batch_no);

      if (!sku_id || !batch_id) {
        console.warn(`❌ Could not find SKU or Batch for row:`, row);
        continue; // Skip rows that don't resolve
      }

      transformedRows.push({
        sku_id,
        batch_id,
        barcode,
        date_in,
        warranty_months: parseInt(warranty_months), // ensure it's a number
      });
    }

    // Upload to inventory
    const response = await fetch(`${SUPABASE_URL}/rest/v1/inventory`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
        Prefer: "resolution=merge-duplicates",
      },
      body: JSON.stringify(transformedRows),
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error("🧨 Supabase error response:", errorText);
      return new Response(JSON.stringify({ error: "Supabase error", detail: errorText }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }

    return new Response(JSON.stringify({ message: "Upload successful" }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });

  } catch (error) {
    console.error("❌ Worker error:", error.message);
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
}
