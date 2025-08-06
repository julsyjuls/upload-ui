export const SUPABASE_URL = "https://YOUR_PROJECT.supabase.co";
export const SUPABASE_KEY = "sbp_xxx..."; // your anon/public key

export async function onRequestPost(context) {
  const { rows } = await context.request.json();
  const insertedRows = [];
  const skippedRows = [];

  for (const row of rows) {
    const { sku_code, batch_no, barcode, date_in, warranty_months } = row;

    if (!sku_code || !batch_no || !barcode || !date_in) {
      skippedRows.push({ ...row, reason: "Missing required field(s)" });
      continue;
    }

    // Get SKU ID
    const skuRes = await fetch(`${SUPABASE_URL}/rest/v1/skus?sku_code=eq.${sku_code}`, {
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
      },
    });
    const skuData = await skuRes.json();
    const sku_id = skuData[0]?.id;

    if (!sku_id) {
      skippedRows.push({ ...row, reason: `No SKU found for code "${sku_code}"` });
      continue;
    }

    // Insert into inventory (will trigger linking to batch and SKU)
    const insertRes = await fetch(`${SUPABASE_URL}/rest/v1/inventory`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
        Prefer: "resolution=merge-duplicates",
      },
      body: JSON.stringify([
        {
          sku_id,
          sku_code,
          batch_no,
          barcode,
          date_in,
          warranty_months: warranty_months ? parseInt(warranty_months) : null,
        },
      ]),
    });

    const result = await insertRes.json();
    if (insertRes.ok) {
      insertedRows.push(row);
    } else {
      skippedRows.push({ ...row, reason: result.message || "Insert failed" });
    }
  }

  return new Response(
    JSON.stringify(
      insertedRows.length
        ? { success: true, inserted: insertedRows.length, skipped_rows: skippedRows }
        : { error: "Insert failed", skipped_rows: skippedRows }
    ),
    { headers: { "Content-Type": "application/json" } }
);

