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

    // Insert into inventory — rely on trigger to resolve sku_id and batch_id
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
          sku_code,
          batch_no,
          barcode,
          date_in,
          warranty_months: warranty_months ? parseInt(warranty_months) : null,
        },
      ]),
    });

    const resultText = await insertRes.text();
    let result;

    try {
      result = JSON.parse(resultText);
    } catch (err) {
      skippedRows.push({ ...row, reason: "Invalid JSON from Supabase", raw: resultText });
      continue;
    }

    if (insertRes.ok) {
      insertedRows.push(row);
    } else {
      skippedRows.push({ ...row, reason: result.message || "Insert failed", raw: resultText });
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
}
