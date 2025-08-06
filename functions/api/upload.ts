export const SUPABASE_URL = "https://idtwjchmeldqwurigvkx.supabase.co";
export const SUPABASE_KEY = "sb_publishable_NXoYAsEV8RMXoVZggIeVAg_eGrfwnVb";

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

    try {
      const res = await fetch(`${SUPABASE_URL}/rest/v1/inventory`, {
        method: "POST",
        headers: {
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`,
          "Content-Type": "application/json",
          Prefer: "return=representation",
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

      if (!res.ok) {
        const error = await res.text();
        skippedRows.push({ ...row, reason: `Supabase error: ${error}` });
        continue;
      }

      const data = await res.json();
      insertedRows.push(data[0]);
    } catch (err) {
      skippedRows.push({ ...row, reason: `Exception: ${err.message}` });
    }
  }

  return new Response(
    JSON.stringify({ inserted: insertedRows.length, skipped: skippedRows.length, skippedRows }),
    { headers: { "Content-Type": "application/json" } }
  );
}
