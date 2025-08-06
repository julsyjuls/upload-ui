export async function onRequestPost(context) {
  try {
    const body = await context.request.json();
    const rows = body.rows;

    const SUPABASE_URL = context.env.SUPABASE_URL;
    const SUPABASE_KEY = context.env.SUPABASE_SERVICE_ROLE_KEY;

    // 🔍 Get sku_id from sku_code
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

    // 🔍 or ➕ Get/create batch_id from batch_no
    async function getOrCreateBatchId(batch_no) {
      const fetchUrl = `${SUPABASE_URL}/rest/v1/batches?batch_no=eq.${encodeURIComponent(batch_no)}&select=id&limit=1`;
      const fetchRes = await fetch(fetchUrl, {
        headers: {
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`,
        },
      });
      const fetchData = await fetchRes.json();

      if (fetchData.length > 0) {
        return fetchData[0].id;
      }

      // ➕ Insert batch if not found
      const insertRes = await fetch(`${SUPABASE_URL}/rest/v1/batches`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          apikey: SUPABASE_KEY,
          Authorization: `Bearer ${SUPABASE_KEY}`,
          Prefer: "return=representation",
        },
        body: JSON.stringify([{ batch_no }]),
      });

      const insertData = await insertRes.json();
      console.log("🆕 Created batch:", insertData[0]);
      return insertData[0]?.id || null;
    }

    const transformedRows = [];

    for (const row of rows) {
      const { sku_code, batch_no, barcode, date_in, warranty_months } = row;

      const sku_id = await getSkuId(sku_code);
      const batch_id = await getOrCreateBatchId(batch_no);

      console.log("🔎 Lookup Result:", { sku_code, sku_id, batch_no, batch_id });

      if (!sku_id || !batch_id) {
        console.warn(`❌ Skipping row due to missing lookup:`, row);
        continue;
      }

      const finalRow = {
        sku_id,
        batch_id,
        barcode,
        date_in,
        warranty_months: parseInt(warranty_months),
      };

      console.log("✅ Row ready to insert:", finalRow);

      transformedRows.push(finalRow);
    }

    console.log(`📦 Total rows ready for insert: ${transformedRows.length}`);

    if (transformedRows.length === 0) {
      return new Response(JSON.stringify({ error: "No valid rows to insert." }), {
        status: 400,
        headers: { "Content-Type": "application/json" },
      });
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

    const insertData = await response.json();
    console.log("🧾 Insert response:", insertData);

    if (!response.ok) {
      console.error("🧨 Supabase insert error:", insertData);
      return new Response(JSON.stringify({ error: "Supabase insert failed", detail: insertData }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }

    return new Response(JSON.stringify({ message: "Upload successful", inserted: insertData.length }), {
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
