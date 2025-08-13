export const SUPABASE_URL = "https://idtwjchmeldqwurigvkx.supabase.co";

export async function onRequestPost(context) {
  const { SUPABASE_SERVICE_ROLE_KEY } = context.env;
  const SUPABASE_KEY = SUPABASE_SERVICE_ROLE_KEY;

  const { rows } = await context.request.json();
  const insertedRows: any[] = [];
  const skippedRows: any[] = [];

  // ---- Simple in-memory cache for (sku_code|brand_name) lookups ----
  const skuCache = new Map<string, any[]>();

  async function fetchJson(url: string) {
    const res = await fetch(url, {
      headers: {
        apikey: SUPABASE_KEY,
        Authorization: `Bearer ${SUPABASE_KEY}`,
      },
    });
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  }

  // Helper to query skus by (sku_code, brand_name) with cache
  async function fetchSkuByCodeAndBrand(sku_code: string, brand_name: string) {
    const key = `${sku_code}|${brand_name}`;
    if (skuCache.has(key)) return skuCache.get(key)!;

    const url =
      `${SUPABASE_URL}/rest/v1/skus` +
      `?sku_code=eq.${encodeURIComponent(sku_code)}` +
      `&brand_name=eq.${encodeURIComponent(brand_name)}` +
      `&select=id,sku_code,brand_name`;

    const data = await fetchJson(url);
    skuCache.set(key, data);
    return data as Array<{ id: string; sku_code: string; brand_name: string }>;
  }

  // Optional helpers for friendlier “what’s wrong” messages
  async function codeExists(sku_code: string) {
    try {
      const data = await fetchJson(
        `${SUPABASE_URL}/rest/v1/skus?sku_code=eq.${encodeURIComponent(sku_code)}&select=id&limit=1`
      );
      return Array.isArray(data) && data.length > 0;
    } catch {
      return false;
    }
  }
  async function brandExists(brand_name: string) {
    try {
      const data = await fetchJson(
        `${SUPABASE_URL}/rest/v1/skus?brand_name=eq.${encodeURIComponent(brand_name)}&select=id&limit=1`
      );
      return Array.isArray(data) && data.length > 0;
    } catch {
      return false;
    }
  }

  for (const rawRow of rows) {
    // Minimal normalization (keep change safe)
    const row = {
      sku_code: (rawRow?.sku_code ?? "").trim(),
      brand_name: (rawRow?.brand_name ?? "").trim(),
      batch_no: (rawRow?.batch_no ?? "").trim(),
      barcode: (rawRow?.barcode ?? "").trim(),
      date_in: (rawRow?.date_in ?? "").trim(),
      warranty_months: rawRow?.warranty_months,
    };

    const { sku_code, brand_name, batch_no, barcode, date_in, warranty_months } = row;

    // Required fields (now includes brand_name)
    if (!sku_code || !brand_name || !batch_no || !barcode || !date_in) {
      skippedRows.push({
        ...row,
        reason:
          "❌ Missing required field(s). Required: sku_code, brand_name, batch_no, barcode, date_in",
      });
      continue;
    }

    try {
      // Validate that (sku_code, brand_name) exists and is unique in SKUs
      const skuMatches = await fetchSkuByCodeAndBrand(sku_code, brand_name);

      if (skuMatches.length === 0) {
        // Friendlier hint: which part is likely wrong?
        let reason = `❌ SKU not found for combination (sku_code="${sku_code}", brand_name="${brand_name}"). Please check exact spelling/case/spaces.`;
        try {
          const [codeOk, brandOk] = await Promise.all([
            codeExists(sku_code),
            brandExists(brand_name),
          ]);
          if (codeOk && !brandOk) {
            reason = `❌ Brand name "${brand_name}" not found for the given SKU code "${sku_code}". Check exact spelling/case/spaces.`;
          } else if (!codeOk && brandOk) {
            reason = `❌ SKU code "${sku_code}" not found (brand "${brand_name}" exists). Check exact spelling/case/spaces.`;
          }
        } catch {
          // If the hint checks fail silently, keep the generic reason.
        }

        skippedRows.push({ ...row, reason });
        continue;
      }

      if (skuMatches.length > 1) {
        skippedRows.push({
          ...row,
          reason: `❌ Duplicate SKU definition found in SKUs table for (sku_code="${sku_code}", brand_name="${brand_name}"). Please clean SKUs table.`,
        });
        continue;
      }

      // Proceed with insert (Option B): send brand_name so trigger can resolve sku_id by BOTH fields
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
            sku_code,           // keep for reference
            brand_name,         // 👈 REQUIRED so the trigger can match by both
            batch_no,
            barcode,
            date_in,
            warranty_months: warranty_months ? parseInt(String(warranty_months)) : null,
            // If you ever want to bypass trigger resolution and set directly:
            // sku_id: skuMatches[0].id,
          },
        ]),
      });

      if (!res.ok) {
        const errorText = await res.text();
        let reason = `❌ Supabase error`;

        if (errorText.includes("inventory_barcode_key")) {
          reason = `❌ Barcode "${barcode}" already exists in inventory`;
        } else if (errorText.includes("brand_name is NULL")) {
          reason = `❌ brand_name is missing in payload (inventory insert).`;
        } else if (errorText.includes("not found in skus") || sku_code === "EMP-NONE") {
          reason = `❌ SKU code "${sku_code}" not found in SKUs table`;
        } else {
          reason = `❌ Supabase error: ${errorText}`;
        }

        skippedRows.push({ ...row, reason });
        continue;
      }

      const data = await res.json();
      insertedRows.push(data[0]);
    } catch (err: any) {
      skippedRows.push({ ...row, reason: `❌ Exception: ${err.message}` });
    }
  }

  return new Response(
    JSON.stringify({
      inserted: insertedRows.length,
      skipped: skippedRows.length,
      skippedRows,
    }),
    { headers: { "Content-Type": "application/json" } }
  );
}
