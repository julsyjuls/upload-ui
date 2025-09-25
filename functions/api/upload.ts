// upload.ts — Batched CSV upload (v2: legacy keys + safer IN encoding)

export const SUPABASE_URL = "https://idtwjchmeldqwurigvkx.supabase.co";

// ---------- tunables ----------
const INVENTORY_CHUNK_SIZE = 400;
const MAX_SKIPPED_RETURN = 200;
// --------------------------------

type RawRow = {
  sku_code?: string;
  brand_name?: string;
  batch_no?: string;
  barcode?: string;
  date_in?: string;
  warranty_months?: number | string | null;
};

type NormalizedRow = {
  sku_code: string;
  brand_name: string;
  batch_no: string;
  barcode: string;
  date_in: string;
  warranty_months: number | null;
  __rowIndex: number;
};

export async function onRequestPost(context: any) {
  const { SUPABASE_SERVICE_ROLE_KEY } = context.env;
  const SB_KEY = SUPABASE_SERVICE_ROLE_KEY as string;

  try {
    const body = await context.request.json();
    const inputRows: RawRow[] = Array.isArray(body?.rows) ? body.rows : [];

    if (!SB_KEY) return json({ error: "Missing SUPABASE_SERVICE_ROLE_KEY" }, 500);
    if (!Array.isArray(inputRows) || inputRows.length === 0)
      return json({ error: "No rows provided" }, 400);

    const rows: NormalizedRow[] = [];
    const skipped: any[] = [];

    for (let i = 0; i < inputRows.length; i++) {
      const r = inputRows[i] || {};
      const row: NormalizedRow = {
        sku_code: (r.sku_code ?? "").trim(),
        brand_name: (r.brand_name ?? "").trim(),
        batch_no: (r.batch_no ?? "").trim(),
        barcode: (r.barcode ?? "").trim(),
        date_in: normalizeDate(r.date_in),
        warranty_months: normalizeInt(r.warranty_months),
        __rowIndex: i,
      };

      if (!row.sku_code || !row.brand_name || !row.batch_no || !row.barcode || !row.date_in) {
        skipped.push({
          ...row,
          reason:
            "❌ Missing required field(s). Required: sku_code, brand_name, batch_no, barcode, date_in",
        });
        continue;
      }
      rows.push(row);
    }

    if (rows.length === 0) {
      return json({
        added: 0,
        skipped: skipped.length,
        skippedRows: skipped.slice(0, MAX_SKIPPED_RETURN),
        note: "All rows skipped due to missing required fields",
      });
    }

    // ---------- Build sets ----------
    const skuCodes = new Set(rows.map((r) => r.sku_code));
    const brandNames = new Set(rows.map((r) => r.brand_name));
    const batchKeys = new Set(rows.map((r) => keyBatch(r.sku_code, r.brand_name, r.batch_no)));
    const batchEarliestDateIn = new Map<string, string>();
    for (const r of rows) {
      const bk = keyBatch(r.sku_code, r.brand_name, r.batch_no);
      const ex = batchEarliestDateIn.get(bk);
      if (!ex || r.date_in < ex) batchEarliestDateIn.set(bk, r.date_in);
    }

    // ---------- Helpers ----------
    const baseHeaders = {
      apikey: SB_KEY,
      Authorization: `Bearer ${SB_KEY}`,
      "Content-Type": "application/json",
    };

    const fetchJson = async (url: string, init?: RequestInit) => {
      const res = await fetch(url, { headers: baseHeaders, ...(init || {}) });
      if (!res.ok) throw new Error(await safeText(res));
      return res.json();
    };

    // ---------- 1) Preload SKUs (handles encoding for in.(...)) ----------
    const sku_code_in = buildInList([...skuCodes]);
    const brand_name_in = buildInList([...brandNames]);

    let skus: Array<{ id: string; sku_code: string; brand_name: string }>;
    try {
      const url =
        `${SUPABASE_URL}/rest/v1/skus` +
        `?select=id,sku_code,brand_name` +
        `&sku_code=in.(${sku_code_in})` +
        `&brand_name=in.(${brand_name_in})`;
      skus = await fetchJson(url);
    } catch (e: any) {
      return json({ error: `Failed to preload SKUs: ${e.message}` }, 500);
    }

    const skuMap = new Map<string, { id: string; sku_code: string; brand_name: string }>();
    for (const s of skus) skuMap.set(keySku(s.sku_code, s.brand_name), s);

    const usableRows: NormalizedRow[] = [];
    for (const r of rows) {
      if (!skuMap.has(keySku(r.sku_code, r.brand_name))) {
        skipped.push({
          ...r,
          reason: `❌ SKU not found for (sku_code="${r.sku_code}", brand_name="${r.brand_name}")`,
        });
      } else {
        usableRows.push(r);
      }
    }

    if (usableRows.length === 0) {
      return json({
        added: 0,
        skipped: skipped.length,
        skippedRows: skipped.slice(0, MAX_SKIPPED_RETURN),
        note: "All rows skipped due to missing SKUs",
      });
    }

    // ---------- 2) Preload batches ----------
    const skuIdSet = new Set<string>();
    for (const r of usableRows) skuIdSet.add(skuMap.get(keySku(r.sku_code, r.brand_name))!.id);

    const sku_id_in = buildInList([...skuIdSet]);
    const batch_no_in = buildInList([...new Set(usableRows.map((r) => r.batch_no))]);

    let batches: Array<{ id: string; sku_id: string; batch_no: string }>;
    try {
      const url =
        `${SUPABASE_URL}/rest/v1/batches` +
        `?select=id,sku_id,batch_no` +
        `&sku_id=in.(${sku_id_in})` +
        `&batch_no=in.(${batch_no_in})`;
      batches = await fetchJson(url);
    } catch (e: any) {
      return json({ error: `Failed to preload Batches: ${e.message}` }, 500);
    }

    const batchMap = new Map<string, { id: string; sku_id: string; batch_no: string }>();
    for (const b of batches) batchMap.set(`${b.sku_id}|${b.batch_no}`, b);

    // ---------- 3) Upsert missing batches ----------
    const missingBatchPayload: Array<{ sku_id: string; batch_no: string; date_in?: string }> = [];
    for (const bk of batchKeys) {
      const [sku_code, brand_name, batch_no] = splitBatchKey(bk);
      const sku = skuMap.get(keySku(sku_code, brand_name))!;
      const k = `${sku.id}|${batch_no}`;
      if (!batchMap.has(k)) {
        const earliest = batchEarliestDateIn.get(bk);
        missingBatchPayload.push({ sku_id: sku.id, batch_no, ...(earliest ? { date_in: earliest } : {}) });
      }
    }

    if (missingBatchPayload.length > 0) {
      try {
        await fetch(`${SUPABASE_URL}/rest/v1/batches?on_conflict=sku_id,batch_no`, {
          method: "POST",
          headers: { ...baseHeaders, Prefer: "resolution=merge-duplicates,return=minimal" },
          body: JSON.stringify(missingBatchPayload),
        });
      } catch (e: any) {
        return json({ error: `Failed to upsert batches: ${String(e.message || e)}` }, 500);
      }

      // re-fetch
      try {
        const url =
          `${SUPABASE_URL}/rest/v1/batches` +
          `?select=id,sku_id,batch_no` +
          `&sku_id=in.(${sku_id_in})` +
          `&batch_no=in.(${batch_no_in})`;
        batches = await fetchJson(url);
        batchMap.clear();
        for (const b of batches) batchMap.set(`${b.sku_id}|${b.batch_no}`, b);
      } catch (e: any) {
        return json({ error: `Failed to re-fetch batches: ${e.message}` }, 500);
      }
    }

    // ---------- 4) Build inventory payload ----------
    const inventoryPayload: any[] = [];
    for (const r of usableRows) {
      const sku = skuMap.get(keySku(r.sku_code, r.brand_name))!;
      const b = batchMap.get(`${sku.id}|${r.batch_no}`);
      if (!b) {
        skipped.push({
          ...r,
          reason: `❌ Batch not resolved for (sku="${r.sku_code}|${r.brand_name}", batch_no="${r.batch_no}")`,
        });
        continue;
      }

      inventoryPayload.push({
        sku_code: r.sku_code,
        brand_name: r.brand_name,
        batch_no: r.batch_no,
        sku_id: sku.id,
        batch_id: b.id,
        barcode: r.barcode,
        date_in: r.date_in,
        warranty_months: r.warranty_months,
      });
    }

    if (inventoryPayload.length === 0) {
      return json({
        added: 0,
        skipped: skipped.length,
        skippedRows: skipped.slice(0, MAX_SKIPPED_RETURN),
        note: "Nothing to insert after resolving SKUs/Batches.",
      });
    }

    // ---------- 5) Chunked insert ----------
    let insertedCount = 0;
    let duplicateCount = 0;

    for (let i = 0; i < inventoryPayload.length; i += INVENTORY_CHUNK_SIZE) {
      const chunk = inventoryPayload.slice(i, i + INVENTORY_CHUNK_SIZE);

      const res = await fetch(`${SUPABASE_URL}/rest/v1/inventory?on_conflict=barcode`, {
        method: "POST",
        headers: { ...baseHeaders, Prefer: "resolution=ignore-duplicates,return=representation" },
        body: JSON.stringify(chunk),
      });

      if (!res.ok) {
        const err = await safeText(res);
        // mark all rows in this chunk as skipped with the same error
        for (const bad of chunk) {
          skipped.push({ ...bad, reason: `❌ Supabase error (bulk insert): ${truncate(err, 300)}` });
        }
        continue;
      }

      const data = await res.json();
      const inserted = Array.isArray(data) ? data.length : 0;
      insertedCount += inserted;
      duplicateCount += chunk.length - inserted;
    }

    // ---------- 6) Respond (legacy keys preserved) ----------
    return json({
      // legacy
      added: insertedCount,               // 👈 what your UI expects
      skipped: skipped.length,            // 👈 what your UI expects

      // detailed
      inserted: insertedCount,
      duplicates_skipped: duplicateCount,
      skippedRows: skipped.slice(0, MAX_SKIPPED_RETURN),
      note:
        "Batched mode: preloaded SKUs, upserted batches, and inserted inventory in chunks (on_conflict=barcode).",
    });
  } catch (e: any) {
    return json({ error: `Unexpected error: ${String(e?.message || e)}` }, 500);
  }
}

// ---------------- helpers ----------------

function json(payload: any, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function keySku(code: string, brand: string) {
  return `${code}┃${brand}`;
}

function keyBatch(code: string, brand: string, batchNo: string) {
  return `${code}┃${brand}┃${batchNo}`;
}

function splitBatchKey(k: string): [string, string, string] {
  const [code, brand, batchNo] = k.split("┃");
  return [code, brand, batchNo];
}

function normalizeDate(v: any): string {
  if (!v) return "";
  const t = String(v).trim();
  const d = new Date(t);
  if (isNaN(d.getTime())) return t;
  return d.toISOString().slice(0, 10);
}

function normalizeInt(v: any): number | null {
  if (v === null || v === undefined || v === "") return null;
  const n = parseInt(String(v), 10);
  return Number.isFinite(n) ? n : null;
}

/**
 * Build a URL-safe PostgREST in.(...) list.
 * Produces something like: %22VAL1%22,%22VAL%202%22
 * (i.e., values quoted, then fully percent-encoded)
 */
function buildInList(values: string[]): string {
  return values
    .map((s) => `"${s.replace(/"/g, '\\"')}"`) // quote and escape quotes
    .map((q) => encodeURIComponent(q))          // encode quotes/spaces/etc.
    .join(",");
}

async function safeText(res: Response) {
  try {
    return await res.text();
  } catch {
    return `${res.status} ${res.statusText}`;
  }
}

function truncate(s: string, n: number) {
  return s && s.length > n ? s.slice(0, n) + "…" : s;
}
