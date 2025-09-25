// upload.ts — Big-file safe: chunked preloads (smaller), CORS, phase-tagged errors, legacy UI keys

export const SUPABASE_URL = "https://idtwjchmeldqwurigvkx.supabase.co";

// ---------- tunables ----------
const INVENTORY_CHUNK_SIZE = 300;   // slightly smaller for smoother inserts
const MAX_SKIPPED_RETURN   = 200;   // cap verbose skipped list
const SKU_IN_CHUNK         = 60;    // smaller chunks -> shorter URLs, fewer 414s
const BATCH_IN_CHUNK       = 60;    // ditto
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

const CORS_HEADERS = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "POST, OPTIONS",
  "Access-Control-Allow-Headers": "Authorization, Content-Type",
  "Content-Type": "application/json",
};

export async function onRequestOptions() {
  // Proper preflight: no body for 204
  return new Response(null, { status: 204, headers: CORS_HEADERS });
}

export async function onRequestPost(context: any) {
  const { SUPABASE_SERVICE_ROLE_KEY } = context.env;
  const SB_KEY = SUPABASE_SERVICE_ROLE_KEY as string;

  // counters live here so *every* return includes them
  let insertedCount = 0;
  const skipped: any[] = [];
  let phase = "init";

  try {
    if (!SB_KEY) return json({ error: "Missing SUPABASE_SERVICE_ROLE_KEY", phase }, 500, { insertedCount, skippedArr: skipped });

    // Body parse
    let body: any = null;
    try {
      phase = "parse_body";
      body = await context.request.json();
    } catch {
      return json({ error: "Invalid JSON body", phase }, 400, { insertedCount, skippedArr: skipped });
    }

    const inputRows: RawRow[] = Array.isArray(body?.rows) ? body.rows : [];
    if (!inputRows.length) {
      return json({ error: "No rows provided (expected { rows: [...] })", phase }, 400, { insertedCount, skippedArr: skipped });
    }

    // ---------- Normalize & basic validation ----------
    phase = "normalize_rows";
    const rows: NormalizedRow[] = [];

    for (let i = 0; i < inputRows.length; i++) {
      const r = remapAliases(inputRows[i] || {});
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
          reason: "❌ Missing required field(s). Required: sku_code, brand_name, batch_no, barcode, date_in",
        });
        continue;
      }
      rows.push(row);
    }

    if (!rows.length) {
      return json({ note: "All rows skipped due to missing required fields", phase }, 200, { insertedCount, skippedArr: skipped });
    }

    // ---------- Sets ----------
    const skuCodes = new Set(rows.map((r) => r.sku_code));
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

    // ---------- 1) Preload SKUs (chunked by sku_code) ----------
    phase = "preload_skus";
    const skuCodesArr = [...skuCodes];
    const skus: Array<{ id: string; sku_code: string; brand_name: string }> = [];

    for (let i = 0; i < skuCodesArr.length; i += SKU_IN_CHUNK) {
      const part = skuCodesArr.slice(i, i + SKU_IN_CHUNK);
      const inlist = buildInList(part);
      const url = `${SUPABASE_URL}/rest/v1/skus?select=id,sku_code,brand_name&sku_code=in.(${inlist})`;
      const page = await fetchJson(url);
      if (Array.isArray(page)) skus.push(...page);
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

    if (!usableRows.length) {
      return json({ note: "All rows skipped due to missing SKUs", phase }, 200, { insertedCount, skippedArr: skipped });
    }

    // ---------- 2) Preload batches (chunked by batch_no) ----------
    phase = "preload_batches";
    const batchNosArr = [...new Set(usableRows.map((r) => r.batch_no))];
    let batches: Array<{ id: string; sku_id: string; batch_no: string }> = [];

    for (let i = 0; i < batchNosArr.length; i += BATCH_IN_CHUNK) {
      const part = batchNosArr.slice(i, i + BATCH_IN_CHUNK);
      const inlist = buildInList(part);
      const url = `${SUPABASE_URL}/rest/v1/batches?select=id,sku_id,batch_no&batch_no=in.(${inlist})`;
      const page = await fetchJson(url);
      if (Array.isArray(page)) batches.push(...page);
    }

    const batchMap = new Map<string, { id: string; sku_id: string; batch_no: string }>();
    for (const b of batches) batchMap.set(`${b.sku_id}|${b.batch_no}`, b);

    // ---------- 3) Upsert missing batches ----------
    phase = "upsert_batches";
    const missingBatchPayload: Array<{ sku_id: string; batch_no: string; date_in?: string }> = [];
    for (const bk of batchKeys) {
      const [sku_code, brand_name, batch_no] = splitBatchKey(bk);
      const sku = skuMap.get(keySku(sku_code, brand_name));
      if (!sku) continue; // already skipped above
      const k = `${sku.id}|${batch_no}`;
      if (!batchMap.has(k)) {
        const earliest = batchEarliestDateIn.get(bk);
        missingBatchPayload.push({ sku_id: sku.id, batch_no, ...(earliest ? { date_in: earliest } : {}) });
      }
    }

    if (missingBatchPayload.length) {
      const upsert = await fetch(`${SUPABASE_URL}/rest/v1/batches?on_conflict=sku_id,batch_no`, {
        method: "POST",
        headers: { ...baseHeaders, Prefer: "resolution=merge-duplicates,return=minimal" },
        body: JSON.stringify(missingBatchPayload),
      });
      if (!upsert.ok) {
        const err = await safeText(upsert);
        return json({ error: `Failed to upsert batches: ${truncate(err, 300)}`, phase }, 500, { insertedCount, skippedArr: skipped });
      }

      // Re-fetch batches once (same chunking)
      phase = "refetch_batches";
      batches = [];
      for (let i = 0; i < batchNosArr.length; i += BATCH_IN_CHUNK) {
        const part = batchNosArr.slice(i, i + BATCH_IN_CHUNK);
        const inlist = buildInList(part);
        const url = `${SUPABASE_URL}/rest/v1/batches?select=id,sku_id,batch_no&batch_no=in.(${inlist})`;
        const page = await fetchJson(url);
        if (Array.isArray(page)) batches.push(...page);
      }
      batchMap.clear();
      for (const b of batches) batchMap.set(`${b.sku_id}|${b.batch_no}`, b);
    }

    // ---------- 4) Build inventory payload ----------
    phase = "build_inventory_payload";
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

    if (!inventoryPayload.length) {
      return json({ note: "Nothing to insert after resolving SKUs/Batches.", phase }, 200, { insertedCount, skippedArr: skipped });
    }

    // ---------- 5) Chunked bulk insert ----------
    phase = "insert_inventory_chunks";
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
        for (const bad of chunk) {
          skipped.push({ ...bad, reason: `❌ Supabase error (bulk insert): ${truncate(err, 300)}` });
        }
        continue;
      }

      const data = await res.json();
      const inserted = Array.isArray(data) ? data.length : 0;
      insertedCount += inserted;

      const returnedBarcodes = new Set(
        (Array.isArray(data) ? data : []).map((r: any) => String(r.barcode))
      );

      for (const row of chunk) {
        if (!returnedBarcodes.has(String(row.barcode))) {
          duplicateCount += 1;
          skipped.push({
            ...row,
            reason: `❌ Barcode already exists (duplicate): "${row.barcode}"`,
          });
        }
      }
    }

    // ---------- 6) Done ----------
    phase = "done";
    return json({
      inserted: insertedCount,
      duplicates_skipped: duplicateCount,
      skippedRows: skipped.slice(0, MAX_SKIPPED_RETURN),
      note: "Batched mode: preloaded SKUs (chunked), upserted batches, inserted inventory (chunked).",
    }, 200, { insertedCount, skippedArr: skipped });

  } catch (e: any) {
    return json({ error: `Unexpected error: ${String(e?.message || e)}`, phase }, 500, { insertedCount, skippedArr: skipped });
  }
}

// ---------------- helpers ----------------

/** Always include UI fields + CORS headers */
function json(payload: any = {}, status = 200, counters?: { insertedCount?: number, skippedArr?: any[] }) {
  const inserted = counters?.insertedCount ?? 0;
  const skippedArr = counters?.skippedArr ?? [];
  const body = {
    // legacy/UI fields always present
    added: inserted,
    addedCount: inserted,
    skipped: skippedArr.length,
    // payload last (error/note/phase/etc.)
    ...payload,
  };
  return new Response(JSON.stringify(body), {
    status,
    headers: CORS_HEADERS,
  });
}

function remapAliases(r: any) {
  return {
    sku_code: r.sku_code ?? r.sku ?? r.code ?? "",
    brand_name: r.brand_name ?? r.brand ?? r.brandname ?? "",
    batch_no: r.batch_no ?? r.batch ?? r.batchno ?? "",
    barcode: r.barcode ?? r.code128 ?? "",
    date_in: r.date_in ?? r.date ?? r.datein ?? "",
    warranty_months: r.warranty_months ?? r.warranty ?? r.warranty_month ?? r.warrantyMonths ?? null,
  };
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

  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(t);
  if (m) {
    const dd = m[1].padStart(2, "0");
    const mm = m[2].padStart(2, "0");
    const yyyy = m[3];
    return `${yyyy}-${mm}-${dd}`;
  }

  const d = new Date(t);
  if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  return t;
}

function normalizeInt(v: any): number | null {
  if (v === null || v === undefined || v === "") return null;
  const n = parseInt(String(v), 10);
  return Number.isFinite(n) ? n : null;
}

function buildInList(values: string[]): string {
  return values
    .map((s) => `"${s.replace(/"/g, '\\"')}"`)
    .map((q) => encodeURIComponent(q))
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
