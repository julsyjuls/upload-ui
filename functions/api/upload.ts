// upload.ts — Batched CSV upload (5k–10k rows capable)

export const SUPABASE_URL = "https://idtwjchmeldqwurigvkx.supabase.co";

// ---------- tunables ----------
const INVENTORY_CHUNK_SIZE = 400;         // ~300–500 is a good range
const MAX_SKIPPED_RETURN = 200;           // cap how many skipped details to return
// --------------------------------

type RawRow = {
  sku_code?: string;
  brand_name?: string;
  batch_no?: string;
  barcode?: string;
  date_in?: string;           // expect YYYY-MM-DD or similar
  warranty_months?: number | string | null;
};

type NormalizedRow = {
  sku_code: string;
  brand_name: string;
  batch_no: string;
  barcode: string;
  date_in: string;            // normalized YYYY-MM-DD
  warranty_months: number | null;
  __rowIndex: number;         // original index for better errors
};

export async function onRequestPost(context: any) {
  const { SUPABASE_SERVICE_ROLE_KEY } = context.env;
  const SB_KEY = SUPABASE_SERVICE_ROLE_KEY as string;

  // read input
  const body = await context.request.json();
  const inputRows: RawRow[] = Array.isArray(body?.rows) ? body.rows : [];

  // quick guards
  if (!SB_KEY) {
    return json({ error: "Missing SUPABASE_SERVICE_ROLE_KEY" }, 500);
  }
  if (!Array.isArray(inputRows) || inputRows.length === 0) {
    return json({ error: "No rows provided" }, 400);
  }

  // normalize + validate required fields
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
      inserted: 0,
      skipped: skipped.length,
      skippedRows: skipped.slice(0, MAX_SKIPPED_RETURN),
      note: "All rows skipped due to missing required fields",
    });
  }

  // ---------- build sets for batched lookups ----------
  const skuKeys = new Set<string>(); // "sku_code|brand_name"
  const skuCodes = new Set<string>();
  const brandNames = new Set<string>();
  const batchKeys = new Set<string>(); // "sku_code|brand_name|batch_no"

  // Also capture earliest date_in per (sku_code|brand_name|batch_no) for batch metadata
  const batchEarliestDateIn = new Map<string, string>();

  for (const r of rows) {
    const sk = keySku(r.sku_code, r.brand_name);
    skuKeys.add(sk);
    skuCodes.add(r.sku_code);
    brandNames.add(r.brand_name);

    const bk = keyBatch(r.sku_code, r.brand_name, r.batch_no);
    batchKeys.add(bk);

    const existing = batchEarliestDateIn.get(bk);
    if (!existing || r.date_in < existing) batchEarliestDateIn.set(bk, r.date_in);
  }

  // ---------- helpers ----------
  const headers = {
    apikey: SB_KEY,
    Authorization: `Bearer ${SB_KEY}`,
    "Content-Type": "application/json",
  };

  const fetchJson = async (url: string, init?: RequestInit) => {
    const res = await fetch(url, { headers, ...(init || {}) });
    if (!res.ok) throw new Error(await res.text());
    return res.json();
  };

  // ---------- 1) Preload SKUs (one request) ----------
  // We can’t filter by the pair directly server-side, so:
  //   WHERE sku_code IN (...) AND brand_name IN (...)
  // then client-filter into an exact (sku_code|brand_name) map.
  const sku_code_in = encodeInList([...skuCodes]);
  const brand_name_in = encodeInList([...brandNames]);

  console.log(`Preloading SKUs: codes=${skuCodes.size}, brands=${brandNames.size}`);

  let skus: Array<{ id: string; sku_code: string; brand_name: string }>;
  try {
    skus = await fetchJson(
      `${SUPABASE_URL}/rest/v1/skus?select=id,sku_code,brand_name&sku_code=in.(${sku_code_in})&brand_name=in.(${brand_name_in})`
    );
  } catch (e: any) {
    return json({ error: `Failed to preload SKUs: ${e.message}` }, 500);
  }

  const skuMap = new Map<string, { id: string; sku_code: string; brand_name: string }>();
  for (const s of skus) {
    skuMap.set(keySku(s.sku_code, s.brand_name), s);
  }

  // Any rows with missing SKU? mark skipped now
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
      inserted: 0,
      skipped: skipped.length,
      skippedRows: skipped.slice(0, MAX_SKIPPED_RETURN),
      note: "All rows skipped due to missing SKUs",
    });
  }

  // ---------- 2) Preload existing batches (one request) ----------
  // After we know sku_ids, compute sets:
  const skuIdSet = new Set<string>();
  for (const r of usableRows) {
    const s = skuMap.get(keySku(r.sku_code, r.brand_name))!;
    skuIdSet.add(s.id);
  }

  const sku_id_in = encodeInList([...skuIdSet]);
  const batch_no_in = encodeInList([...new Set(usableRows.map(r => r.batch_no))]);

  console.log(
    `Preloading Batches: sku_ids=${skuIdSet.size}, batch_nos=${new Set(usableRows.map(r=>r.batch_no)).size}`
  );

  let batches: Array<{ id: string; sku_id: string; batch_no: string }>;
  try {
    batches = await fetchJson(
      `${SUPABASE_URL}/rest/v1/batches?select=id,sku_id,batch_no&sku_id=in.(${sku_id_in})&batch_no=in.(${batch_no_in})`
    );
  } catch (e: any) {
    return json({ error: `Failed to preload Batches: ${e.message}` }, 500);
  }

  const batchMap = new Map<string, { id: string; sku_id: string; batch_no: string }>();
  for (const b of batches) batchMap.set(`${b.sku_id}|${b.batch_no}`, b);

  // ---------- 3) Upsert missing batches (one request) ----------
  // Determine which (sku_id, batch_no) are missing.
  const missingBatchPayload: Array<{ sku_id: string; batch_no: string; date_in?: string }> = [];
  for (const bk of batchKeys) {
    const [sku_code, brand_name, batch_no] = splitBatchKey(bk);
    const sku = skuMap.get(keySku(sku_code, brand_name))!;
    const key = `${sku.id}|${batch_no}`;
    if (!batchMap.has(key)) {
      // use earliest date_in across rows of this batch; optional
      const earliest = batchEarliestDateIn.get(bk);
      missingBatchPayload.push({
        sku_id: sku.id,
        batch_no,
        ...(earliest ? { date_in: earliest } : {}),
      });
    }
  }

  if (missingBatchPayload.length > 0) {
    console.log(`Upserting ${missingBatchPayload.length} missing batches...`);
    try {
      await fetchJson(
        `${SUPABASE_URL}/rest/v1/batches?on_conflict=sku_id,batch_no`,
        {
          method: "POST",
          body: JSON.stringify(missingBatchPayload),
          // merge duplicates & don't return full rows
          headers: { ...headers, Prefer: "resolution=merge-duplicates,return=minimal" },
        } as any
      );
    } catch (e: any) {
      return json({ error: `Failed to upsert batches: ${e.message}` }, 500);
    }

    // Re-fetch batches once to complete the map
    try {
      batches = await fetchJson(
        `${SUPABASE_URL}/rest/v1/batches?select=id,sku_id,batch_no&sku_id=in.(${sku_id_in})&batch_no=in.(${batch_no_in})`
      );
      batchMap.clear();
      for (const b of batches) batchMap.set(`${b.sku_id}|${b.batch_no}`, b);
    } catch (e: any) {
      return json({ error: `Failed to re-fetch batches: ${e.message}` }, 500);
    }
  }

  // ---------- 4) Build inventory payload ----------
  // We’ll post with on_conflict=barcode so duplicates are ignored.
  const inventoryPayload = [];
  for (const r of usableRows) {
    const sku = skuMap.get(keySku(r.sku_code, r.brand_name))!;
    const batch = batchMap.get(`${sku.id}|${r.batch_no}`);
    if (!batch) {
      // Should be very rare after upsert+refetch; but guard anyway
      skipped.push({
        ...r,
        reason: `❌ Batch not resolved for (sku="${r.sku_code}|${r.brand_name}", batch_no="${r.batch_no}")`,
      });
      continue;
    }

    inventoryPayload.push({
      // keep original text fields if your DB trigger still references them
      sku_code: r.sku_code,
      brand_name: r.brand_name,
      batch_no: r.batch_no,

      // direct FKs (if your triggers prefer direct assignments, they’ll be already set)
      sku_id: sku.id,
      batch_id: batch.id,

      barcode: r.barcode,
      date_in: r.date_in,
      warranty_months: r.warranty_months,
    });
  }

  if (inventoryPayload.length === 0) {
    return json({
      inserted: 0,
      skipped: skipped.length,
      skippedRows: skipped.slice(0, MAX_SKIPPED_RETURN),
      note: "Nothing to insert after resolving SKUs/Batches.",
    });
  }

  // ---------- 5) Chunked bulk insert into inventory ----------
  let insertedCount = 0;
  let duplicateCount = 0;

  console.log(
    `Inserting inventory in chunks of ${INVENTORY_CHUNK_SIZE} (total rows: ${inventoryPayload.length})`
  );

  for (let i = 0; i < inventoryPayload.length; i += INVENTORY_CHUNK_SIZE) {
    const chunk = inventoryPayload.slice(i, i + INVENTORY_CHUNK_SIZE);

    // Use resolution=ignore-duplicates to skip known barcodes quietly
    const res = await fetch(`${SUPABASE_URL}/rest/v1/inventory?on_conflict=barcode`, {
      method: "POST",
      headers: {
        ...headers,
        Prefer: "resolution=ignore-duplicates,return=representation",
      },
      body: JSON.stringify(chunk),
    });

    if (!res.ok) {
      const err = await res.text();
      // If it’s a generic error, capture a sample and continue with next chunks
      console.error(`Chunk insert failed: ${err}`);
      // Optionally: push chunk rows to skipped with summary
      for (const bad of chunk) {
        skipped.push({ ...bad, reason: `❌ Supabase error during bulk insert: ${truncate(err, 300)}` });
      }
      continue;
    }

    // We asked return=representation; rows that were inserted are returned;
    // rows that conflicted (barcode dup) are omitted.
    const data = await res.json();
    insertedCount += Array.isArray(data) ? data.length : 0;
    duplicateCount += chunk.length - (Array.isArray(data) ? data.length : 0);
  }

  // ---------- 6) Respond ----------
  return json({
    inserted: insertedCount,
    duplicates_skipped: duplicateCount,
    skipped: skipped.length,
    skippedRows: skipped.slice(0, MAX_SKIPPED_RETURN), // cap details
    note:
      "Batched mode: preloaded SKUs, upserted batches, and inserted inventory in chunks (on_conflict=barcode).",
  });
}

// ---------------- helpers ----------------

function json(payload: any, status = 200) {
  return new Response(JSON.stringify(payload), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function keySku(code: string, brand: string) {
  return `${code}┃${brand}`; // use a rare delimiter to avoid accidental collisions
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
  // Accept 'YYYY-MM-DD' or ISO; return YYYY-MM-DD if possible
  const d = new Date(t);
  if (isNaN(d.getTime())) return t; // send as-is; let DB try if it’s valid
  return d.toISOString().slice(0, 10);
}

function normalizeInt(v: any): number | null {
  if (v === null || v === undefined || v === "") return null;
  const n = parseInt(String(v), 10);
  return Number.isFinite(n) ? n : null;
}

function encodeInList(values: string[]): string {
  // Properly escape commas and quotes for PostgREST in.() list
  // We’ll wrap each value in double quotes and escape any existing quotes
  return values
    .map((s) => `"${s.replace(/"/g, '\\"')}"`)
    .join(",");
}

function truncate(s: string, n: number) {
  return s.length > n ? s.slice(0, n) + "…" : s;
}
