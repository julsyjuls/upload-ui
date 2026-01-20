// functions/upload.ts
// Upload Inventory CSV — chunked preloads, global batch_no upserts, inventory inserts
// Updated: supports same sku_code across different brands by resolving sku_id via (brand_id, sku_code)

export const onRequestOptions = async () =>
  new Response(null, {
    status: 204,
    headers: CORS_HEADERS,
  });

// Simple health check to verify env bindings are reaching the function
export const onRequestGet = async (context: any) => {
  const url = new URL(context.request.url);
  if (url.pathname.endsWith("/health")) {
    const hasKey = !!context.env.SUPABASE_SERVICE_ROLE_KEY;
    const hasUrl = !!(context.env.SUPABASE_URL || SUPABASE_URL_FALLBACK);
    return new Response(
      JSON.stringify(
        {
          ok: true,
          has_key: hasKey,
          has_url: hasUrl,
          using_url: context.env.SUPABASE_URL || SUPABASE_URL_FALLBACK,
        },
        null,
        2
      ),
      { headers: { "Content-Type": "application/json", ...CORS_HEADERS } }
    );
  }
  return new Response("OK", { headers: CORS_HEADERS });
};

export const onRequestPost = async (context: any) => {
  // ---------- tunables ----------
  const INVENTORY_CHUNK_SIZE = 300;
  const MAX_SKIPPED_RETURN = 200;

  // Preload chunk sizes
  const BRAND_IN_CHUNK = 60;
  const SKU_PAIR_CHUNK = 40;
  const BATCH_IN_CHUNK = 60;
  // ------------------------------

  const SB_URL = (context.env.SUPABASE_URL as string) || SUPABASE_URL_FALLBACK;
  const SB_KEY = context.env.SUPABASE_SERVICE_ROLE_KEY as string;

  let insertedCount = 0;
  const skipped: any[] = [];
  let phase = "init";

  try {
    if (!SB_KEY) {
      return json(
        { error: "Missing SUPABASE_SERVICE_ROLE_KEY", phase },
        500,
        { insertedCount, skippedArr: skipped }
      );
    }

    // Parse body
    let body: any = null;
    try {
      phase = "parse_body";
      body = await context.request.json();
    } catch {
      return json(
        { error: "Invalid JSON body", phase },
        400,
        { insertedCount, skippedArr: skipped }
      );
    }

    const inputRows: RawRow[] = Array.isArray(body?.rows) ? body.rows : [];
    if (!inputRows.length) {
      return json(
        { error: "No rows provided (expected { rows: [...] })", phase },
        400,
        { insertedCount, skippedArr: skipped }
      );
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

      if (
        !row.sku_code ||
        !row.brand_name ||
        !row.batch_no ||
        !row.barcode ||
        !row.date_in
      ) {
        skipped.push({
          ...row,
          reason:
            "❌ Missing required field(s). Required: sku_code, brand_name, batch_no, barcode, date_in",
        });
        continue;
      }
      rows.push(row);
    }

    if (!rows.length) {
      return json(
        { note: "All rows skipped due to missing required fields", phase },
        200,
        { insertedCount, skippedArr: skipped }
      );
    }

    // ---------- Helpers ----------
    const baseHeaders = {
      apikey: SB_KEY,
      Authorization: `Bearer ${SB_KEY}`,
      "Content-Type": "application/json",
    };

    const fetchJson = async (url: string, init?: RequestInit) => {
      const res = await fetch(url, { headers: baseHeaders, ...(init || {}) });
      if (!res.ok) {
        const t = await safeText(res);
        throw new Error(t || `${res.status} ${res.statusText}`);
      }
      return res.json();
    };

    // ---------- 1) Preload BRANDS (case-insensitive exact match) ----------
    phase = "preload_brands";
    const brandNamesArr = [...new Set(rows.map((r) => r.brand_name))];

    const brands: Array<{ id: string; name: string }> = [];
    for (let i = 0; i < brandNamesArr.length; i += BRAND_IN_CHUNK) {
      const part = brandNamesArr.slice(i, i + BRAND_IN_CHUNK);

      // PostgREST: ilike is case-insensitive
      // Build: or=(name.ilike."Nell",name.ilike."Duramaxx",...)
      const orFilter = part
        .map((v) => `name.ilike.${JSON.stringify(v)}`)
        .join(",");

      const url = `${SB_URL}/rest/v1/brands?select=id,name&or=(${encodeURIComponent(
        orFilter
      )})`;

      const page = await fetchJson(url);
      if (Array.isArray(page)) brands.push(...page);
    }

    // Map normalized brand name -> brand row
    const brandMap = new Map<string, { id: string; name: string }>();
    for (const b of brands) {
      brandMap.set(normKey(b.name), b);
    }

    // Attach brand_id to rows (skip if brand not found)
    const rowsWithBrand: Array<NormalizedRow & { brand_id: string }> = [];
    for (const r of rows) {
      const b = brandMap.get(normKey(r.brand_name));
      if (!b) {
        skipped.push({
          ...r,
          reason: `❌ Brand not found: "${r.brand_name}"`,
        });
        continue;
      }
      rowsWithBrand.push({ ...r, brand_id: b.id });
    }

    if (!rowsWithBrand.length) {
      return json(
        { note: "All rows skipped due to missing brands", phase },
        200,
        { insertedCount, skippedArr: skipped }
      );
    }

    // ---------- 2) Preload SKUs by (brand_id, sku_code) ----------
    phase = "preload_skus";

    type SkuRow = { id: string; sku_code: string; brand_id: string };

    // Unique pairs: brand_id┃sku_code
    const skuPairs = [
      ...new Set(rowsWithBrand.map((r) => `${r.brand_id}┃${r.sku_code}`)),
    ];

    const skus: SkuRow[] = [];
    for (let i = 0; i < skuPairs.length; i += SKU_PAIR_CHUNK) {
      const part = skuPairs.slice(i, i + SKU_PAIR_CHUNK);

      // Build:
      // or=(and(brand_id.eq.<id>,sku_code.eq."A"),and(brand_id.eq.<id>,sku_code.eq."B"),...)
      const orFilter = part
        .map((k) => {
          const [brand_id, sku_code] = k.split("┃");
          return `and(brand_id.eq.${brand_id},sku_code.eq.${JSON.stringify(
            sku_code
          )})`;
        })
        .join(",");

      const url = `${SB_URL}/rest/v1/skus?select=id,sku_code,brand_id&or=(${encodeURIComponent(
        orFilter
      )})`;

      const page = await fetchJson(url);
      if (Array.isArray(page)) skus.push(...page);
    }

    // Map brand_id┃sku_code -> sku row
    const skuMap = new Map<string, SkuRow>();
    for (const s of skus) {
      skuMap.set(`${s.brand_id}┃${s.sku_code}`, s);
    }

    // Validate SKUs exist and attach sku_id
    const usableRows: Array<
      NormalizedRow & { brand_id: string; sku_id: string }
    > = [];

    for (const r of rowsWithBrand) {
      const s = skuMap.get(`${r.brand_id}┃${r.sku_code}`);
      if (!s) {
        skipped.push({
          ...r,
          reason: `❌ SKU not found for (brand_name="${r.brand_name}", sku_code="${r.sku_code}")`,
        });
      } else {
        usableRows.push({ ...r, sku_id: s.id });
      }
    }

    if (!usableRows.length) {
      return json(
        { note: "All rows skipped due to missing SKUs", phase },
        200,
        { insertedCount, skippedArr: skipped }
      );
    }

    // ---------- 3) Preload batches (GLOBAL by batch_no) ----------
    phase = "preload_batches";
    const batchNosArr = [...new Set(usableRows.map((r) => r.batch_no))];
    let batches: Array<{ id: string; sku_id: string; batch_no: string }> = [];

    for (let i = 0; i < batchNosArr.length; i += BATCH_IN_CHUNK) {
      const part = batchNosArr.slice(i, i + BATCH_IN_CHUNK);
      const inlist = buildInList(part);
      const url = `${SB_URL}/rest/v1/batches?select=id,sku_id,batch_no&batch_no=in.(${inlist})`;
      const page = await fetchJson(url);
      if (Array.isArray(page)) batches.push(...page);
    }

    // IMPORTANT: key map by batch_no (global), not by sku_id|batch_no
    const batchMap = new Map<
      string,
      { id: string; sku_id: string; batch_no: string }
    >();
    for (const b of batches) batchMap.set(b.batch_no, b);

    // ---------- 4) Upsert missing batches (GLOBAL by batch_no) ----------
    phase = "upsert_batches";

    // Earliest date_in per batch_no (global)
    const batchEarliestByNo = new Map<string, string>();
    for (const r of usableRows) {
      const ex = batchEarliestByNo.get(r.batch_no);
      if (!ex || r.date_in < ex) batchEarliestByNo.set(r.batch_no, r.date_in);
    }

    // Representative sku_id per batch_no (first seen)
    const batchNoToSkuId = new Map<string, string>();
    for (const r of usableRows) {
      if (!batchNoToSkuId.has(r.batch_no)) batchNoToSkuId.set(r.batch_no, r.sku_id);
    }

    const missingBatchPayload: Array<{
      sku_id: string;
      batch_no: string;
      date_in?: string;
    }> = [];

    for (const bn of batchNosArr) {
      if (!batchMap.has(bn)) {
        const sku_id = batchNoToSkuId.get(bn)!;
        const earliest = batchEarliestByNo.get(bn);
        missingBatchPayload.push({
          sku_id,
          batch_no: bn,
          ...(earliest ? { date_in: earliest } : {}),
        });
      }
    }

    if (missingBatchPayload.length) {
      // Match your DB's unique constraint (batches_batch_no_norm_uq)
      const upsert = await fetch(
        `${SB_URL}/rest/v1/batches?on_conflict=batch_no_norm`,
        {
          method: "POST",
          headers: {
            ...baseHeaders,
            Prefer: "resolution=merge-duplicates,return=minimal",
          },
          body: JSON.stringify(missingBatchPayload),
        }
      );

      if (!upsert.ok) {
        const errText = await safeText(upsert);
        const isConflict =
          upsert.status === 409 ||
          /23505/.test(errText) ||
          /duplicate key value/.test(errText) ||
          /unique constraint/i.test(errText);

        if (!isConflict) {
          return json(
            {
              error: `Failed to upsert batches: ${truncate(errText, 300)}`,
              phase,
            },
            500,
            { insertedCount, skippedArr: skipped }
          );
        }
        // else: duplicates mean batch already exists -> proceed
      }

      // Re-fetch batches by batch_no to refresh ids
      phase = "refetch_batches";
      batches = [];
      for (let i = 0; i < batchNosArr.length; i += BATCH_IN_CHUNK) {
        const part = batchNosArr.slice(i, i + BATCH_IN_CHUNK);
        const inlist = buildInList(part);
        const url = `${SB_URL}/rest/v1/batches?select=id,sku_id,batch_no&batch_no=in.(${inlist})`;
        const page = await fetchJson(url);
        if (Array.isArray(page)) batches.push(...page);
      }
      batchMap.clear();
      for (const b of batches) batchMap.set(b.batch_no, b);
    }

    // ---------- 5) Build inventory payload ----------
    phase = "build_inventory_payload";
    const inventoryPayload: any[] = [];

    for (const r of usableRows) {
      const b = batchMap.get(r.batch_no); // GLOBAL lookup by batch_no

      if (!b) {
        skipped.push({
          ...r,
          reason: `❌ Batch not resolved for (batch_no="${r.batch_no}")`,
        });
        continue;
      }

      inventoryPayload.push({
        sku_code: r.sku_code,
        brand_name: r.brand_name,
        batch_no: r.batch_no,
        sku_id: r.sku_id,
        batch_id: b.id, // global container batch id
        barcode: r.barcode,
        date_in: r.date_in, // YYYY-MM-DD
        warranty_months: r.warranty_months, // null OK
      });
    }

    if (!inventoryPayload.length) {
      return json(
        { note: "Nothing to insert after resolving SKUs/Batches.", phase },
        200,
        { insertedCount, skippedArr: skipped }
      );
    }

    // ---------- 6) Chunked bulk insert ----------
    phase = "insert_inventory_chunks";
    let duplicateCount = 0;

    for (let i = 0; i < inventoryPayload.length; i += INVENTORY_CHUNK_SIZE) {
      const chunk = inventoryPayload.slice(i, i + INVENTORY_CHUNK_SIZE);

      const res = await fetch(`${SB_URL}/rest/v1/inventory?on_conflict=barcode`, {
        method: "POST",
        headers: {
          ...baseHeaders,
          Prefer: "resolution=ignore-duplicates,return=representation",
        },
        body: JSON.stringify(chunk),
      });

      if (!res.ok) {
        const err = await safeText(res);
        for (const bad of chunk) {
          skipped.push({
            ...bad,
            reason: `❌ Supabase error (bulk insert): ${truncate(err, 300)}`,
          });
        }
        continue;
      }

      const data = await res.json();
      const inserted = Array.isArray(data) ? data.length : 0;
      insertedCount += inserted;

      // If Prefer return=representation didn't echo a row, we treat it as duplicate
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

    // Done
    phase = "done";
    return json(
      {
        inserted: insertedCount,
        duplicates_skipped: duplicateCount,
        skippedRows: skipped.slice(0, MAX_SKIPPED_RETURN),
        note:
          "Batched mode: preloaded Brands (chunked), preloaded SKUs by (brand_id, sku_code), upserted global batches by batch_no, inserted inventory (chunked).",
      },
      200,
      { insertedCount, skippedArr: skipped }
    );
  } catch (e: any) {
    return json(
      { error: `Unexpected error: ${String(e?.message || e)}`, phase },
      500,
      { insertedCount, skippedArr: skipped }
    );
  }
};

// ---------------- Types & helpers ----------------

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
  date_in: string; // YYYY-MM-DD
  warranty_months: number | null;
  __rowIndex: number;
};

const SUPABASE_URL_FALLBACK = "https://idtwjchmeldqwurigvkx.supabase.co";

const CORS_HEADERS: Record<string, string> = {
  "Access-Control-Allow-Origin": "*",
  "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
  "Access-Control-Allow-Headers": "Authorization, Content-Type",
  "Content-Type": "application/json",
};

function json(
  payload: any = {},
  status = 200,
  counters?: { insertedCount?: number; skippedArr?: any[] }
) {
  const inserted = counters?.insertedCount ?? 0;
  const skippedArr = counters?.skippedArr ?? [];
  const body = {
    // legacy/UI fields
    added: inserted,
    addedCount: inserted,
    skipped: skippedArr.length,
    // custom payload
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
    warranty_months:
      r.warranty_months ??
      r.warranty ??
      r.warranty_month ??
      r.warrantyMonths ??
      null,
  };
}

// Normalize brand keys for lookup
function normKey(s: string) {
  return (s ?? "").trim().toUpperCase();
}

// Accepts "01/20/2025" or "2025-01-20" and returns "YYYY-MM-DD"
function normalizeDate(v: any): string {
  if (!v) return "";
  const t = String(v).trim();

  // mm/dd/yyyy
  const m = /^(\d{1,2})\/(\d{1,2})\/(\d{4})$/.exec(t);
  if (m) {
    const mm = m[1].padStart(2, "0");
    const dd = m[2].padStart(2, "0");
    const yyyy = m[3];
    return `${yyyy}-${mm}-${dd}`;
  }

  const d = new Date(t);
  if (!isNaN(d.getTime())) return d.toISOString().slice(0, 10);
  return t; // let DB reject if truly invalid
}

function normalizeInt(v: any): number | null {
  if (v === null || v === undefined || v === "") return null;
  const n = parseInt(String(v), 10);
  return Number.isFinite(n) ? n : null;
}

// Build in() list for PostgREST: "A","B"
function buildInList(values: string[]): string {
  return values.map((s) => `"${String(s).replace(/"/g, '""')}"`).join(",");
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
