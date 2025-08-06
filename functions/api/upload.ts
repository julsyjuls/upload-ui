import { parse } from 'https://cdn.skypack.dev/papaparse';

export async function onRequestPost(context) {
  try {
    const formData = await context.request.formData();
    const file = formData.get('file');

    if (!file) {
      return new Response(JSON.stringify({ error: 'No file uploaded' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    const text = await file.text();
    const parsed = parse(text, {
      header: true,
      skipEmptyLines: true,
    });

    const rows = parsed.data;

    const SUPABASE_URL = context.env.SUPABASE_URL;
    const SUPABASE_KEY = context.env.SUPABASE_SERVICE_ROLE_KEY;

    const response = await fetch(`${SUPABASE_URL}/rest/v1/inventory_upload_buffer`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'apikey': SUPABASE_KEY,
        'Authorization': `Bearer ${SUPABASE_KEY}`,
        'Prefer': 'resolution=merge-duplicates',
      },
      body: JSON.stringify(rows),
    });

    if (!response.ok) {
      const errorText = await response.text();
      return new Response(JSON.stringify({ error: 'Supabase error', detail: errorText }), {
        status: 500,
        headers: { 'Content-Type': 'application/json' },
      });
    }

    return new Response(JSON.stringify({ message: 'Upload successful' }), {
      status: 200,
      headers: { 'Content-Type': 'application/json' },
    });
  } catch (error) {
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { 'Content-Type': 'application/json' },
    });
  }
}
