export async function onRequestPost(context) {
  try {
    const body = await context.request.json();
    const rows = body.rows;

    const SUPABASE_URL = context.env.SUPABASE_URL;
    const SUPABASE_KEY = context.env.SUPABASE_SERVICE_ROLE_KEY;

    const response = await fetch(`${SUPABASE_URL}/rest/v1/inventory_upload_buffer`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "apikey": SUPABASE_KEY,
        "Authorization": `Bearer ${SUPABASE_KEY}`,
        "Prefer": "resolution=merge-duplicates",
      },
      body: JSON.stringify(rows),
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error("🧨 Supabase error response:", errorText); // 👈 add this line
      return new Response(JSON.stringify({ error: "Supabase error", detail: errorText }), {
        status: 500,
        headers: { "Content-Type": "application/json" },
      });
    }

    return new Response(JSON.stringify({ message: "Upload successful" }), {
      status: 200,
      headers: { "Content-Type": "application/json" },
    });
  } catch (error) {
    console.error("❌ Worker error:", error.message); // 👈 add this line too
    return new Response(JSON.stringify({ error: error.message }), {
      status: 500,
      headers: { "Content-Type": "application/json" },
    });
  }
}
