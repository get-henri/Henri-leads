const NOTION_API_KEY = process.env.NOTION_API_KEY;
const DATABASE_ID = process.env.NOTION_DATABASE_ID;

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const DAILY_TARGET = 150;

function hasUsableEmail(email) {
  if (!email) return false;

  const lower = email.toLowerCase().trim();

  const blocked = [
    "user@domain.com",
    "email@example.com",
    "test@test.com",
    "info@example.com",
    "admin@example.com",
    "contact@example.com",
    "name@domain.com",
    "your@email.com",
    "missing@placeholder.com",
    "example@gmail.com",
    "youremail@eataly.com"
  ];

  if (blocked.includes(lower)) return false;
  if (!lower.includes("@")) return false;
  if (lower.includes("example.com")) return false;
  if (lower.includes("domain.com")) return false;
  if (lower.includes("test.com")) return false;
  if (lower.includes("noreply")) return false;
  if (lower.includes("no-reply")) return false;

  if (lower.startsWith("example@")) return false;
  if (lower.startsWith("test@")) return false;
  if (lower.startsWith("your")) return false;

  return true;
}

async function createScrapeRun() {
  const { data, error } = await supabase
    .from("scrape_runs")
    .insert([{ status: "running", notes: "queue to notion run" }])
    .select()
    .single();

  if (error) {
    console.error("Create scrape run error:", error);
    return null;
  }

  return data;
}

async function updateScrapeRun(runId, fields) {
  const { error } = await supabase
    .from("scrape_runs")
    .update(fields)
    .eq("id", runId);

  if (error) {
    console.error("Update scrape run error:", error);
  }
}

async function countBuffer() {
  const { count, error } = await supabase
    .from("lead_buffer")
    .select("*", { count: "exact", head: true })
    .eq("status", "buffer");

  if (error) {
    console.error("Count buffer error:", error);
    return 0;
  }

  return count || 0;
}

async function countReady() {
  const { count, error } = await supabase
    .from("lead_buffer")
    .select("*", { count: "exact", head: true })
    .eq("status", "ready");

  if (error) {
    console.error("Count ready error:", error);
    return 0;
  }

  return count || 0;
}

async function countQueued() {
  const { count, error } = await supabase
    .from("lead_buffer")
    .select("*", { count: "exact", head: true })
    .eq("status", "queued");

  if (error) {
    console.error("Count queued error:", error);
    return 0;
  }

  return count || 0;
}

async function getBatchLeads(limit = DAILY_TARGET) {
  const { data, error } = await supabase
    .from("lead_buffer")
    .select("*")
    .eq("status", "ready")
    .not("email", "is", null)
    .order("quality_score", { ascending: false })
    .limit(limit);

  if (error) {
    console.error("Get batch leads error:", error);
    return [];
  }

  return (data || []).filter(lead => hasUsableEmail(lead.email));
}

async function sendToNotion(lead) {
  const response = await fetch("https://api.notion.com/v1/pages", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${NOTION_API_KEY}`,
      "Content-Type": "application/json",
      "Notion-Version": "2022-06-28"
    },
    body: JSON.stringify({
      parent: { database_id: DATABASE_ID },
      properties: {
        "Business Name": {
          title: [{ text: { content: lead.business_name } }]
        },
        "Email": { email: lead.email },
        "Lead Type": { select: { name: lead.lead_type } },
        "City": { rich_text: [{ text: { content: lead.city || "" } }] },
        "State": { rich_text: [{ text: { content: lead.state || "" } }] },
        "Rating": { number: lead.rating },
        "Review Count": { number: lead.review_count },
        "Website": { url: lead.website || null },
        "Google Maps URL": { url: lead.google_maps_url || null },
        "Phone": { phone_number: lead.phone || null },
        "Personalization Note": {
          rich_text: [{ text: { content: lead.personalization_note || "" } }]
        },
        "Email Type": { select: { name: lead.email_type || "general" } },
        "Source": { select: { name: lead.source || "website" } },
        "Status": { select: { name: "new" } },
        "Date Added": { date: { start: new Date().toISOString() } },
        "Dedupe Key": {
          rich_text: [{ text: { content: lead.dedupe_key } }]
        },
        "Quality Score": { number: lead.quality_score || 0 }
      }
    })
  });

  const data = await response.json();

  if (!response.ok) {
    console.error(`Notion error for ${lead.business_name}:`, data);
    return null;
  }

  return data;
}

async function markQueued(lead, notionPageId) {
  const { error } = await supabase
    .from("lead_buffer")
    .update({
      status: "queued",
      queued_at: new Date().toISOString(),
      notion_page_id: notionPageId
    })
    .eq("id", lead.id);

  if (error) {
    console.error(`Mark queued error for ${lead.business_name}:`, error);
    return false;
  }

  return true;
}

async function processLead(lead) {
  console.log(`Queueing to Notion: ${lead.business_name}`);

  const notionResult = await sendToNotion(lead);
  if (!notionResult) {
    return { success: false };
  }

  const queued = await markQueued(lead, notionResult.id);
  if (!queued) {
    return { success: false };
  }

  console.log(`Queued: ${lead.business_name}`);
  return { success: true };
}

async function main() {
  const run = await createScrapeRun();
  if (!run) return;

  console.log("Started queue run:", run.id);

  const bufferStart = await countBuffer();
  const readyStart = await countReady();
  const queuedStart = await countQueued();

  console.log("Buffer start:", bufferStart);
  console.log("Ready start:", readyStart);
  console.log("Queued start:", queuedStart);

  const leads = await getBatchLeads(DAILY_TARGET);
  console.log(`Found ${leads.length} ready leads with usable emails to queue`);

  let queuedToday = 0;

  for (const lead of leads) {
    const result = await processLead(lead);
    if (result.success) queuedToday++;
  }

  const bufferEnd = await countBuffer();
  const readyEnd = await countReady();
  const queuedEnd = await countQueued();

  await updateScrapeRun(run.id, {
    finished_at: new Date().toISOString(),
    raw_found: 0,
    qualified_found: leads.length,
    duplicates_skipped: 0,
    rejected_no_email: 0,
    pushed_today: queuedToday,
    buffer_start: bufferStart,
    buffer_end: bufferEnd,
    status: "completed",
    notes: "queued ready leads to notion"
  });

  console.log("Finished queue job");
  console.log({
    bufferStart,
    readyStart,
    queuedStart,
    qualifiedFound: leads.length,
    queuedToday,
    bufferEnd,
    readyEnd,
    queuedEnd
  });
}

main();