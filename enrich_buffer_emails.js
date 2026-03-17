const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

function extractEmailsFromText(text) {
  const matches = text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || [];
  return [...new Set(matches.map(email => email.toLowerCase()))];
}

function pickBestEmail(emails, websiteDomain) {
  if (!emails.length) return null;

  const blockedExact = new Set([
    "user@domain.com",
    "email@example.com",
    "test@test.com",
    "info@example.com",
    "admin@example.com",
    "contact@example.com",
    "name@domain.com",
    "your@email.com"
  ]);

  const blockedParts = [
    "example.com",
    "domain.com",
    "test.com",
    "noreply",
    "no-reply",
    "@2x.png",
    ".jpg",
    ".jpeg",
    ".png",
    ".webp",
    ".svg"
  ];

  const filtered = emails.filter(email => {
    const lower = email.toLowerCase().trim();

    if (blockedExact.has(lower)) return false;
    if (blockedParts.some(part => lower.includes(part))) return false;

    if (
      websiteDomain &&
      !lower.endsWith(`@${websiteDomain}`) &&
      !lower.includes("@gmail.com") &&
      !lower.includes("@outlook.com") &&
      !lower.includes("@hotmail.com") &&
      !lower.includes("@yahoo.com")
    ) {
      return false;
    }

    return true;
  });

  if (!filtered.length) return null;

  const scored = filtered.map(email => {
    let score = 0;
    const lower = email.toLowerCase();

    if (websiteDomain && lower.endsWith(`@${websiteDomain}`)) score += 5;
    if (lower.startsWith("hello@")) score += 4;
    if (lower.startsWith("info@")) score += 4;
    if (lower.startsWith("contact@")) score += 4;
    if (lower.startsWith("events@")) score += 4;
    if (lower.startsWith("catering@")) score += 4;
    if (lower.startsWith("orders@")) score += 3;
    if (lower.startsWith("admin@")) score += 2;
    if (lower.startsWith("office@")) score += 2;

    return { email, score };
  });

  scored.sort((a, b) => b.score - a.score);
  return scored[0].email;
}

async function fetchHtml(url) {
  try {
    const response = await axios.get(url, {
      timeout: 15000,
      maxRedirects: 5,
      headers: {
        "User-Agent": "Mozilla/5.0"
      }
    });

    return typeof response.data === "string" ? response.data : null;
  } catch {
    return null;
  }
}

async function findEmailForWebsite(website, websiteDomain) {
  if (!website) return null;

  const normalizedWebsite = website.endsWith("/")
    ? website.slice(0, -1)
    : website;

  const urlsToTry = [
    normalizedWebsite,
    `${normalizedWebsite}/contact`,
    `${normalizedWebsite}/about`
  ];

  const allEmails = [];

  for (const url of urlsToTry) {
    const html = await fetchHtml(url);
    if (!html) continue;

    const emails = extractEmailsFromText(html);
    allEmails.push(...emails);
  }

  const uniqueEmails = [...new Set(allEmails)];
  return pickBestEmail(uniqueEmails, websiteDomain);
}

async function main() {
  const { data: leads, error } = await supabase
    .from("lead_buffer")
    .select("*")
    .eq("status", "buffer");

  if (error) {
    console.error("Fetch lead_buffer error:", error);
    return;
  }

  console.log(`Found ${leads.length} leads in buffer`);

  let updated = 0;
  let missing = 0;

  for (const lead of leads) {
    console.log(`Checking ${lead.business_name}...`);

    const email = await findEmailForWebsite(lead.website, lead.website_domain);

    if (!email) {
      console.log(`No email found for ${lead.business_name}`);

      const { error: clearError } = await supabase
        .from("lead_buffer")
        .update({
          email: null,
          email_type: null
        })
        .eq("id", lead.id);

      if (clearError) {
        console.error(`Clear email error for ${lead.business_name}:`, clearError);
      }

      missing++;
      continue;
    }

    const { error: updateError } = await supabase
      .from("lead_buffer")
      .update({
        email,
        email_type: "general"
      })
      .eq("id", lead.id);

    if (updateError) {
      console.error(`Update error for ${lead.business_name}:`, updateError);
      continue;
    }

    console.log(`Updated ${lead.business_name} -> ${email}`);
    updated++;
  }

  console.log({ updated, missing });
}

main();