const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const MAX_PAGES = 5;

const PAGE_PRIORITY = [
  "contact",
  "contact-us",
  "about",
  "about-us",
  "catering",
  "order",
  "menu",
  "locations",
  "location",
  "get-in-touch"
];

function extractEmailsFromText(text) {
  if (!text) return [];

  const emails = [];
  const textMatches =
    text.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi) || [];

  emails.push(...textMatches);

  const mailtoRegex = /mailto:([^"'?\s>]+)/gi;
  let match;

  while ((match = mailtoRegex.exec(text)) !== null) {
    emails.push(match[1]);
  }

  return [...new Set(emails.map(email => email.toLowerCase().trim()))];
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
    "your@email.com",
    "example@gmail.com",
    "youremail@eataly.com"
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
    ".svg",
    "sentry.",
    "wixpress.",
    "godaddy.",
    "wordpress.",
    "squarespace.",
    "shopify.",
    "wix.",
    "ezcater.",
    "grubhub.",
    "doordash.",
    "ubereats.",
    "postmates.",
    "yelp."
  ];

  const filtered = emails.filter(email => {
    const lower = email.toLowerCase().trim();

    if (blockedExact.has(lower)) return false;
    if (blockedParts.some(part => lower.includes(part))) return false;
    if (!lower.includes("@")) return false;

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
    if (lower.startsWith("hello@")) score += 5;
    if (lower.startsWith("info@")) score += 4;
    if (lower.startsWith("contact@")) score += 4;
    if (lower.startsWith("events@")) score += 4;
    if (lower.startsWith("catering@")) score += 4;
    if (lower.startsWith("orders@")) score += 3;
    if (lower.startsWith("admin@")) score += 2;
    if (lower.startsWith("office@")) score += 2;
    if (lower.includes("@gmail.com")) score += 1;
    if (lower.includes("@outlook.com")) score += 1;
    if (lower.includes("@hotmail.com")) score += 1;
    if (lower.includes("@yahoo.com")) score += 1;

    return { email, score };
  });

  scored.sort((a, b) => b.score - a.score);
  return scored[0].email;
}

function getBaseUrl(url) {
  try {
    const parsed = new URL(url);
    return `${parsed.protocol}//${parsed.hostname}`;
  } catch {
    return null;
  }
}

function extractInternalLinks(html, baseUrl) {
  if (!html || !baseUrl) return [];

  const links = [];
  const regex = /href=["']([^"'#]+)["']/gi;
  let match;

  while ((match = regex.exec(html)) !== null) {
    let href = match[1].trim();

    if (!href) continue;
    if (href.startsWith("mailto:")) continue;
    if (href.startsWith("tel:")) continue;
    if (href.startsWith("javascript:")) continue;

    try {
      const absolute = new URL(href, baseUrl).toString();

      if (absolute.startsWith(baseUrl)) {
        links.push(absolute.replace(/\/$/, ""));
      }
    } catch {
      continue;
    }
  }

  return [...new Set(links)];
}

function sortLinksByPriority(links) {
  return links.sort((a, b) => {
    const aLower = a.toLowerCase();
    const bLower = b.toLowerCase();

    const aIndex = PAGE_PRIORITY.findIndex(term => aLower.includes(term));
    const bIndex = PAGE_PRIORITY.findIndex(term => bLower.includes(term));

    const aScore = aIndex === -1 ? 999 : aIndex;
    const bScore = bIndex === -1 ? 999 : bIndex;

    return aScore - bScore;
  });
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

  const baseUrl = getBaseUrl(website);
  if (!baseUrl) return null;

  const homepage = await fetchHtml(website);
  if (!homepage) return null;

  // 1. Check homepage first
  const homepageEmails = extractEmailsFromText(homepage);
  const bestHomepageEmail = pickBestEmail(homepageEmails, websiteDomain);
  if (bestHomepageEmail) return bestHomepageEmail;

  // 2. Build internal link list from homepage
  let links = extractInternalLinks(homepage, baseUrl);

  // 3. Add likely pages manually in case they are not linked clearly
  const likelyPages = PAGE_PRIORITY.map(path => `${baseUrl}/${path}`);
  links.push(...likelyPages);

  // 4. Deduplicate + prioritize + limit
  links = [...new Set(links.map(link => link.replace(/\/$/, "")))];
  links = sortLinksByPriority(links).slice(0, MAX_PAGES);

  // 5. Crawl a few high-value pages
  const allEmails = [];

  for (const link of links) {
    const html = await fetchHtml(link);
    if (!html) continue;

    const found = extractEmailsFromText(html);
    allEmails.push(...found);

    const best = pickBestEmail([...new Set(allEmails)], websiteDomain);
    if (best) return best;
  }

  return null;
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