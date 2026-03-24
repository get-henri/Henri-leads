try {
  require("dotenv").config();
} catch {}

const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY =
  process.env.SUPABASE_SERVICE_KEY || process.env.SUPABASE_KEY;

if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

const BATCH_SIZE = 100;
const MAX_ATTEMPTS = 2;
const REQUEST_TIMEOUT_MS = 5000;
const MAX_INTERNAL_PAGES = 3;

const COMMON_PATHS_FIRST_PASS = [
  "",
  "/contact",
  "/about"
];

const COMMON_PATHS_SECOND_PASS = [
  "",
  "/contact",
  "/about",
  "/contact-us",
  "/about-us",
  "/catering",
  "/events",
  "/faq"
];

const BLOCKED_EMAIL_PARTS = [
  "example",
  "test",
  "noreply",
  "no-reply",
  ".png",
  ".jpg",
  ".jpeg",
  ".webp",
  ".svg",
  "wix",
  "squarespace",
  "shopify",
  "godaddy",
  "wordpress",
  "yelp",
  "grubhub",
  "doordash",
  "ubereats",
  "ezcater",
  "placeholder"
];

function normalizeUrl(url) {
  if (!url) return null;

  let trimmed = url.trim();
  if (!trimmed) return null;

  if (!/^https?:\/\//i.test(trimmed)) {
    trimmed = `https://${trimmed}`;
  }

  try {
    const parsed = new URL(trimmed);
    return parsed.toString().replace(/\/$/, "");
  } catch {
    return null;
  }
}

function getDomainFromUrl(url) {
  try {
    const parsed = new URL(url);
    return parsed.hostname.replace(/^www\./, "").toLowerCase();
  } catch {
    return null;
  }
}

function buildWebsiteVariants(website, websiteDomain) {
  const variants = new Set();

  const normalizedWebsite = normalizeUrl(website);
  if (normalizedWebsite) variants.add(normalizedWebsite);

  if (websiteDomain) {
    const cleanDomain = websiteDomain.replace(/^www\./, "").toLowerCase();
    variants.add(`https://${cleanDomain}`);
    variants.add(`https://www.${cleanDomain}`);
    variants.add(`http://${cleanDomain}`);
  }

  return [...variants];
}

async function fetchHtml(url) {
  try {
    const response = await axios.get(url, {
      timeout: REQUEST_TIMEOUT_MS,
      maxRedirects: 3,
      headers: {
        "User-Agent":
          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122 Safari/537.36"
      }
    });

    return typeof response.data === "string" ? response.data : null;
  } catch {
    return null;
  }
}

function extractEmails(text) {
  if (!text) return [];

  const regex = /[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/gi;
  const matches = text.match(regex) || [];

  const mailtoRegex = /mailto:([^"'?\s>]+)/gi;
  let mailtoMatch;

  while ((mailtoMatch = mailtoRegex.exec(text)) !== null) {
    matches.push(mailtoMatch[1]);
  }

  return [...new Set(matches.map(e => e.toLowerCase().trim()))];
}

function filterEmails(emails) {
  return emails.filter(email => {
    if (!email || !email.includes("@")) return false;

    const lower = email.toLowerCase().trim();
    const [local, domain] = lower.split("@");

    if (!local || !domain) return false;
    if (lower.length > 80) return false;
    if (local.length < 2) return false;
    if (domain.length < 4) return false;

    if (BLOCKED_EMAIL_PARTS.some(part => lower.includes(part))) return false;

    if (
      local === "example" ||
      local === "test" ||
      local === "user" ||
      local === "name"
    ) {
      return false;
    }

    if (
      lower.endsWith(".png") ||
      lower.endsWith(".jpg") ||
      lower.endsWith(".jpeg") ||
      lower.endsWith(".webp") ||
      lower.endsWith(".svg")
    ) {
      return false;
    }

    if (local.includes("noreply") || local.includes("no-reply")) return false;

    return true;
  });
}

function pickBestEmail(emails, websiteDomain) {
  if (!emails.length) return null;

  const scored = emails.map(email => {
    const lower = email.toLowerCase().trim();
    const [local, domain] = lower.split("@");
    let score = 0;

    if (websiteDomain && domain === websiteDomain) score += 20;

    if (local === "hello") score += 12;
    if (local === "info") score += 11;
    if (local === "contact") score += 11;
    if (local === "office") score += 10;
    if (local === "sales") score += 9;
    if (local === "events") score += 9;
    if (local === "catering") score += 10;
    if (local === "orders") score += 8;
    if (local === "admin") score += 3;

    if (/^[a-z]+$/.test(local)) score += 5;
    if (/^[a-z]+\.[a-z]+$/.test(local)) score += 6;
    if (/^[a-z]+[0-9]*$/.test(local)) score += 4;

    if (
      domain === "gmail.com" ||
      domain === "outlook.com" ||
      domain === "hotmail.com" ||
      domain === "yahoo.com"
    ) {
      score += 2;
    }

    return { email, score };
  });

  scored.sort((a, b) => b.score - a.score);
  return scored[0].email;
}

function extractLinks(html, baseUrl) {
  if (!html) return [];

  const links = [];
  const regex = /href=["']([^"'#]+)["']/gi;
  let match;

  while ((match = regex.exec(html)) !== null) {
    const raw = match[1].trim();

    if (!raw) continue;
    if (raw.startsWith("mailto:")) continue;
    if (raw.startsWith("tel:")) continue;
    if (raw.startsWith("javascript:")) continue;

    try {
      const absolute = new URL(raw, baseUrl).toString().replace(/\/$/, "");
      links.push(absolute);
    } catch {}
  }

  return [...new Set(links)];
}

function extractInstagramUrl(html, baseUrl) {
  const links = extractLinks(html, baseUrl);
  return links.find(link => link.includes("instagram.com")) || null;
}

function detectContactForm(html) {
  if (!html) return false;

  const lower = html.toLowerCase();

  return (
    lower.includes("<form") &&
    (
      lower.includes("contact") ||
      lower.includes("message") ||
      lower.includes("type=\"email\"") ||
      lower.includes("type='email'")
    )
  );
}

function sortInternalLinks(links, baseDomain) {
  const internal = links.filter(link => {
    const domain = getDomainFromUrl(link);
    return domain && baseDomain && domain === baseDomain;
  });

  const priorityTerms = ["contact", "about", "catering", "events", "faq"];

  internal.sort((a, b) => {
    const aLower = a.toLowerCase();
    const bLower = b.toLowerCase();

    const aIndex = priorityTerms.findIndex(term => aLower.includes(term));
    const bIndex = priorityTerms.findIndex(term => bLower.includes(term));

    const aScore = aIndex === -1 ? 999 : aIndex;
    const bScore = bIndex === -1 ? 999 : bIndex;

    return aScore - bScore;
  });

  return [...new Set(internal)].slice(0, MAX_INTERNAL_PAGES);
}

async function crawlWebsiteForEmail(website, websiteDomain, attemptNumber) {
  const variants = buildWebsiteVariants(website, websiteDomain);

  let foundInstagramUrl = null;
  let hasContactForm = false;
  let allEmails = [];

  const commonPaths =
    attemptNumber >= 2 ? COMMON_PATHS_SECOND_PASS : COMMON_PATHS_FIRST_PASS;

  for (const baseUrl of variants) {
    const baseDomain = getDomainFromUrl(baseUrl);
    if (!baseDomain) continue;

    for (const path of commonPaths) {
      const pageUrl = `${baseUrl}${path}`.replace(/([^:]\/)\/+/g, "$1");
      const html = await fetchHtml(pageUrl);
      if (!html) continue;

      const pageEmails = filterEmails(extractEmails(html));
      allEmails.push(...pageEmails);

      if (!foundInstagramUrl) {
        foundInstagramUrl = extractInstagramUrl(html, baseUrl);
      }

      if (!hasContactForm && detectContactForm(html)) {
        hasContactForm = true;
      }

      const best = pickBestEmail([...new Set(allEmails)], baseDomain);
      if (best) {
        return {
          email: best,
          instagramUrl: foundInstagramUrl,
          hasContactForm
        };
      }
    }

    const homepage = await fetchHtml(baseUrl);
    if (!homepage) continue;

    if (!foundInstagramUrl) {
      foundInstagramUrl = extractInstagramUrl(homepage, baseUrl);
    }

    if (!hasContactForm && detectContactForm(homepage)) {
      hasContactForm = true;
    }

    const internalLinks = sortInternalLinks(
      extractLinks(homepage, baseUrl),
      baseDomain
    );

    for (const link of internalLinks) {
      const html = await fetchHtml(link);
      if (!html) continue;

      const pageEmails = filterEmails(extractEmails(html));
      allEmails.push(...pageEmails);

      if (!foundInstagramUrl) {
        foundInstagramUrl = extractInstagramUrl(html, baseUrl);
      }

      if (!hasContactForm && detectContactForm(html)) {
        hasContactForm = true;
      }

      const best = pickBestEmail([...new Set(allEmails)], baseDomain);
      if (best) {
        return {
          email: best,
          instagramUrl: foundInstagramUrl,
          hasContactForm
        };
      }
    }
  }

  return {
    email: null,
    instagramUrl: foundInstagramUrl,
    hasContactForm
  };
}

async function scrapeInstagramForEmail(instagramUrl) {
  if (!instagramUrl) return null;

  const html = await fetchHtml(instagramUrl);
  if (!html) return null;

  const directEmails = filterEmails(extractEmails(html));
  if (directEmails.length) return directEmails[0];

  const links = extractLinks(html, instagramUrl);

  const fallbackBioLink = links.find(link =>
    link.includes("linktr.ee") ||
    link.includes("beacons.ai") ||
    link.includes("bio.site") ||
    link.includes("lnk.bio")
  );

  if (!fallbackBioLink) return null;

  const linkedPage = await fetchHtml(fallbackBioLink);
  if (!linkedPage) return null;

  const fallbackEmails = filterEmails(extractEmails(linkedPage));
  return fallbackEmails[0] || null;
}

function nextEnrichmentState(foundEmail, attemptsAfterRun) {
  if (foundEmail) return "found";
  if (attemptsAfterRun >= MAX_ATTEMPTS) return "failed";
  return "not_found";
}

function nextLeadStatus(foundEmail, attemptsAfterRun) {
  if (foundEmail) return "ready";
  if (attemptsAfterRun >= MAX_ATTEMPTS) return "failed";
  return "buffer";
}

async function run() {
  const { data: rawLeads, error } = await supabase
    .from("lead_buffer")
    .select("*")
    .eq("status", "buffer")
    .is("email", null)
    .lt("enrich_attempts", MAX_ATTEMPTS)
    .order("last_enriched_at", { ascending: true, nullsFirst: true })
    .limit(BATCH_SIZE);

  if (error) {
    console.error("Fetch lead_buffer error:", error);
    return;
  }

  const processedIds = new Set();
  const leads = [];

  for (const lead of rawLeads || []) {
    if (processedIds.has(lead.id)) continue;
    processedIds.add(lead.id);
    leads.push(lead);
  }

  console.log(`Found ${leads.length} leads to enrich`);

  let updated = 0;
  let found = 0;
  let notFound = 0;
  let failed = 0;

  for (const lead of leads) {
    const attemptsAfterRun = (lead.enrich_attempts || 0) + 1;

    console.log(`\nChecking ${lead.business_name}...`);

    let email = null;
    let instagramUrl = lead.instagram_url || null;
    let hasContactForm = lead.has_contact_form || false;

    if (lead.website || lead.website_domain) {
      const websiteResult = await crawlWebsiteForEmail(
        lead.website,
        lead.website_domain,
        attemptsAfterRun
      );

      if (websiteResult.email) {
        email = websiteResult.email;
      }

      if (!instagramUrl && websiteResult.instagramUrl) {
        instagramUrl = websiteResult.instagramUrl;
      }

      if (websiteResult.hasContactForm) {
        hasContactForm = true;
      }
    }

    if (!email && instagramUrl && attemptsAfterRun >= 2) {
      console.log("Trying Instagram...");
      const instagramEmail = await scrapeInstagramForEmail(instagramUrl);
      if (instagramEmail) {
        email = instagramEmail;
      }
    }

    const enrichmentStatus = nextEnrichmentState(!!email, attemptsAfterRun);
    const nextStatus = nextLeadStatus(!!email, attemptsAfterRun);

    const updatePayload = {
      enrich_attempts: attemptsAfterRun,
      last_enriched_at: new Date().toISOString(),
      enrichment_status: enrichmentStatus,
      instagram_url: instagramUrl,
      has_contact_form: hasContactForm,
      status: nextStatus
    };

    if (email) {
      updatePayload.email = email;
      updatePayload.email_type = "real";
    }

    const { error: updateError } = await supabase
      .from("lead_buffer")
      .update(updatePayload)
      .eq("id", lead.id);

    if (updateError) {
      console.error(`Update error for ${lead.business_name}:`, updateError);
      continue;
    }

    if (email) {
      console.log(`Found email → ${email} (status: ready)`);
      found++;
    } else if (nextStatus === "failed") {
      console.log("No email found, marking as failed");
      failed++;
    } else {
      console.log("No email found, keeping in buffer for another attempt");
      notFound++;
    }

    updated++;
  }

  console.log("\nRESULT:");
  console.log({
    processed: leads.length,
    updated,
    found,
    notFound,
    failed
  });
}

run();