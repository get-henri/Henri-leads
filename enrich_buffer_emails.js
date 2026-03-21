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
const MAX_INTERNAL_PAGES = 2;

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
  "/catering"
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
  "ezcater"
];

const COMMON_FIRST_NAMES = new Set([
  "joe", "mike", "john", "dave", "steve", "chris", "matt", "mark",
  "jessica", "jennifer", "ashley", "sarah", "emily", "anna", "alex",
  "dan", "daniel", "kevin", "brian", "ryan", "sam", "tom", "tim"
]);

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

function looksLikePersonalFreeEmail(email) {
  const lower = email.toLowerCase();
  const [local, domain] = lower.split("@");

  if (!domain) return true;

  const isFreeDomain =
    domain.includes("gmail.com") ||
    domain.includes("outlook.com") ||
    domain.includes("hotmail.com") ||
    domain.includes("yahoo.com");

  if (!isFreeDomain) return false;

  if (!local) return true;
  if (local.length <= 5 && COMMON_FIRST_NAMES.has(local)) return true;
  if (/^[a-z]+$/.test(local) && COMMON_FIRST_NAMES.has(local)) return true;

  return false;
}

function filterEmails(emails, websiteDomain, businessName = "") {
  const businessTokens = businessName
    .toLowerCase()
    .replace(/[^a-z0-9\s]/g, " ")
    .split(/\s+/)
    .filter(token => token.length >= 4);

  return emails.filter(email => {
    if (!email.includes("@")) return false;
    if (email.length > 60) return false;

    const lower = email.toLowerCase();

    if (BLOCKED_EMAIL_PARTS.some(part => lower.includes(part))) return false;
    if (looksLikePersonalFreeEmail(lower)) return false;

    const isFreeDomain =
      lower.includes("@gmail.com") ||
      lower.includes("@outlook.com") ||
      lower.includes("@hotmail.com") ||
      lower.includes("@yahoo.com");

    if (
      websiteDomain &&
      !lower.endsWith(`@${websiteDomain}`) &&
      !isFreeDomain
    ) {
      return false;
    }

    if (isFreeDomain && businessTokens.length > 0) {
      const local = lower.split("@")[0];
      const overlapsBusiness = businessTokens.some(token => local.includes(token));
      if (!overlapsBusiness && local.length < 10) return false;
    }

    return true;
  });
}

function pickBestEmail(emails, websiteDomain) {
  if (!emails.length) return null;

  const scored = emails.map(email => {
    const lower = email.toLowerCase();
    let score = 0;

    if (websiteDomain && lower.endsWith(`@${websiteDomain}`)) score += 10;
    if (lower.startsWith("hello@")) score += 6;
    if (lower.startsWith("info@")) score += 5;
    if (lower.startsWith("contact@")) score += 5;
    if (lower.startsWith("events@")) score += 5;
    if (lower.startsWith("catering@")) score += 5;
    if (lower.startsWith("orders@")) score += 4;
    if (lower.startsWith("office@")) score += 3;

    if (
      lower.includes("@gmail.com") ||
      lower.includes("@outlook.com") ||
      lower.includes("@hotmail.com") ||
      lower.includes("@yahoo.com")
    ) {
      score += 1;
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

  const priorityTerms = ["contact", "about", "catering"];

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

async function crawlWebsiteForEmail(website, websiteDomain, businessName, attemptNumber) {
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

      const pageEmails = filterEmails(
        extractEmails(html),
        baseDomain,
        businessName
      );
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

      const pageEmails = filterEmails(
        extractEmails(html),
        baseDomain,
        businessName
      );
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

async function scrapeInstagramForEmail(instagramUrl, businessName) {
  if (!instagramUrl) return null;

  const html = await fetchHtml(instagramUrl);
  if (!html) return null;

  const directEmails = filterEmails(extractEmails(html), null, businessName);
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

  const fallbackEmails = filterEmails(extractEmails(linkedPage), null, businessName);
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
        lead.business_name,
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
      const instagramEmail = await scrapeInstagramForEmail(
        instagramUrl,
        lead.business_name
      );
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