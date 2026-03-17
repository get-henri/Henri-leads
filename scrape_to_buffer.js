const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");

const OUTSCRAPER_API_KEY = process.env.OUTSCRAPER_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// How many leads you want sitting in buffer before scraper stops
const TARGET_BUFFER = 500;

// How many Google Maps results to ask for per search
const RESULTS_PER_SEARCH = 20;

// Wait time between checking Outscraper results
const POLL_MS = 3000;

// Max number of times to poll Outscraper before giving up
const MAX_POLLS = 10;

// Southern states only
const SEARCHES = [
  { query: "catering", city: "Tampa", state: "FL" },
  { query: "meal prep", city: "Tampa", state: "FL" },
  { query: "bakery", city: "Tampa", state: "FL" },

  { query: "catering", city: "Orlando", state: "FL" },
  { query: "meal prep", city: "Orlando", state: "FL" },
  { query: "bakery", city: "Orlando", state: "FL" },

  { query: "catering", city: "Miami", state: "FL" },
  { query: "meal prep", city: "Miami", state: "FL" },
  { query: "bakery", city: "Miami", state: "FL" },

  { query: "catering", city: "Jacksonville", state: "FL" },
  { query: "food truck", city: "Jacksonville", state: "FL" },

  { query: "catering", city: "Atlanta", state: "GA" },
  { query: "meal prep", city: "Atlanta", state: "GA" },
  { query: "bakery", city: "Atlanta", state: "GA" },
  { query: "food truck", city: "Atlanta", state: "GA" },

  { query: "catering", city: "Savannah", state: "GA" },
  { query: "bakery", city: "Savannah", state: "GA" },

  { query: "catering", city: "Birmingham", state: "AL" },
  { query: "meal prep", city: "Birmingham", state: "AL" },
  { query: "bakery", city: "Birmingham", state: "AL" },

  { query: "catering", city: "Mobile", state: "AL" },
  { query: "meal prep", city: "Mobile", state: "AL" },

  { query: "catering", city: "New Orleans", state: "LA" },
  { query: "meal prep", city: "New Orleans", state: "LA" },
  { query: "bakery", city: "New Orleans", state: "LA" },
  { query: "food truck", city: "New Orleans", state: "LA" },

  { query: "catering", city: "Baton Rouge", state: "LA" },
  { query: "meal prep", city: "Baton Rouge", state: "LA" },

  { query: "catering", city: "Houston", state: "TX" },
  { query: "meal prep", city: "Houston", state: "TX" },
  { query: "bakery", city: "Houston", state: "TX" },
  { query: "food truck", city: "Houston", state: "TX" },

  { query: "catering", city: "Dallas", state: "TX" },
  { query: "meal prep", city: "Dallas", state: "TX" },
  { query: "bakery", city: "Dallas", state: "TX" },
  { query: "food truck", city: "Dallas", state: "TX" },

  { query: "catering", city: "Austin", state: "TX" },
  { query: "meal prep", city: "Austin", state: "TX" },
  { query: "bakery", city: "Austin", state: "TX" },
  { query: "food truck", city: "Austin", state: "TX" },

  { query: "catering", city: "San Antonio", state: "TX" },
  { query: "meal prep", city: "San Antonio", state: "TX" },
  { query: "bakery", city: "San Antonio", state: "TX" },

  { query: "catering", city: "Nashville", state: "TN" },
  { query: "meal prep", city: "Nashville", state: "TN" },
  { query: "bakery", city: "Nashville", state: "TN" },
  { query: "food truck", city: "Nashville", state: "TN" },

  { query: "catering", city: "Memphis", state: "TN" },
  { query: "meal prep", city: "Memphis", state: "TN" },

  { query: "catering", city: "Charleston", state: "SC" },
  { query: "bakery", city: "Charleston", state: "SC" },

  { query: "catering", city: "Columbia", state: "SC" },
  { query: "meal prep", city: "Columbia", state: "SC" },

  { query: "catering", city: "Charlotte", state: "NC" },
  { query: "meal prep", city: "Charlotte", state: "NC" },
  { query: "bakery", city: "Charlotte", state: "NC" },
  { query: "food truck", city: "Charlotte", state: "NC" },

  { query: "catering", city: "Raleigh", state: "NC" },
  { query: "meal prep", city: "Raleigh", state: "NC" },
  { query: "bakery", city: "Raleigh", state: "NC" },

  { query: "catering", city: "Richmond", state: "VA" },
  { query: "meal prep", city: "Richmond", state: "VA" },
  { query: "bakery", city: "Richmond", state: "VA" },

  { query: "catering", city: "Oklahoma City", state: "OK" },
  { query: "meal prep", city: "Oklahoma City", state: "OK" },
  { query: "bakery", city: "Oklahoma City", state: "OK" },

  { query: "catering", city: "Little Rock", state: "AR" },
  { query: "meal prep", city: "Little Rock", state: "AR" },
  { query: "bakery", city: "Little Rock", state: "AR" },

  { query: "catering", city: "Louisville", state: "KY" },
  { query: "meal prep", city: "Louisville", state: "KY" },
  { query: "bakery", city: "Louisville", state: "KY" },

  { query: "catering", city: "Jackson", state: "MS" },
  { query: "meal prep", city: "Jackson", state: "MS" },
  { query: "bakery", city: "Jackson", state: "MS" }
];

const BLOCKED_NAME_TERMS = [
  "eataly",
  "clean eatz",
  "tous les jours",
  "corporate caterers",
  "miami grill",
  "paris baguette",
  "panera",
  "corner bakery",
  "nothing bundt cakes",
  "crumbl",
  "whole foods",
  "costco",
  "sam's club",
  "walmart",
  "target",
  "publix",
  "kroger",
  "fresh market",
  "trader joe",
  "wegmans",
  "milk bar",
  "magnolia bakery",
  "levain bakery",
  "rosetta bakery",
  "bloomingdale",
  "marriott",
  "hilton",
  "hyatt",
  "holiday inn",
  "hampton inn",
  "wyndham",
  "four seasons",
  "ritz",
  "sheraton",
  "westin",
  "omni",
  "country club",
  "resort",
  "casino"
];

const BLOCKED_WEBSITE_TERMS = [
  "ezcater.com",
  "grubhub.com",
  "doordash.com",
  "ubereats.com",
  "postmates.com",
  "seamless.com",
  "tripadvisor.com",
  "yelp.com",
  "opentable.com",
  "slice.com",
  "goldbelly.com"
];

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function normalizeName(name) {
  return (name || "")
    .toLowerCase()
    .replace(/[^\w\s]/g, " ")
    .replace(/\b(llc|inc|co|company|corp|corporation|ltd)\b/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function getWebsiteDomain(url) {
  try {
    const hostname = new URL(url).hostname.toLowerCase();
    return hostname.replace(/^www\./, "");
  } catch {
    return "";
  }
}

function containsBlockedTerm(text, blockedTerms) {
  const lower = (text || "").toLowerCase();
  return blockedTerms.some(term => lower.includes(term));
}

function isBlockedWebsite(place) {
  const website = (place.website || "").toLowerCase();
  return containsBlockedTerm(website, BLOCKED_WEBSITE_TERMS);
}

function isChainLike(place) {
  const text = [
    place.name,
    place.category,
    place.type,
    place.subtypes,
    place.description,
    place.website
  ]
    .filter(Boolean)
    .join(" | ")
    .toLowerCase();

  return containsBlockedTerm(text, BLOCKED_NAME_TERMS);
}

function classifyLeadType(place) {
  const text = [
    place.category,
    place.type,
    place.subtypes,
    place.name,
    place.description
  ]
    .filter(Boolean)
    .join(" | ")
    .toLowerCase();

  if (text.includes("food truck")) return "food_truck";

  if (text.includes("bakery")) return "bakery";

  if (
    text.includes("meal prep") ||
    text.includes("meal delivery") ||
    text.includes("prepared meal") ||
    text.includes("healthy meal") ||
    text.includes("meal plan") ||
    text.includes("meal plans")
  ) {
    return "meal_prep";
  }

  if (text.includes("cater")) return "catering";

  return null;
}

function shouldRejectByLeadType(place, leadType) {
  const text = [
    place.category,
    place.type,
    place.subtypes,
    place.name,
    place.description
  ]
    .filter(Boolean)
    .join(" | ")
    .toLowerCase();

  if (leadType === "bakery") {
    const badBakeryTerms = [
      "grocery",
      "supermarket",
      "wholesale",
      "hotel",
      "resort"
    ];
    if (badBakeryTerms.some(term => text.includes(term))) return true;
  }

  if (leadType === "meal_prep") {
    const badMealPrepTerms = [
      "vitamin shop",
      "supplement",
      "nutrition store",
      "gym",
      "fitness center"
    ];
    if (badMealPrepTerms.some(term => text.includes(term))) return true;
  }

  if (leadType === "catering") {
    const badCateringTerms = [
      "hotel",
      "resort",
      "country club",
      "banquet hall",
      "wedding venue"
    ];
    if (badCateringTerms.some(term => text.includes(term))) return true;
  }

  return false;
}

function buildPersonalizationNote(place, leadType) {
  const reviews = place.reviews || 0;
  const rating = place.rating || 0;

  if (leadType === "catering") {
    return `${rating} stars from ${reviews} reviews on Google`;
  }

  if (leadType === "bakery") {
    return `Bakery with ${rating} stars and ${reviews} Google reviews`;
  }

  if (leadType === "meal_prep") {
    return `Meal prep business with ${rating} stars and active web presence`;
  }

  if (leadType === "food_truck") {
    return `Food truck with ${rating} stars from ${reviews} reviews`;
  }

  return "Active local business";
}

function transformPlace(place, search) {
  const leadType = classifyLeadType(place);
  if (!leadType) return null;

  if (place.business_status !== "OPERATIONAL") return null;
  if (!place.website) return null;
  if ((place.rating || 0) < 3.8) return null;
  if ((place.reviews || 0) < 10) return null;
  if (isChainLike(place)) return null;
  if (isBlockedWebsite(place)) return null;
  if (shouldRejectByLeadType(place, leadType)) return null;

  const resultState = (place.state_code || place.state || "").trim().toUpperCase();
  if (resultState !== search.state.toUpperCase()) return null;

  const normalizedName = normalizeName(place.name);
  const websiteDomain = getWebsiteDomain(place.website);
  const city = (place.city || "").trim();
  const state = (place.state_code || place.state || "").trim();

  if (!normalizedName || !websiteDomain || !city || !state) return null;

  const dedupeKey =
    `${normalizedName}|${city.toLowerCase()}|${state.toLowerCase()}|${websiteDomain}`;

  return {
    business_name: place.name,
    normalized_name: normalizedName,
    lead_type: leadType,
    email: null,
    email_type: null,
    rating: place.rating || 0,
    review_count: place.reviews || 0,
    city,
    state,
    website: place.website,
    website_domain: websiteDomain,
    google_maps_url: place.location_link || null,
    phone: place.phone || null,
    personalization_note: buildPersonalizationNote(place, leadType),
    source: "google_maps",
    dedupe_key: dedupeKey,
    quality_score: Math.round((place.rating || 0) * 10 + Math.min(place.reviews || 0, 50)),
    status: "buffer"
  };
}

async function existsInTable(table, dedupeKey) {
  const { data, error } = await supabase
    .from(table)
    .select("id")
    .eq("dedupe_key", dedupeKey)
    .limit(1);

  if (error) {
    console.error(`Check ${table} error:`, error);
    return true;
  }

  return Array.isArray(data) && data.length > 0;
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

async function fetchOutscraperResults(query, limit = RESULTS_PER_SEARCH) {
  const startResponse = await axios.get(
    "https://api.app.outscraper.com/maps/search-v3",
    {
      params: { query, limit },
      headers: { "X-API-KEY": OUTSCRAPER_API_KEY }
    }
  );

  const resultsLocation = startResponse.data.results_location;
  if (!resultsLocation) {
    throw new Error("No results_location returned from Outscraper");
  }

  for (let i = 1; i <= MAX_POLLS; i++) {
    await sleep(POLL_MS);

    const resultResponse = await axios.get(resultsLocation, {
      headers: { "X-API-KEY": OUTSCRAPER_API_KEY }
    });

    const body = resultResponse.data;

    if (body.status === "Success" && body.data) {
      return body.data[0] || [];
    }
  }

  throw new Error("Outscraper results still pending after max polls");
}

async function scrapeOneSearch(search) {
  const queryString = `${search.query}, ${search.city}, ${search.state}, US`;
  console.log(`\nFetching: ${queryString}`);

  let rawPlaces = [];
  try {
    rawPlaces = await fetchOutscraperResults(queryString, RESULTS_PER_SEARCH);
  } catch (err) {
    console.error(`Fetch failed for ${queryString}:`, err.message);
    return {
      query: queryString,
      rawPlaces: 0,
      transformed: 0,
      inserted: 0,
      skippedDuplicates: 0
    };
  }

  console.log(`Fetched ${rawPlaces.length} raw places`);

  const transformed = rawPlaces
    .map(place => transformPlace(place, search))
    .filter(Boolean);

  console.log(`After filtering: ${transformed.length} leads`);

  let inserted = 0;
  let skippedDuplicates = 0;
  const seenThisRun = new Set();

  for (const lead of transformed) {
    if (seenThisRun.has(lead.dedupe_key)) {
      console.log("Skipping same-run duplicate:", lead.business_name);
      skippedDuplicates++;
      continue;
    }

    seenThisRun.add(lead.dedupe_key);

    const inBuffer = await existsInTable("lead_buffer", lead.dedupe_key);
    const inHistory = await existsInTable("lead_history", lead.dedupe_key);

    if (inBuffer || inHistory) {
      console.log("Skipping duplicate:", lead.business_name);
      skippedDuplicates++;
      continue;
    }

    const { error } = await supabase
      .from("lead_buffer")
      .insert([lead]);

    if (error) {
      console.error("Insert error:", lead.business_name, error);
      continue;
    }

    console.log("Inserted:", lead.business_name);
    inserted++;
  }

  return {
    query: queryString,
    rawPlaces: rawPlaces.length,
    transformed: transformed.length,
    inserted,
    skippedDuplicates
  };
}

async function main() {
  if (!OUTSCRAPER_API_KEY) {
    console.error("Missing OUTSCRAPER_API_KEY");
    return;
  }

  let bufferCount = await countBuffer();
  console.log(`Starting buffer count: ${bufferCount}`);
  console.log(`Target buffer count: ${TARGET_BUFFER}`);

  let totalRaw = 0;
  let totalTransformed = 0;
  let totalInserted = 0;
  let totalSkippedDuplicates = 0;

  for (const search of SEARCHES) {
    if (bufferCount >= TARGET_BUFFER) {
      console.log(`\nTarget reached. Buffer is now ${bufferCount}. Stopping scraper.`);
      break;
    }

    const result = await scrapeOneSearch(search);

    totalRaw += result.rawPlaces;
    totalTransformed += result.transformed;
    totalInserted += result.inserted;
    totalSkippedDuplicates += result.skippedDuplicates;

    bufferCount = await countBuffer();
    console.log(`Current buffer count: ${bufferCount}`);
  }

  console.log("\nFinished scrape_to_buffer");
  console.log({
    targetBuffer: TARGET_BUFFER,
    finalBuffer: bufferCount,
    totalRaw,
    totalTransformed,
    totalInserted,
    totalSkippedDuplicates
  });
}

main().catch(err => {
  console.error("Fatal error:", err.message);
});