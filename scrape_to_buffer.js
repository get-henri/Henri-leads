const axios = require("axios");
const { createClient } = require("@supabase/supabase-js");

const OUTSCRAPER_API_KEY = process.env.OUTSCRAPER_API_KEY;
const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

if (!OUTSCRAPER_API_KEY) {
  throw new Error("Missing OUTSCRAPER_API_KEY");
}
if (!SUPABASE_URL || !SUPABASE_KEY) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_KEY");
}

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

// =========================
// INVENTORY / RUN SETTINGS
// =========================

// Active unsent lead inventory cap (buffer + ready only)
const MAX_LEADS = 400;

// Start refilling when active unsent inventory drops below this
const REFILL_TRIGGER = 200;

// Max number of new leads to add per scraper run
const BATCH_TARGET = 50;

// How many Google Maps results to ask for per search
const RESULTS_PER_SEARCH = 20;

// Wait time between checking Outscraper results
const POLL_MS = 3000;

// Max number of times to poll Outscraper before giving up
const MAX_POLLS = 10;

// Optional: throttle between search queries to be gentler / clearer in logs
const BETWEEN_SEARCH_DELAY_MS = 500;

// =========================
// STATE / CITY COVERAGE
// =========================

// "Whole state" in practice means: cover many cities/metros across each state.
// This gives much broader coverage without re-querying the same few cities forever.

const STATE_CITIES = {
  FL: [
    "Miami",
    "Fort Lauderdale",
    "West Palm Beach",
    "Orlando",
    "Tampa",
    "St. Petersburg",
    "Jacksonville",
    "Sarasota",
    "Naples",
    "Fort Myers",
    "Boca Raton",
    "Pensacola"
  ],
  GA: [
    "Atlanta",
    "Savannah",
    "Augusta",
    "Athens",
    "Macon",
    "Columbus",
    "Roswell",
    "Alpharetta"
  ],
  AL: [
    "Birmingham",
    "Mobile",
    "Montgomery",
    "Huntsville",
    "Tuscaloosa"
  ],
  LA: [
    "New Orleans",
    "Baton Rouge",
    "Lafayette",
    "Shreveport",
    "Lake Charles"
  ],
  TX: [
    "Houston",
    "Dallas",
    "Austin",
    "San Antonio",
    "Fort Worth",
    "Plano",
    "Arlington",
    "Irving",
    "Corpus Christi",
    "Lubbock",
    "Waco",
    "McAllen"
  ],
  TN: [
    "Nashville",
    "Memphis",
    "Knoxville",
    "Chattanooga",
    "Clarksville"
  ],
  SC: [
    "Charleston",
    "Columbia",
    "Greenville",
    "Myrtle Beach"
  ],
  NC: [
    "Charlotte",
    "Raleigh",
    "Durham",
    "Greensboro",
    "Winston-Salem",
    "Asheville",
    "Wilmington"
  ],
  VA: [
    "Richmond",
    "Virginia Beach",
    "Norfolk",
    "Arlington",
    "Alexandria",
    "Roanoke"
  ],
  OK: [
    "Oklahoma City",
    "Tulsa",
    "Norman"
  ],
  AR: [
    "Little Rock",
    "Fayetteville",
    "Fort Smith"
  ],
  KY: [
    "Louisville",
    "Lexington",
    "Bowling Green"
  ],
  MS: [
    "Jackson",
    "Gulfport",
    "Biloxi"
  ]
};

const QUERY_TYPES = [
  "catering",
  "meal prep",
  "bakery",
  "food truck"
];

// If you want to slightly bias away from lower-yield categories later,
// we can remove food truck from some states, but for now this keeps coverage broad.

function buildSearches() {
  const searches = [];

  for (const [state, cities] of Object.entries(STATE_CITIES)) {
    for (const city of cities) {
      for (const query of QUERY_TYPES) {
        searches.push({ query, city, state });
      }
    }
  }

  return searches;
}

const SEARCHES = buildSearches();

// =========================
// FILTERS
// =========================

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

// =========================
// HELPERS
// =========================

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

  if (place.business_status && place.business_status !== "OPERATIONAL") return null;
  if (!place.website) return null;
  if ((place.rating || 0) < 3.8) return null;
  if ((place.reviews || 0) < 10) return null;
  if (isChainLike(place)) return null;
  if (isBlockedWebsite(place)) return null;
  if (shouldRejectByLeadType(place, leadType)) return null;

  const resultState = (place.state_code || place.state || "").trim().toUpperCase();
  if (resultState && resultState !== search.state.toUpperCase()) return null;

  const normalizedName = normalizeName(place.name);
  const websiteDomain = getWebsiteDomain(place.website);
  const city = (place.city || search.city || "").trim();
  const state = (place.state_code || place.state || search.state || "").trim();

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

// =========================
// DATABASE CHECKS
// =========================

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

// Count only UNSENT inventory.
// queued leads are already in Notion and should NOT reduce scraping.
async function countUsableInventory() {
  const { count, error } = await supabase
    .from("lead_buffer")
    .select("*", { count: "exact", head: true })
    .in("status", ["buffer", "ready"]);

  if (error) {
    console.error("Count usable inventory error:", error);
    return 0;
  }

  return count || 0;
}

async function countByStatus(status) {
  const { count, error } = await supabase
    .from("lead_buffer")
    .select("*", { count: "exact", head: true })
    .eq("status", status);

  if (error) {
    console.error(`Count ${status} error:`, error);
    return 0;
  }

  return count || 0;
}

// =========================
// OUTSCRAPER
// =========================

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

// =========================
// SEARCH ROTATION
// =========================

// Rotates start position automatically based on 6-hour windows.
// This prevents re-hitting the same few searches every run.
function getRotatedSearches() {
  const windowIndex = Math.floor(Date.now() / (6 * 60 * 60 * 1000));
  const startIndex = windowIndex % SEARCHES.length;

  return [
    ...SEARCHES.slice(startIndex),
    ...SEARCHES.slice(0, startIndex)
  ];
}

// =========================
// SCRAPE ONE SEARCH
// =========================

async function scrapeOneSearch(search, maxInsertsRemaining) {
  if (maxInsertsRemaining <= 0) {
    return {
      query: `${search.query}, ${search.city}, ${search.state}, US`,
      rawPlaces: 0,
      transformed: 0,
      inserted: 0,
      skippedDuplicates: 0
    };
  }

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
    if (inserted >= maxInsertsRemaining) {
      console.log(`Reached this run's insert cap (${maxInsertsRemaining}) for ${queryString}`);
      break;
    }

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

// =========================
// MAIN
// =========================

async function main() {
  const startingInventory = await countUsableInventory();
  const startingBuffer = await countByStatus("buffer");
  const startingReady = await countByStatus("ready");
  const startingQueued = await countByStatus("queued");
  const startingFailed = await countByStatus("failed");

  console.log(`Starting usable inventory (buffer + ready): ${startingInventory}`);
  console.log(`Starting buffer: ${startingBuffer}`);
  console.log(`Starting ready: ${startingReady}`);
  console.log(`Starting queued: ${startingQueued}`);
  console.log(`Starting failed: ${startingFailed}`);
  console.log(`Refill trigger: ${REFILL_TRIGGER}`);
  console.log(`Max leads: ${MAX_LEADS}`);
  console.log(`Batch target this run: ${BATCH_TARGET}`);
  console.log(`Total search buckets: ${SEARCHES.length}`);

  if (startingInventory >= REFILL_TRIGGER) {
    console.log(
      `Inventory is at/above refill trigger (${startingInventory} >= ${REFILL_TRIGGER}). Exiting without scraping.`
    );
    return;
  }

  const roomUntilMax = MAX_LEADS - startingInventory;
  const targetThisRun = Math.min(BATCH_TARGET, roomUntilMax);

  if (targetThisRun <= 0) {
    console.log("Already at max capacity. Exiting.");
    return;
  }

  console.log(`This run will try to add up to ${targetThisRun} leads.`);

  const rotatedSearches = getRotatedSearches();

  let totalRaw = 0;
  let totalTransformed = 0;
  let totalInserted = 0;
  let totalSkippedDuplicates = 0;

  for (const search of rotatedSearches) {
    const remainingForRun = targetThisRun - totalInserted;

    if (remainingForRun <= 0) {
      console.log(`\nRun target reached. Inserted ${totalInserted} leads. Stopping scraper.`);
      break;
    }

    const currentInventory = await countUsableInventory();
    if (currentInventory >= MAX_LEADS) {
      console.log(`\nMax inventory reached (${currentInventory}). Stopping scraper.`);
      break;
    }

    const result = await scrapeOneSearch(search, remainingForRun);

    totalRaw += result.rawPlaces;
    totalTransformed += result.transformed;
    totalInserted += result.inserted;
    totalSkippedDuplicates += result.skippedDuplicates;

    const updatedInventory = await countUsableInventory();
    console.log(`Current usable inventory: ${updatedInventory}`);

    if (BETWEEN_SEARCH_DELAY_MS > 0) {
      await sleep(BETWEEN_SEARCH_DELAY_MS);
    }
  }

  const finalInventory = await countUsableInventory();
  const finalBuffer = await countByStatus("buffer");
  const finalReady = await countByStatus("ready");
  const finalQueued = await countByStatus("queued");
  const finalFailed = await countByStatus("failed");

  console.log("\nFinished scrape_to_buffer");
  console.log({
    refillTrigger: REFILL_TRIGGER,
    maxLeads: MAX_LEADS,
    targetThisRun,
    startingInventory,
    finalInventory,
    startingBuffer,
    finalBuffer,
    startingReady,
    finalReady,
    startingQueued,
    finalQueued,
    startingFailed,
    finalFailed,
    totalRaw,
    totalTransformed,
    totalInserted,
    totalSkippedDuplicates
  });
}

main().catch(err => {
  console.error("Fatal error:", err.message);
  process.exit(1);
});