const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

async function main() {
  const lead = {
    business_name: "Sunrise Catering Co",
    normalized_name: "sunrise catering co",
    lead_type: "catering",
    email: "hello@sunrisecateringco.com",
    email_type: "general",
    rating: 4.6,
    review_count: 47,
    city: "Tampa",
    state: "FL",
    website: "https://sunrisecateringco.com",
    website_domain: "sunrisecateringco.com",
    google_maps_url: "https://maps.google.com/?q=Sunrise+Catering+Co+Tampa",
    phone: "813-555-1234",
    personalization_note: "Offers wedding and corporate catering",
    source: "google_maps",
    dedupe_key: "sunrise catering co|tampa|fl|sunrisecateringco.com",
    quality_score: 88,
    status: "buffer"
  };

  const { data, error } = await supabase
    .from("lead_buffer")
    .insert([lead])
    .select();

  if (error) {
    console.error("Insert error:", error);
    return;
  }

  console.log("Inserted lead:", data[0]);
}

main();