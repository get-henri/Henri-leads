const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_KEY = process.env.SUPABASE_SERVICE_KEY;

const { createClient } = require("@supabase/supabase-js");

const supabase = createClient(SUPABASE_URL, SUPABASE_KEY);

async function main() {
  const leads = [
    {
      business_name: "Cypress Coast Catering",
      normalized_name: "cypress coast catering",
      lead_type: "catering",
      email: "hello@cypresscoastcatering.com",
      email_type: "general",
      rating: 4.6,
      review_count: 41,
      city: "Pensacola",
      state: "FL",
      website: "https://cypresscoastcatering.com",
      website_domain: "cypresscoastcatering.com",
      google_maps_url: "https://maps.google.com/?q=Cypress+Coast+Catering+Pensacola",
      phone: "850-555-1111",
      personalization_note: "Offers wedding and private event catering",
      source: "google_maps",
      dedupe_key: "cypress coast catering|pensacola|fl|cypresscoastcatering.com",
      quality_score: 89,
      status: "buffer"
    },
    {
      business_name: "Southern Crumb Bakery",
      normalized_name: "southern crumb bakery",
      lead_type: "bakery",
      email: "orders@southerncrumbbakery.com",
      email_type: "general",
      rating: 4.7,
      review_count: 58,
      city: "Birmingham",
      state: "AL",
      website: "https://southerncrumbbakery.com",
      website_domain: "southerncrumbbakery.com",
      google_maps_url: "https://maps.google.com/?q=Southern+Crumb+Bakery+Birmingham",
      phone: "205-555-2222",
      personalization_note: "Custom cakes and dessert catering",
      source: "google_maps",
      dedupe_key: "southern crumb bakery|birmingham|al|southerncrumbbakery.com",
      quality_score: 91,
      status: "buffer"
    },
    {
      business_name: "Bayou Fresh Meal Prep",
      normalized_name: "bayou fresh meal prep",
      lead_type: "meal_prep",
      email: "info@bayoufreshmealprep.com",
      email_type: "general",
      rating: 4.5,
      review_count: 36,
      city: "Baton Rouge",
      state: "LA",
      website: "https://bayoufreshmealprep.com",
      website_domain: "bayoufreshmealprep.com",
      google_maps_url: "https://maps.google.com/?q=Bayou+Fresh+Meal+Prep+Baton+Rouge",
      phone: "225-555-3333",
      personalization_note: "Weekly prepared meal delivery",
      source: "google_maps",
      dedupe_key: "bayou fresh meal prep|baton rouge|la|bayoufreshmealprep.com",
      quality_score: 87,
      status: "buffer"
    },
    {
      business_name: "Peachtree Street Bites",
      normalized_name: "peachtree street bites",
      lead_type: "food_truck",
      email: "hello@peachtreestreetbites.com",
      email_type: "general",
      rating: 4.4,
      review_count: 29,
      city: "Atlanta",
      state: "GA",
      website: "https://peachtreestreetbites.com",
      website_domain: "peachtreestreetbites.com",
      google_maps_url: "https://maps.google.com/?q=Peachtree+Street+Bites+Atlanta",
      phone: "404-555-4444",
      personalization_note: "Popular local food truck for events",
      source: "google_maps",
      dedupe_key: "peachtree street bites|atlanta|ga|peachtreestreetbites.com",
      quality_score: 84,
      status: "buffer"
    },
    {
      business_name: "Lowcountry Event Kitchen",
      normalized_name: "lowcountry event kitchen",
      lead_type: "catering",
      email: "events@lowcountryeventkitchen.com",
      email_type: "general",
      rating: 4.8,
      review_count: 67,
      city: "Savannah",
      state: "GA",
      website: "https://lowcountryeventkitchen.com",
      website_domain: "lowcountryeventkitchen.com",
      google_maps_url: "https://maps.google.com/?q=Lowcountry+Event+Kitchen+Savannah",
      phone: "912-555-5555",
      personalization_note: "Specializes in weddings and private dinners",
      source: "google_maps",
      dedupe_key: "lowcountry event kitchen|savannah|ga|lowcountryeventkitchen.com",
      quality_score: 94,
      status: "buffer"
    }
  ];

  const { data, error } = await supabase
    .from("lead_buffer")
    .insert(leads)
    .select();

  if (error) {
    console.error("Insert error:", error);
    return;
  }

  console.log(`Inserted ${data.length} leads into lead_buffer`);
}

main();