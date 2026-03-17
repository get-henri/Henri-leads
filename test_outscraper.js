const axios = require("axios");

const API_KEY = process.env.OUTSCRAPER_API_KEY;

function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function main() {
  try {
    const startResponse = await axios.get(
      "https://api.app.outscraper.com/maps/search-v3",
      {
        params: {
          query: "catering, Tampa, FL, US",
          limit: 5
        },
        headers: {
          "X-API-KEY": API_KEY
        }
      }
    );

    console.log("START RESPONSE:");
    console.log(JSON.stringify(startResponse.data, null, 2));

    const resultsLocation = startResponse.data.results_location;
    if (!resultsLocation) {
      console.log("No results_location returned.");
      return;
    }

    for (let i = 1; i <= 10; i++) {
      console.log(`Checking results... attempt ${i}`);
      await sleep(3000);

      const resultResponse = await axios.get(resultsLocation, {
        headers: {
          "X-API-KEY": API_KEY
        }
      });

      console.log("RESULT RESPONSE:");
      console.log(JSON.stringify(resultResponse.data, null, 2));

      if (
        resultResponse.data.status === "Success" ||
        resultResponse.data.data
      ) {
        console.log("Task finished.");
        return;
      }
    }

    console.log("Still pending after 10 checks.");
  } catch (error) {
    if (error.response) {
      console.error("API error:", error.response.status, error.response.data);
    } else {
      console.error("Error:", error.message);
    }
  }
}

main();