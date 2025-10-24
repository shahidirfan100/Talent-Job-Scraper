# Talent.com Job Scraper (Apify + Crawlee + gotScraping)

This Apify actor scrapes job listings from Talent.com using Crawlee + Cheerio and falls back to got-scraping HTTP fetches when needed. It prioritizes JSON-LD JobPosting schema extraction, then DOM selectors as a fallback.

## Features
- JSON-LD extraction (JobPosting schema)
- Robust DOM selectors with multiple fallbacks
- Pagination handling and detail-page enqueueing
- Optional HTTP fallback using got-scraping
- Apify proxy support via `proxyConfiguration`

## Input parameters
- `searchQuery` (string) — search keywords, e.g. "developer"
- `location` (string) — city / region, e.g. "Lahore"
- `maxItems` (number) — maximum job items to collect (default 100)
- `maxPages` (number) — how many listing pages to visit (default 3)
- `proxyConfiguration` (object) — pass Apify proxy configuration
- `delayBetweenRequests` (number) — ms delay between requests (default 1500)
- `maxConcurrency` (number) — crawler concurrency (default 3)
- `debugMode` (boolean) — enable extra logs

## Output
The actor pushes items into the default dataset. Example item:

```json
{
  "title": "Frontend Developer",
  "company": "Talent Group",
  "location": "Karachi, Pakistan",
  "url": "https://www.talent.com/view?id=xxxx",
  "salary": "PKR 180,000/month",
  "date_posted": "2025-10-18",
  "description_html": "<div>Job details here...</div>",
  "description_text": "Job details here..."
}
```

## Run locally
1. Install dependencies:

```powershell
npm install
```

2. Run the actor (uses `Actor.getInput()` when run in Apify; locally it will use defaults unless you modify `src/main.js`):

```powershell
node .\src\main.js
```

To test with specific input locally, create an `INPUT.json` in the project root with the actor input (Apify runner will pick it up when running in the Apify CLI / actor environment):

```json
{
  "searchQuery": "developer",
  "location": "Lahore",
  "maxItems": 20,
  "maxPages": 2
}
```

## Apify actor metadata
This repository includes a minimal `actor.json` so it passes basic Apify QA checks (main entry, input schema). See `actor.json` for details.

## Notes & next steps
- If you see 403/429 responses, enable Apify proxy or slow down `delayBetweenRequests`.
- For higher reliability, consider adding tests for the JSON-LD parser and caching sample HTML fixtures.
