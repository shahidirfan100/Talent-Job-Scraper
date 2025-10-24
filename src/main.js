import { Actor, log } from 'apify';
import { CheerioCrawler } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';
import { gotScraping } from 'got-scraping';

// ============================================================================
// ANTI-SCRAPING COUNTERMEASURES & UTILITIES
// ============================================================================

// Rotating User-Agent pool for bot detection bypass
const USER_AGENTS = [
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
  'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
  'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
];

// Realistic browser headers to mimic real user
const BROWSER_HEADERS = {
  'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
  'Accept-Encoding': 'gzip, deflate, br',
  'Accept-Language': 'en-US,en;q=0.9',
  'Cache-Control': 'no-cache',
  'Pragma': 'no-cache',
  'Upgrade-Insecure-Requests': '1',
  'Sec-Fetch-Dest': 'document',
  'Sec-Fetch-Mode': 'navigate',
  'Sec-Fetch-Site': 'none',
  'Sec-Fetch-User': '?1',
  'Sec-Ch-Ua': '"Not_A Brand";v="8", "Chromium";v="120"',
  'Sec-Ch-Ua-Mobile': '?0',
  'Sec-Ch-Ua-Platform': '"macOS"',
};

/**
 * Normalize @type fields that can be strings or arrays
 */
function isJobPostingType(value) {
  if (!value) return false;
  if (Array.isArray(value)) return value.includes('JobPosting');
  return value === 'JobPosting';
}

/**
 * Get random user agent for rotation (anti-bot detection)
 */
function getRandomUserAgent() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

/**
 * Generate realistic request delay between 2-5 seconds
 */
async function delayRequest(minMs = 2000, maxMs = 5000) {
  const delay = Math.random() * (maxMs - minMs) + minMs;
  await new Promise(resolve => setTimeout(resolve, delay));
}

/**
 * Extract JobPosting objects from JSON-LD script tags
 * Robust extraction supporting multiple JSON-LD structures
 */
function extractJobPostingsFromJsonLd($) {
  const found = [];

  $('script[type="application/ld+json"]').each((_index, element) => {
    const text = $(element).text();
    if (!text || text.length === 0) return;

    try {
      const data = JSON.parse(text);

      // Handle different JSON-LD structures
      if (Array.isArray(data)) {
        data.forEach((item) => {
          if (isJobPostingType(item['@type'])) {
            found.push(item);
          }
        });
      } else if (isJobPostingType(data['@type'])) {
        found.push(data);
      } else if (data['@graph']) {
        const graph = Array.isArray(data['@graph']) ? data['@graph'] : [data['@graph']];
        graph.forEach((item) => {
          if (isJobPostingType(item['@type'])) {
            found.push(item);
          }
        });
      }
    } catch (error) {
      // Silently skip malformed JSON
    }
  });

  return found;
}

/**
 * Safely resolve relative URLs to absolute
 */
function resolveUrl(href, baseUrl) {
  if (!href || typeof href !== 'string') return null;
  try {
    return new URL(href, baseUrl).toString();
  } catch (_error) {
    return null;
  }
}

/**
 * Extract location from JSON-LD jobLocation
 */
function extractLocationFromJsonLd(jobLocation) {
  if (!jobLocation) return '';

  if (Array.isArray(jobLocation)) {
    return jobLocation
      .map(loc => loc.address?.addressLocality || loc.address?.streetAddress || '')
      .filter(l => l)
      .join(', ');
  }

  if (typeof jobLocation === 'object') {
    return jobLocation.address?.addressLocality || jobLocation.address?.streetAddress || '';
  }

  return String(jobLocation);
}

/**
 * Extract salary from JSON-LD baseSalary
 */
function extractSalaryFromJsonLd(baseSalary) {
  if (!baseSalary) return '';

  if (baseSalary.value?.minValue && baseSalary.value?.maxValue) {
    const currency = baseSalary.currency || '';
    return `${currency} ${baseSalary.value.minValue} - ${baseSalary.value.maxValue}`;
  }

  return '';
}

/**
 * Extract job listings from Next.js streamed payload (jobListData)
 */
function extractJobsFromNextData(html, baseUrl) {
  if (!html || typeof html !== 'string') return [];

  const key = '"jobListData":';
  const keyIndex = html.indexOf(key);
  if (keyIndex === -1) return [];

  let index = keyIndex + key.length;
  while (index < html.length && /\s/.test(html[index])) index++;
  if (html[index] !== '[') return [];

  const start = index;
  let depth = 0;
  let inString = false;
  let isEscaped = false;
  let end = -1;

  for (let i = start; i < html.length; i++) {
    const char = html[i];

    if (isEscaped) {
      isEscaped = false;
      continue;
    }

    if (char === '\\') {
      isEscaped = true;
      continue;
    }

    if (char === '"') {
      inString = !inString;
      continue;
    }

    if (inString) continue;

    if (char === '[') depth++;
    if (char === ']') {
      depth--;
      if (depth === 0) {
        end = i + 1;
        break;
      }
    }
  }

  if (end === -1) return [];

  const jsonSlice = html.slice(start, end);
  let parsed;
  try {
    parsed = JSON.parse(jsonSlice);
  } catch (error) {
    log.debug(`Failed to parse jobListData JSON: ${error.message}`);
    return [];
  }

  if (!Array.isArray(parsed)) return [];

  const seenIds = new Set();

  return parsed
    .filter(job => job && typeof job === 'object')
    .map(job => ({
      id: job.id || job.system_legacy_id || null,
      title: job.source_title || job.distilled_title || job.title || '',
      company: job.enrich_company_name || job.source_company_name || job.company || '',
      location: job.source_location || job.enrich_geo_city || '',
      jobType: Array.isArray(job.enrich_jobtype) ? job.enrich_jobtype.join(', ') : job.employmentType || '',
      salary: job.enrich_salary_snippet || job.salary || '',
      description: (job.source_jobdesc_text || job.description || '').substring(0, 500),
      datePosted: job.system_date_found || job.datePosted || '',
      url: resolveUrl(job.source_link || job.url || '', baseUrl),
    }))
    .filter(job => {
      if (!job.title || !job.url) return false;
      if (seenIds.has(job.url)) return false;
      seenIds.add(job.url);
      return true;
    });
}

/**
 * Extract job data from DOM using flexible selectors with fallbacks
 * Multiple selector strategies for resilience against HTML changes
 */
function extractJobsFromDOM($, baseUrl) {
  const jobs = [];
  const seenUrls = new Set();

  // Primary job card selectors (tested on talent.com)
  const cardSelectors = [
    '[data-testid="JobCardContainer"]',
    'header[data-testid="JobCard"]',
    'article',
    '[class*="job-card"]',
    '[class*="job-listing"]',
    '[class*="search-result"]',
    '.job',
    '.card',
  ];

  let jobCards = $();
  for (const selector of cardSelectors) {
    jobCards = $(selector);
    if (jobCards.length > 0) break;
  }

  jobCards.each((_index, element) => {
    const $el = $(element);
    const $container = $el.is('[data-testid="JobCardContainer"]') ? $el : $el.find('[data-testid="JobCardContainer"]').first() || $el;
    const infoTexts = $container
      .find('span')
      .map((_idx, node) => $(node).text().trim())
      .get()
      .map(text => text.replace(/\s+/g, ' '))
      .filter(text => text && !/this job offer is not available/i.test(text));

    // Extract title with multiple fallbacks
    const titleSelectors = ['h2 a', 'h3 a', 'a.job-link', 'a[href*="/view?id="]', 'h2', 'h3'];
    let titleEl = null;
    let href = null;

    for (const selector of titleSelectors) {
      titleEl = $container.find(selector).first();
      if (titleEl.length > 0 && titleEl.is('h2, h3') && titleEl.find('a').length > 0) {
        titleEl = titleEl.find('a').first();
      }
      if (titleEl.length > 0) {
        href = titleEl.attr('href') || $container.closest('a').attr('href') || $container.find('a[href*="/view?id="]').first().attr('href');
        break;
      }
    }

    if (!titleEl || titleEl.length === 0) return;

    const title = titleEl.text().trim();
    const absoluteUrl = resolveUrl(href, baseUrl);

    if (!title || !absoluteUrl) return;
    if (seenUrls.has(absoluteUrl)) return;

    seenUrls.add(absoluteUrl);

    // Extract company
    const companySelectors = [
      '[class*="company"]',
      '[class*="employer"]',
      '[class*="org-name"]',
    ];
    let company = '';
    for (const selector of companySelectors) {
      const el = $container.find(selector).filter((_, node) => $(node).text().trim().length > 0).first();
      if (el.length > 0) {
        company = el.text().trim();
        break;
      }
    }
    if (!company && infoTexts.length > 0) {
      company = infoTexts[0];
    }

    // Extract location
    const locationSelectors = [
      '[class*="location"]',
      '[class*="place"]',
      '[class*="address"]',
    ];
    let location = '';
    for (const selector of locationSelectors) {
      const el = $container.find(selector).first();
      if (el.length > 0) {
        const candidate = el.text().trim();
        if (candidate) {
          location = candidate;
          if (/,/.test(location) || /remote/i.test(location)) break;
        }
      }
    }
    if (!location) {
      const locationCandidate = infoTexts.slice(1).find(text => text.includes(',') || /remote/i.test(text));
      if (locationCandidate) location = locationCandidate;
    }

    // Extract job types (Full-time, Part-time, Remote, etc.)
    const jobTypeElements = $container.find('[data-testid="badge"], [class*="badge"], [class*="tag"], [class*="employment"]');
    const jobTypes = [];
    jobTypeElements.each((_idx, typeElement) => {
      const type = $(typeElement).text().trim();
      if (type && type.length < 50) {
        jobTypes.push(type);
      }
    });

    // Extract salary if present in title
    const salaryMatch = title.match(/\$[\d,]+\s*[--]\s*\$[\d,]+/);
    const salary = salaryMatch ? salaryMatch[0] : null;

    // Extract snippet/description
    const snippetSelectors = [
      '[data-testid="JobCardDescription"]',
      '[class*="snippet"]',
      '[class*="summary"]',
      '[class*="description"]',
      'p',
    ];
    let snippet = '';
    for (const selector of snippetSelectors) {
      const el = $container.find(selector).first();
      if (el.length > 0) {
        snippet = el.text().trim();
        if (snippet.length > 20) break;
      }
    }

    // Extract posted date
    const dateSelectors = [
      '[data-testid="JobCardContainer"] time',
      '[class*="date"]',
      'time',
      '[class*="posted"]',
    ];
    let postedDate = '';
    for (const selector of dateSelectors) {
      const el = $container.find(selector).first();
      if (el.length > 0) {
        postedDate = el.text().trim();
        break;
      }
    }
    if (!postedDate) {
      const dateCandidate = infoTexts.find(text => /\bday\b|\bweek\b|\bmonth\b|ago/i.test(text));
      if (dateCandidate) postedDate = dateCandidate;
    }

    jobs.push({
      title,
      company,
      location,
      jobTypes: jobTypes.join(', ') || 'Not specified',
      salary,
      snippet: snippet.substring(0, 200),
      url: absoluteUrl,
      postedDate,
      source: 'talent.com',
    });
  });

  return jobs;
}

/**
 * Extract detailed job information from detail page
 */
function extractJobDetail($, baseUrl, jobId) {
  // Try JSON-LD first (most reliable)
  const jsonLdJobs = extractJobPostingsFromJsonLd($);
  if (jsonLdJobs.length > 0) {
    const job = jsonLdJobs[0];
    return {
      jobId,
      title: job.title || '',
      company: job.hiringOrganization?.name || '',
      location: extractLocationFromJsonLd(job.jobLocation),
      jobType: job.employmentType || '',
      salary: extractSalaryFromJsonLd(job.baseSalary),
      description: (job.description || '').substring(0, 1000),
      descriptionHtml: job.description || '',
      datePosted: job.datePosted || '',
      url: baseUrl,
    };
  }

  // Fallback to DOM extraction
  const titleSelectors = ['h1', 'h2', '[class*="job-title"]'];
  let title = '';
  for (const selector of titleSelectors) {
    const el = $(selector).first();
    if (el.length > 0) {
      title = el.text().trim();
      break;
    }
  }

  const descriptionSelectors = [
    '[class*="job-description"]',
    '[class*="description"]',
    '.job-content',
    'main',
  ];
  let descriptionHtml = '';
  for (const selector of descriptionSelectors) {
    const el = $(selector).first();
    if (el.length > 0) {
      descriptionHtml = el.html() || '';
      break;
    }
  }

  const descriptionText = descriptionHtml ? cheerioLoad(descriptionHtml).text() : '';

  return {
    jobId,
    title,
    description: descriptionText.substring(0, 1000),
    descriptionHtml,
    url: baseUrl,
  };
}

// ============================================================================
// MAIN ACTOR LOGIC
// ============================================================================

await Actor.init();

try {
  const input = await Actor.getInput() || {};

  const {
    startUrl = '',
    location: rawLocation = '',
    searchQuery: rawSearchQuery = '',
    maxPages = 5,
    maxItems = 100,
    includeJobDetails = false,
    proxyConfiguration = null,
    delayBetweenRequests = 3000,
    maxConcurrency = 1,
    debugMode = false,
    cookies = '',
    cookiesJson = '',
  } = input;

  const location = typeof rawLocation === 'string' ? rawLocation.trim() : '';
  let searchQuery = typeof rawSearchQuery === 'string' ? rawSearchQuery.trim() : '';

  // apify.log may not expose getLogger in some SDK versions in the runtime.
  // Create a small prefixed wrapper to avoid relying on getLogger.
  const crawlerLog = {
    info: (...args) => log.info('[TALENT.COM-CRAWLER]', ...args),
    warning: (...args) => log.warning ? log.warning('[TALENT.COM-CRAWLER]', ...args) : log.info('[TALENT.COM-CRAWLER] WARNING', ...args),
    error: (...args) => log.error('[TALENT.COM-CRAWLER]', ...args),
    debug: (...args) => log.debug ? log.debug('[TALENT.COM-CRAWLER]', ...args) : log.info('[TALENT.COM-CRAWLER] DEBUG', ...args),
  };

  // Runtime validation: Apify input schema doesn't support conditional
  // "either-or" requirements (e.g., at least one of startUrl or searchQuery),
  // so validate here and fail fast with a clear error message.
  if (!startUrl && !rawSearchQuery) {
    throw new Error('Input validation error: please provide either `startUrl` (a Talent.com search URL) or `searchQuery` (keyword) in the actor input.');
  }

  // Build start URLs with pagination
  const startUrls = [];
  
  if (startUrl) {
    // User provided a specific URL
    crawlerLog.info(`Using custom start URL: ${startUrl}`);
    startUrls.push({
      url: startUrl,
      userData: {
        label: 'LIST',
        page: 1,
      },
    });
  } else {
    // Build search URLs from provided searchQuery

    for (let page = 1; page <= maxPages; page++) {
      let url = `https://www.talent.com/jobs?k=${encodeURIComponent(searchQuery)}`;
      if (location) url += `&l=${encodeURIComponent(location)}`;
      if (page > 1) url += `&p=${page}`;

      startUrls.push({
        url,
        userData: {
          label: 'LIST',
          page,
        },
      });
    }
  }

  crawlerLog.info(`Starting Talent.com scraper - Query: "${searchQuery}", Location: "${location}", MaxItems: ${maxItems}`);
  crawlerLog.info(`Generated ${startUrls.length} start URLs`);

  // Parse custom cookies if provided
  let parsedCookies = [];
  if (cookiesJson) {
    try {
      const parsed = JSON.parse(cookiesJson);
      if (Array.isArray(parsed)) {
        parsedCookies = parsed;
      } else if (typeof parsed === 'object') {
        parsedCookies = Object.entries(parsed).map(([name, value]) => ({ name, value }));
      }
    } catch (err) {
      crawlerLog.warning('Failed to parse cookiesJson:', err.message);
    }
  }

  // Create proxy configuration for IP rotation
  let proxyConf;
  try {
    proxyConf = await Actor.createProxyConfiguration(proxyConfiguration ?? undefined);
  } catch (proxyError) {
    crawlerLog.warning(`Proxy configuration failed: ${proxyError.message}. Proceeding without proxy.`);
  }

  let itemCount = 0;

  const crawler = new CheerioCrawler({
    proxyConfiguration: proxyConf,
    maxRequestRetries: 5,
    maxConcurrency,
    requestHandlerTimeoutSecs: 60,
    maxRequestsPerMinute: 30, // Rate limit: 30 requests per minute
    useSessionPool: true,

    preNavigationHooks: [
      async ({ request, session }) => {
        // Add custom headers and cookies
        request.headers = {
          ...(request.headers ?? {}),
          ...BROWSER_HEADERS,
          'User-Agent': getRandomUserAgent(),
        };

        if (cookies) {
          request.headers.Cookie = cookies;
        }

        if (parsedCookies.length > 0 && session) {
          session.setCookies(parsedCookies, request.url);
        }
      },
    ],

    async requestHandler({ request, $, response, enqueueLinks, log: reqLog }) {
      const { url, userData } = request;
      const responseBody = typeof response?.body === 'string'
        ? response.body
        : Buffer.isBuffer(response?.body)
          ? response.body.toString('utf8')
          : '';

      // ✅ ANTI-SCRAPING: Realistic delay between requests
      await delayRequest(delayBetweenRequests, delayBetweenRequests + 2000);

      reqLog.info(`[${userData.label}] Processing: ${url}, Status: ${response.statusCode}`);

      // LISTING PAGE
      if (userData.label === 'LIST') {
        if (itemCount >= maxItems) {
          reqLog.info(`Max items (${maxItems}) reached. Stopping crawl.`);
          return;
        }

        // ✅ Strategy 1: Next.js payload extraction
        const nextDataJobs = extractJobsFromNextData(responseBody, url);

        if (nextDataJobs.length > 0) {
          reqLog.info(`Page ${userData.page}: Found ${nextDataJobs.length} jobs via Next.js payload`);

          for (const job of nextDataJobs) {
            if (itemCount >= maxItems) break;

            const jobData = {
              title: job.title || '',
              company: job.company || '',
              location: job.location || '',
              jobType: job.jobType || 'Not specified',
              salary: job.salary || 'Not specified',
              description: job.description || '',
              datePosted: job.datePosted || '',
              url: job.url || url,
              source: 'talent.com',
              scrapedAt: new Date().toISOString(),
            };

            await Actor.pushData(jobData);
            itemCount++;
            reqLog.info(`✅ Saved job: ${jobData.title} at ${jobData.company} (${itemCount}/${maxItems})`);
          }
        } else {
          // ✅ Strategy 2: JSON-LD extraction (fallback)
          const jsonLdJobs = extractJobPostingsFromJsonLd($);

          if (jsonLdJobs.length > 0) {
            reqLog.info(`Page ${userData.page}: Found ${jsonLdJobs.length} jobs via JSON-LD`);

            for (const job of jsonLdJobs) {
              if (itemCount >= maxItems) break;

              const jobUrl = resolveUrl(job.url, url) || url;

              const jobData = {
                title: job.title || '',
                company: job.hiringOrganization?.name || '',
                location: extractLocationFromJsonLd(job.jobLocation),
                jobType: job.employmentType || 'Not specified',
                salary: extractSalaryFromJsonLd(job.baseSalary) || 'Not specified',
                description: (job.description || '').substring(0, 500),
                datePosted: job.datePosted || '',
                url: jobUrl,
                source: 'talent.com',
                scrapedAt: new Date().toISOString(),
              };

              await Actor.pushData(jobData);
              itemCount++;
              reqLog.info(`✅ Saved job: ${jobData.title} at ${jobData.company} (${itemCount}/${maxItems})`);
            }
          } else {
            // ✅ Strategy 3: DOM extraction (last resort)
            reqLog.info(`Page ${userData.page}: Trying DOM extraction...`);
            const domJobs = extractJobsFromDOM($, url);

            if (domJobs.length > 0) {
              reqLog.info(`Page ${userData.page}: Found ${domJobs.length} jobs via DOM`);

              for (const job of domJobs) {
                if (itemCount >= maxItems) break;

                const jobData = {
                  ...job,
                  scrapedAt: new Date().toISOString(),
                };

                await Actor.pushData(jobData);
                itemCount++;
                reqLog.info(`✅ Saved job: ${job.title} at ${job.company} (${itemCount}/${maxItems})`);

                if (includeJobDetails && job.url) {
                  await enqueueLinks({
                    urls: [job.url],
                    userData: {
                      label: 'DETAIL',
                      jobId: job.url.split('id=')[1] || Math.random().toString(36).substring(7),
                    },
                  });
                }
              }
            } else {
              reqLog.warning(`Page ${userData.page}: No jobs found. Check selectors or site structure.`);
            }
          }
        }

        // Check if there's a next page
        const nextPageNumber = (userData.page || 1) + 1;
        const explicitNextSelector = `a[href*="p=${nextPageNumber}"]`;
        let nextPageLink = $('a[aria-label*="Next"]').attr('href') ||
                           $('a:contains("Next")').attr('href') ||
                           $('a.next').attr('href') ||
                           $(explicitNextSelector).first().attr('href');

        if (nextPageLink && itemCount < maxItems) {
          const nextUrl = resolveUrl(nextPageLink, url);
          if (nextUrl) {
            await enqueueLinks({
              urls: [nextUrl],
              userData: {
                label: 'LIST',
                page: (userData.page || 1) + 1,
              },
            });
          }
        }
      }

      // DETAIL PAGE (optional)
      if (userData.label === 'DETAIL') {
        const jobDetail = extractJobDetail($, url, userData.jobId);
        await Actor.pushData({
          ...jobDetail,
          scrapedAt: new Date().toISOString(),
        });
        reqLog.info(`✅ Saved job detail: ${jobDetail.title}`);
      }
    },

    async failedRequestHandler({ request, error }, context) {
      context.log.error(`Request ${request.url} failed multiple times:`, error.message);
    },
  });

  // Run the crawler
  crawlerLog.info('🚀 Starting crawler...');
  await crawler.run(startUrls);

  crawlerLog.info(`✅ Crawl completed! Total jobs scraped: ${itemCount}`);
} catch (error) {
  log.error(`❌ An error occurred: ${error.message}`);
  if (error.stack) log.debug(error.stack);
  throw error;
} finally {
  await Actor.exit();
}

