import { Actor, log } from 'apify';
import { CheerioCrawler } from 'crawlee';
import { load as cheerioLoad } from 'cheerio';

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
 * Extract job listings from Next.js streamed payload.
 * Returns partial listing data (if available) and detail URLs discovered in the stream.
 */
function extractJobsFromNextData(html, baseUrl) {
  const result = {
    jobs: [],
    detailUrls: [],
  };

  if (!html || typeof html !== 'string') return result;

  const chunkRegex = /self\.__next_f\.push\((\[.*?\])\)/gs;
  const seenDetailUrls = new Set();
  const seenJobUrls = new Set();

  for (const match of html.matchAll(chunkRegex)) {
    const payload = match[1];
    let items;
    try {
      items = JSON.parse(payload);
    } catch (_err) {
      continue;
    }

    if (!Array.isArray(items)) continue;

    for (const entry of items) {
      if (typeof entry !== 'string') continue;

      const trimmed = entry.trim();
      if (!trimmed) continue;

      // Attempt to parse JSON fragments contained in the stream
      if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
        let parsed;
        try {
          parsed = JSON.parse(trimmed);
        } catch (_err) {
          continue;
        }

        // ItemList contains links to job detail pages
        if (parsed?.['@type'] === 'ItemList' && Array.isArray(parsed.itemListElement)) {
          for (const element of parsed.itemListElement) {
            const urlCandidate = resolveUrl(element?.item?.url, baseUrl);
            if (urlCandidate && !seenDetailUrls.has(urlCandidate)) {
              seenDetailUrls.add(urlCandidate);
              result.detailUrls.push(urlCandidate);
            }
          }
        }

        // JobPosting objects occasionally appear in the stream
        if (parsed?.['@type'] === 'JobPosting') {
          const urlCandidate = resolveUrl(parsed.url, baseUrl);
          if (urlCandidate && !seenJobUrls.has(urlCandidate)) {
            seenJobUrls.add(urlCandidate);
            result.jobs.push({
              title: parsed.title || '',
              company: parsed.hiringOrganization?.name || '',
              location: extractLocationFromJsonLd(parsed.jobLocation),
              jobType: parsed.employmentType || '',
              salary: extractSalaryFromJsonLd(parsed.baseSalary) || '',
              description: (parsed.description || '').substring(0, 500),
              datePosted: parsed.datePosted || '',
              url: urlCandidate,
              source: 'talent.com',
            });
          }
        }
      }
    }
  }

  return result;
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

    const headerTitleEl = $container.children('h2, h3').first();
    if (headerTitleEl.length === 0) return;

    const title = headerTitleEl.text().replace(/\s+/g, ' ').trim();

    const linkEl = $container.find('a[href*="/view?id="]').first();
    const href = linkEl.attr('href') || headerTitleEl.find('a[href*="/view?id="]').attr('href');
    const absoluteUrl = resolveUrl(href, baseUrl);
    if (!absoluteUrl) return;
    if (seenUrls.has(absoluteUrl)) return;
    seenUrls.add(absoluteUrl);

    const sanitize = (text) => (text || '').replace(/\s+/g, ' ').trim();

    const companyEl = $container.children('span').eq(0);
    const locationEl = $container.children('span').eq(1);
    const company = sanitize(companyEl.text());
    const location = sanitize(locationEl.text());

    const metaContainer = $container.find('div').first();
    let jobType = '';
    let snippet = '';
    let postedDate = '';

    if (metaContainer && metaContainer.length > 0) {
      const jobTypeDiv = metaContainer.find('div').first();
      if (jobTypeDiv && jobTypeDiv.length > 0) {
        jobType = sanitize(jobTypeDiv.text());
      }

      const snippetSpan = metaContainer.find('span').first();
      if (snippetSpan && snippetSpan.length > 0) {
        snippet = sanitize(snippetSpan.text().replace(/Show more/gi, ''));
      }

      const postedSpan = metaContainer.find('span').filter((_idx, el) => /last updated/i.test($(el).text())).first();
      if (postedSpan.length > 0) {
        postedDate = sanitize(postedSpan.text());
      } else {
        const relativeDate = metaContainer.find('span').filter((_idx, el) => /\bday\b|\bweek\b|\bmonth\b|ago/i.test($(el).text())).first();
        if (relativeDate.length > 0) {
          postedDate = sanitize(relativeDate.text());
        }
      }
    }

    const salaryMatch = title.match(/\$[\d,]+(?:\s*[-–]\s*\$[\d,]+)?/);
    const salary = salaryMatch ? salaryMatch[0] : null;

    jobs.push({
      title,
      company,
      location,
      jobType: jobType || '',
      salary: salary || '',
      snippet: snippet.substring(0, 500),
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
  // Try dedicated JSON-LD script first (most reliable on talent.com detail pages)
  let jobFromJson = null;
  const ldJsonText = $('#job-data-ld+json').html();
  if (ldJsonText) {
    try {
      const parsed = JSON.parse(ldJsonText);
      if (parsed?.['@type'] === 'JobPosting') {
        jobFromJson = parsed;
      }
    } catch (_error) {
      // ignore malformed JSON, fall through to other strategies
    }
  }

  if (!jobFromJson) {
    const jsonLdJobs = extractJobPostingsFromJsonLd($);
    if (jsonLdJobs.length > 0) {
      jobFromJson = jsonLdJobs.find((job) => {
        const resolved = resolveUrl(job.url, baseUrl);
        return resolved && resolved.split('#')[0] === baseUrl.split('#')[0];
      }) || jsonLdJobs[0];
    }
  }

  if (jobFromJson) {
    const descriptionHtml = jobFromJson.description || '';
    const descriptionText = descriptionHtml ? cheerioLoad(descriptionHtml).text() : '';

    return {
      jobId,
      title: jobFromJson.title || '',
      company: jobFromJson.hiringOrganization?.name || '',
      location: extractLocationFromJsonLd(jobFromJson.jobLocation),
      jobType: jobFromJson.employmentType || '',
      salary: extractSalaryFromJsonLd(jobFromJson.baseSalary),
      description: descriptionText.substring(0, 5000),
      descriptionHtml,
      datePosted: jobFromJson.datePosted || '',
      url: baseUrl,
    };
  }

  // Fallback to DOM extraction
  const sanitize = (text) => (text || '').replace(/\s+/g, ' ').trim();

  const headerBlock = $('[class*="sc-668ba90a-3"]').first();
  let title = '';
  let company = '';
  let location = '';

  if (headerBlock.length > 0) {
    const headerSpans = headerBlock.find('span');
    if (headerSpans.length > 0) {
      title = sanitize(headerSpans.eq(0).text());
    }
    if (headerSpans.length > 1) {
      const metaTextRaw = headerSpans.eq(1).text();
      const normalizedMeta = sanitize(metaTextRaw.replace(/\uFFFD/g, '•'));
      const metaParts = normalizedMeta.split(/[•|·]/).map(part => part.trim()).filter(Boolean);
      if (metaParts.length > 0) {
        company = metaParts[0];
        if (metaParts.length > 1) {
          location = metaParts[metaParts.length - 1];
        }
      }
    }
  }

  if (!title) {
    const titleCandidates = $('h1, h2')
      .map((_idx, el) => sanitize($(el).text()))
      .get()
      .filter(value => value && value.length > 3 && !/show more/i.test(value));
    title = titleCandidates[0] || '';
  }

  const jobTypeContainer = $('[class*="sc-fd8dae98-0"]').first();
  let jobType = '';
  if (jobTypeContainer.length > 0) {
    jobType = sanitize(jobTypeContainer.text());
  }

  let datePosted = '';
  const postedSpan = $('span').filter((_idx, el) => /last updated/i.test($(el).text())).first();
  if (postedSpan.length > 0) {
    datePosted = sanitize(postedSpan.text());
  } else {
    const relativeSpan = $('span').filter((_idx, el) => /\bday\b|\bweek\b|\bmonth\b|ago/i.test($(el).text())).first();
    if (relativeSpan.length > 0) {
      datePosted = sanitize(relativeSpan.text());
    }
  }

  let descriptionHtml = '';
  const descriptionLabel = $('span').filter((_idx, el) => $(el).text().trim().toLowerCase() === 'job description').first();
  if (descriptionLabel.length > 0) {
    const container = descriptionLabel.parent().find('div').last();
    if (container.length > 0) {
      descriptionHtml = container.html() || '';
    }
  }

  if (!descriptionHtml) {
    const fallbackSelectors = [
      '[class*="job-description"]',
      '[data-testid="job-description"]',
      '[class*="description"]',
      '.job-content',
      'main',
    ];

    for (const selector of fallbackSelectors) {
      const el = $(selector).first();
      if (el.length > 0) {
        descriptionHtml = el.html() || '';
        break;
      }
    }
  }

  const descriptionText = descriptionHtml ? cheerioLoad(descriptionHtml).text() : '';

  return {
    jobId,
    title,
    company,
    location,
    jobType,
    salary: '',
    description: descriptionText.substring(0, 5000),
    descriptionHtml,
    datePosted,
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
    includeJobDetails = true,
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
  let scheduledCount = 0;
  const enqueuedDetailUrls = new Set();

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

        if (request.userData?.fromListUrl && !request.headers.Referer) {
          request.headers.Referer = request.userData.fromListUrl;
        }

        if (cookies) {
          request.headers.Cookie = cookies;
        }

        if (parsedCookies.length > 0 && session) {
          session.setCookies(parsedCookies, request.url);
        }
      },
    ],


    
    async requestHandler({ request, $, response, body, enqueueLinks, log: reqLog }) {
      const { url, userData } = request;
      const responseBody = typeof body === 'string'
        ? body
        : Buffer.isBuffer(body)
          ? body.toString('utf8')
          : typeof response?.body === 'string'
            ? response.body
            : Buffer.isBuffer(response?.body)
              ? response.body.toString('utf8')
              : '';
    
      const normalize = (text) => (text || '').replace(/\s+/g, ' ').trim();
      const getProgressCount = () => (includeJobDetails ? scheduledCount : itemCount);
      const canScheduleMore = () => getProgressCount() < maxItems;
    
      const pushSummaryRecord = async (summary) => {
        if (!includeJobDetails && itemCount >= maxItems) return;
        const record = {
          title: summary.title || '',
          company: summary.company || '',
          location: summary.location || '',
          jobType: summary.jobType || '',
          salary: summary.salary || '',
          snippet: summary.snippet || '',
          postedDate: summary.postedDate || '',
          datePosted: summary.postedDate || '',
          description: summary.snippet || '',
          descriptionHtml: '',
          url: summary.url || url,
          source: summary.source || 'talent.com',
          listUrl: summary.listUrl || url,
          scrapedAt: new Date().toISOString(),
        };
        await Actor.pushData(record);
        itemCount++;
        reqLog.info(`Saved job (summary): ${record.title || 'Untitled'} (${itemCount}/${maxItems})`);
      };
    
      const scheduleJobSummary = async (jobSummary) => {
        let detailUrl = jobSummary.url ? resolveUrl(jobSummary.url, url) : '';
        if (detailUrl === url) detailUrl = '';
        const summary = {
          jobId: jobSummary.jobId || (detailUrl ? detailUrl.split('id=')[1] : '') || '',
          title: normalize(jobSummary.title),
          company: normalize(jobSummary.company),
          location: normalize(jobSummary.location),
          jobType: normalize(jobSummary.jobType),
          salary: jobSummary.salary || '',
          snippet: jobSummary.snippet || '',
          postedDate: normalize(jobSummary.postedDate),
          url: detailUrl || '',
          source: jobSummary.source || 'talent.com',
          listUrl: jobSummary.listUrl || url,
        };
    
        if (!includeJobDetails) {
          await pushSummaryRecord(summary);
          return;
        }

        if (!summary.url) {
          reqLog.debug(`Skipping job without detail URL on ${url}: ${summary.title || 'Untitled'}`);
          return;
        }
    
        if (enqueuedDetailUrls.has(summary.url)) return;
        if (!canScheduleMore()) return;
    
        enqueuedDetailUrls.add(summary.url);
        scheduledCount++;
        await enqueueLinks({
          urls: [summary.url],
          userData: {
            label: 'DETAIL',
            jobId: summary.jobId || Math.random().toString(36).substring(7),
            jobSummary: summary,
            fromListUrl: url,
          },
        });
        reqLog.debug(`Enqueued detail URL: ${summary.url}`);
      };
    
      await delayRequest(delayBetweenRequests, delayBetweenRequests + 2000);
    
      reqLog.info(`[${userData.label}] Processing: ${url}, Status: ${response.statusCode}`);
    
      if (userData.label === 'LIST') {
        if (!canScheduleMore()) {
          reqLog.info(`Max items (${maxItems}) already scheduled. Skipping page.`);
          return;
        }
    
        const nextData = extractJobsFromNextData(responseBody, url);
        const nextDataJobs = Array.isArray(nextData.jobs) ? nextData.jobs : [];
        const payloadDetailUrls = new Set(
          (Array.isArray(nextData.detailUrls) ? nextData.detailUrls : [])
            .map((detailUrl) => resolveUrl(detailUrl, url))
            .filter(Boolean),
        );
    
        if (nextDataJobs.length > 0) {
          reqLog.info(`Page ${userData.page}: Found ${nextDataJobs.length} jobs via Next.js payload`);
        }
        for (const job of nextDataJobs) {
          if (!canScheduleMore()) break;
          const jobSummary = {
            title: job.title || '',
            company: job.company || '',
            location: job.location || '',
            jobType: job.jobType || '',
            salary: job.salary || '',
            snippet: (job.description || '').substring(0, 500),
            postedDate: job.datePosted || '',
            url: job.url || '',
            source: 'talent.com',
            listUrl: url,
          };
          await scheduleJobSummary(jobSummary);
          const resolvedUrl = resolveUrl(job.url, url);
          if (resolvedUrl) payloadDetailUrls.delete(resolvedUrl);
        }
    
        const jsonLdJobs = extractJobPostingsFromJsonLd($);
        if (jsonLdJobs.length > 0) {
          reqLog.info(`Page ${userData.page}: Found ${jsonLdJobs.length} jobs via JSON-LD`);
        }
        for (const job of jsonLdJobs) {
          if (!canScheduleMore()) break;
          const jobUrl = resolveUrl(job.url, url);
          const snippetText = job.description ? cheerioLoad(job.description).text().substring(0, 500) : '';
          const jobSummary = {
            title: job.title || '',
            company: job.hiringOrganization?.name || '',
            location: extractLocationFromJsonLd(job.jobLocation),
            jobType: job.employmentType || '',
            salary: extractSalaryFromJsonLd(job.baseSalary) || '',
            snippet: snippetText,
            postedDate: job.datePosted || '',
            url: jobUrl || '',
            source: 'talent.com',
            listUrl: url,
          };
          await scheduleJobSummary(jobSummary);
          if (jobUrl) payloadDetailUrls.delete(jobUrl);
        }
    
        const domJobs = extractJobsFromDOM($, url);
        if (domJobs.length > 0) {
          reqLog.info(`Page ${userData.page}: Found ${domJobs.length} jobs via DOM`);
        } else if (!nextDataJobs.length && !jsonLdJobs.length) {
          reqLog.warning(`Page ${userData.page}: No jobs found. Check selectors or site structure.`);
        }
    
        for (const job of domJobs) {
          if (!canScheduleMore()) break;
          const jobSummary = {
            title: job.title || '',
            company: job.company || '',
            location: job.location || '',
            jobType: job.jobType || '',
            salary: job.salary || '',
            snippet: job.snippet || '',
            postedDate: job.postedDate || '',
            url: job.url || '',
            source: 'talent.com',
            listUrl: url,
          };
          await scheduleJobSummary(jobSummary);
          if (job.url) payloadDetailUrls.delete(job.url);
        }
    
        if (includeJobDetails && payloadDetailUrls.size > 0) {
          reqLog.debug(`Page ${userData.page}: Scheduling ${payloadDetailUrls.size} remaining detail URLs from payload`);
          for (const detailUrl of payloadDetailUrls) {
            if (!canScheduleMore()) break;
            await scheduleJobSummary({
              title: '',
              company: '',
              location: '',
              jobType: '',
              salary: '',
              snippet: '',
              postedDate: '',
              url: detailUrl,
              source: 'talent.com',
              listUrl: url,
            });
          }
        }
    
        const nextPageNumber = (userData.page || 1) + 1;
        const explicitNextSelector = `a[href*="p=${nextPageNumber}"]`;
        let nextPageLink = $('a[aria-label*="Next"]').attr('href') ||
                               $('a:contains("Next")').attr('href') ||
                               $('a.next').attr('href') ||
                               $(explicitNextSelector).first().attr('href');
    
        if (nextPageLink && canScheduleMore()) {
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
    
      if (userData.label === 'DETAIL') {
        const summary = userData.jobSummary || {};
    
        if (!includeJobDetails && !summary.url) {
          reqLog.debug(`Detail fetched with detail collection disabled; skipping ${url}`);
          return;
        }
    
        if (includeJobDetails && itemCount >= maxItems) {
          reqLog.info(`Max items (${maxItems}) reached before detail processing; skipping ${url}`);
          return;
        }
    
        const detail = extractJobDetail($, url, userData.jobId);
        const finalRecord = {
          jobId: detail.jobId || summary.jobId || userData.jobId || '',
          title: detail.title || summary.title || '',
          company: detail.company || summary.company || '',
          location: detail.location || summary.location || '',
          jobType: detail.jobType || summary.jobType || '',
          salary: detail.salary || summary.salary || '',
          postedDate: detail.datePosted || summary.postedDate || '',
          datePosted: detail.datePosted || summary.postedDate || '',
          snippet: summary.snippet || '',
          description: detail.description || summary.snippet || '',
          descriptionHtml: detail.descriptionHtml || '',
          url: detail.url || summary.url || url,
          source: summary.source || 'talent.com',
          listUrl: summary.listUrl || userData.fromListUrl || '',
          scrapedAt: new Date().toISOString(),
        };
    
        await Actor.pushData(finalRecord);
        itemCount++;
        reqLog.info(`Saved job detail: ${finalRecord.title || 'Untitled'} (${itemCount}/${maxItems})`);
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

