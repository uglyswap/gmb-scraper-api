#!/usr/bin/env python3
"""
GMB Scraper PRODUCTION v5.1 - Filter invalid names like "Résultats"

Changes from v5.0:
- Filter "Résultats", "Results", "Recherche", "Search" as invalid names
- These appear when stuck on search results page instead of business page
"""

import asyncio
import re
import json
import sys
import os
import logging
from datetime import datetime
from typing import Dict, List, Set, Optional
from urllib.parse import quote, urljoin, urlparse
from playwright.async_api import async_playwright, Response, Page
import aiohttp

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HEADLESS = os.environ.get('HEADLESS', 'true').lower() == 'true'
DEBUG_MODE = os.environ.get('DEBUG', 'false').lower() == 'true'

# CONFIG
PHASE1_WORKERS = 10
PHASE2_WORKERS = 20
DEFAULT_GRID_SIZE = 10
BASE_COVERAGE = 0.09
SCROLL_COUNT = 8
SCROLL_DELAY = 0.5
PAGE_DELAY = 3.0
INITIAL_PAGE_DELAY = 4.0

RETRY_ATTEMPTS = 3
EMAIL_TIMEOUT = 8
EMAIL_CONCURRENT = 30

# v5.1: Invalid business names (search results page, consent, etc.)
INVALID_NAMES = [
    'résultats', 'resultats', 'results',
    'recherche', 'search',
    'consent', 'google maps', 'google',
    'before you continue', "avant d'accéder",
    'maps', 'plan', 'itinéraire'
]

CONTACT_PAGES = [
    '', '/contact', '/contacts', '/contactez-nous', '/nous-contacter',
    '/about', '/a-propos', '/qui-sommes-nous', '/about-us',
    '/mentions-legales', '/legal', '/cgv', '/cgu'
]

CITY_COORDS = {
    'paris': (48.8566, 2.3522),
    'lyon': (45.7640, 4.8357),
    'marseille': (43.2965, 5.3698),
    'toulouse': (43.6047, 1.4442),
    'nice': (43.7102, 7.2620),
    'nantes': (47.2184, -1.5536),
    'strasbourg': (48.5734, 7.7521),
    'montpellier': (43.6108, 3.8767),
    'bordeaux': (44.8378, -0.5792),
    'lille': (50.6292, 3.0573),
    'rennes': (48.1173, -1.6778),
    'reims': (49.2583, 4.0317),
    'toulon': (43.1242, 5.9280),
    'grenoble': (45.1885, 5.7245),
    'dijon': (47.3220, 5.0415),
    'angers': (47.4784, -0.5632),
    'nimes': (43.8367, 4.3601),
}

INVALID_EMAIL_PATTERNS = [
    'example.', 'exemple.', '@sample.', '@demo.', '@fake.', '@test.',
    'your-email', 'votre-email', 'placeholder', 'changeme',
    '@google.', '@facebook.', '@twitter.', '@instagram.',
    'gstatic.com', 'googleapis.com',
    '@sentry.', '@wix.', '@wordpress.', '@squarespace.',
    'wixpress.com', 'squarespace.com',
    'noreply@', 'no-reply@', 'donotreply@',
    'mailer-daemon@', 'postmaster@', 'webmaster@',
    '.local', '.localhost', '.internal', '.invalid',
]

INVALID_DOMAINS = [
    'example.com', 'exemple.com', 'test.com', 'demo.com',
    'domain.com', 'email.com', 'website.com',
    'sentry.io', 'wix.com', 'squarespace.com',
]


def emit(event_type: str, data: dict):
    event = {"type": event_type, "timestamp": datetime.now().isoformat(), **data}
    print(json.dumps(event, ensure_ascii=False), flush=True)


def is_valid_business_name(name: str) -> bool:
    """v5.1: Check if name is a valid business name"""
    if not name or len(name) < 3:
        return False
    name_lower = name.lower().strip()
    for invalid in INVALID_NAMES:
        if name_lower == invalid or name_lower.startswith(invalid + ' '):
            return False
    return True


def is_valid_email(email: str) -> bool:
    if not email or '@' not in email:
        return False
    email_lower = email.lower().strip()
    if len(email_lower) > 254:
        return False
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email_lower):
        return False
    for pattern in INVALID_EMAIL_PATTERNS:
        if pattern in email_lower:
            return False
    try:
        domain = email_lower.split('@')[1]
        if domain in INVALID_DOMAINS:
            return False
        if len(domain) < 4:
            return False
    except:
        return False
    return True


def extract_emails_from_html(html: str) -> List[str]:
    emails = set()
    standard_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', html)
    for email in standard_emails:
        if is_valid_email(email):
            emails.add(email.lower())
    mailto_matches = re.findall(r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', html, re.IGNORECASE)
    for email in mailto_matches:
        if is_valid_email(email):
            emails.add(email.lower())
    return list(emails)


class DataStore:
    def __init__(self):
        self.place_ids: Set[str] = set()
        self.businesses: Dict[str, Dict] = {}
        self._lock = asyncio.Lock()

    async def add_ids(self, ids: List[str]) -> int:
        async with self._lock:
            before = len(self.place_ids)
            self.place_ids.update(ids)
            return len(self.place_ids) - before

    async def add_business(self, pid: str, data: Dict) -> bool:
        async with self._lock:
            if pid not in self.businesses:
                self.businesses[pid] = data
                return True
            return False

    def get_all_ids(self) -> List[str]:
        return list(self.place_ids)


class GMBScraperProduction:
    def __init__(self, activity: str, city: str, grid_size: int = None):
        self.activity = activity
        self.city = city.lower()
        self.grid_size = min(grid_size or DEFAULT_GRID_SIZE, 55)
        self.data = DataStore()
        self.start_time = datetime.now()
        self.zones_done = 0
        self.total_zones = self.grid_size * self.grid_size
        self._zone_lock = asyncio.Lock()
        self.extracted_count = 0
        self.failed_count = 0
        self._extract_lock = asyncio.Lock()
        self.cell_size = BASE_COVERAGE / self.grid_size

        if self.city in CITY_COORDS:
            self.lat, self.lng = CITY_COORDS[self.city]
        else:
            self.lat, self.lng = CITY_COORDS['paris']

    def extract_place_ids(self, body: str) -> List[str]:
        ids = set()
        ids.update(re.findall(r'ChIJ[A-Za-z0-9_-]{23}', body))
        ids.update(re.findall(r'0x[a-f0-9]{14,16}:0x[a-f0-9]{14,16}', body, re.IGNORECASE))
        ids.update(re.findall(r'\\"(ChIJ[A-Za-z0-9_-]{23})\\"', body))
        ids.update(re.findall(r'!1s(ChIJ[A-Za-z0-9_-]{23})(?:[!&"]|$)', body))
        ids.update(re.findall(r'(?:place_id|ftid)[=:](ChIJ[A-Za-z0-9_-]{23})', body, re.IGNORECASE))

        valid_ids = []
        for pid in ids:
            if pid.startswith('ChIJ') and len(pid) == 27:
                valid_ids.append(pid)
            elif pid.startswith('0x') and ':0x' in pid and 35 <= len(pid) <= 38:
                valid_ids.append(pid)
        return list(set(valid_ids))

    async def create_response_handler(self):
        async def handler(response: Response):
            try:
                url = response.url
                if 'google' not in url:
                    return
                if '/search' in url or '/maps/rpc' in url or '/maps/preview/place' in url or '/place/' in url:
                    body = await response.text()
                    if len(body) > 500:
                        ids = self.extract_place_ids(body)
                        if ids:
                            new = await self.data.add_ids(ids)
                            if new > 0:
                                logger.info(f"[NEW IDS] +{new} IDs (total: {len(self.data.place_ids)})")
                                emit("zone_links", {"unique_new": new, "total_ids": len(self.data.place_ids)})
            except Exception as e:
                pass
        return handler

    async def accept_cookies(self, page: Page) -> bool:
        """v5.0: Use aria-label selector - most stable!"""
        try:
            current_url = page.url

            if 'consent' not in current_url.lower():
                return True

            logger.info(f"[CONSENT] Page detected")
            await asyncio.sleep(1.0)

            consent_selectors = [
                '[aria-label="Tout accepter"]',
                '[aria-label="Accept all"]',
                '[aria-label="Alle akzeptieren"]',
                '[aria-label="Alles accepteren"]',
                'button:has-text("Tout accepter")',
                'button:has-text("Accept all")',
            ]

            for selector in consent_selectors:
                try:
                    btn = await page.query_selector(selector)
                    if btn and await btn.is_visible():
                        logger.info(f"[CONSENT] Clicking: {selector}")
                        await btn.click()
                        await asyncio.sleep(1.5)

                        if 'consent' not in page.url.lower():
                            logger.info("[CONSENT OK]")
                            return True
                except:
                    continue

            logger.warning("[CONSENT] No selector matched")
            return False

        except Exception as e:
            logger.error(f"[CONSENT ERROR] {str(e)}")
            return False

    async def phase1_worker(self, browser, worker_id: int, zones: List[tuple]):
        logger.info(f"[WORKER {worker_id}] Starting with {len(zones)} zones")

        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="fr-FR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        page = await context.new_page()
        handler = await self.create_response_handler()
        page.on("response", handler)

        initial_url = f"https://www.google.com/maps/search/{quote(f'{self.activity} {self.city}')}"
        try:
            logger.info(f"[WORKER {worker_id}] Loading initial page...")
            await page.goto(initial_url, wait_until="networkidle", timeout=45000)
            await asyncio.sleep(INITIAL_PAGE_DELAY)

            await self.accept_cookies(page)

            if 'consent' in page.url.lower():
                logger.info(f"[WORKER {worker_id}] Still on consent, re-navigating...")
                await page.goto(initial_url, wait_until="networkidle", timeout=45000)
                await asyncio.sleep(INITIAL_PAGE_DELAY)

            logger.info(f"[WORKER {worker_id}] Ready. URL: {page.url[:80]}")
        except Exception as e:
            logger.error(f"[WORKER {worker_id}] Initial page error: {str(e)[:100]}")

        for lat, lng, zone_id in zones:
            try:
                emit("zone_start", {"zone": zone_id + 1, "total_zones": self.total_zones, "lat": round(lat, 4), "lng": round(lng, 4)})

                zoom = 15 if self.grid_size <= 10 else (16 if self.grid_size <= 25 else 17)
                url = f"https://www.google.com/maps/search/{quote(f'{self.activity} {self.city}')}/@{lat},{lng},{zoom}z"

                await page.goto(url, wait_until="networkidle", timeout=30000)
                await asyncio.sleep(PAGE_DELAY)

                if 'consent' in page.url.lower():
                    await self.accept_cookies(page)
                    await page.goto(url, wait_until="networkidle", timeout=30000)
                    await asyncio.sleep(PAGE_DELAY)

                for scroll_i in range(SCROLL_COUNT):
                    await page.evaluate('const f=document.querySelector(\'div[role="feed"]\');if(f)f.scrollTo(0,f.scrollHeight);')
                    await asyncio.sleep(SCROLL_DELAY)

                await asyncio.sleep(1.0)

                async with self._zone_lock:
                    self.zones_done += 1
                    phase1_percent = int((self.zones_done / self.total_zones) * 40)
                    emit("zone_complete", {
                        "zone": zone_id + 1,
                        "total_zones": self.total_zones,
                        "total_businesses": len(self.data.place_ids),
                        "percent": phase1_percent,
                        "global_percent": phase1_percent
                    })
            except Exception as e:
                logger.warning(f"[ZONE {zone_id + 1}] Error: {str(e)[:80]}")
                async with self._zone_lock:
                    self.zones_done += 1

        await context.close()

    async def extract_single_pid(self, context, pid: str, total_pids: int, attempt: int = 1) -> bool:
        page = await context.new_page()
        success = False

        try:
            url = f"https://www.google.com/maps/search/?api=1&query=Google&query_place_id={pid}"

            if attempt > 1:
                url = f"https://www.google.com/maps/place/?q=place_id:{pid}"

            await page.goto(url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2.5)

            if "consent" in page.url.lower():
                logger.info(f"[EXTRACT] Consent for {pid[:20]}")
                await self.accept_cookies(page)
                await asyncio.sleep(1.5)
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await asyncio.sleep(2.5)

            data = await page.evaluate('''() => {
                const d = {};

                const nameSelectors = [
                    'h1.DUwDvf', 'h1.fontHeadlineLarge', 'h1[data-attrid="title"]',
                    'div[role="main"] h1', 'span.fontHeadlineLarge', 'h1.LkPWWd', 'h1'
                ];
                for (const sel of nameSelectors) {
                    const el = document.querySelector(sel);
                    if (el && el.textContent) {
                        const name = el.textContent.trim();
                        if (name.length > 2) {
                            d.name = name;
                            break;
                        }
                    }
                }

                const phoneSelectors = [
                    'button[data-item-id*="phone"]', 'a[href^="tel:"]',
                    'button[aria-label*="téléphone"]', 'button[aria-label*="phone"]'
                ];
                for (const sel of phoneSelectors) {
                    const phoneBtn = document.querySelector(sel);
                    if (phoneBtn) {
                        const label = phoneBtn.getAttribute('aria-label') || phoneBtn.href || phoneBtn.textContent || '';
                        const match = label.match(/[0-9+][0-9\\s\\.\\-]{8,}/);
                        if (match) {
                            d.phone = match[0].replace(/[\\s\\.\\-]/g, '');
                            break;
                        }
                    }
                }

                const webSelectors = [
                    'a[data-item-id="authority"]', 'a[data-tooltip="Ouvrir le site Web"]', 'a[aria-label*="site"]'
                ];
                for (const sel of webSelectors) {
                    const webLink = document.querySelector(sel);
                    if (webLink && webLink.href && !webLink.href.includes('google.com')) {
                        d.website = webLink.href;
                        break;
                    }
                }

                const addrBtn = document.querySelector('button[data-item-id="address"], button[aria-label*="Adresse"]');
                if (addrBtn) {
                    const label = addrBtn.getAttribute('aria-label') || addrBtn.textContent || '';
                    d.address = label.replace(/^Adresse\\s*:\\s*/i, '').trim();
                }

                const ratingEl = document.querySelector('div.F7nice span[aria-hidden="true"], span.ceNzKf');
                if (ratingEl) {
                    const rating = parseFloat(ratingEl.textContent?.replace(',', '.'));
                    if (!isNaN(rating) && rating > 0 && rating <= 5) d.rating = rating;
                }

                const catBtn = document.querySelector('button[jsaction*="category"], span.DkEaL');
                if (catBtn) d.category = catBtn.textContent?.trim() || '';

                d._debug_h1 = document.querySelector('h1')?.textContent?.substring(0, 80) || 'NO H1';

                return d;
            }''')

            name = data.get('name', '')
            
            # v5.1: Validate business name
            if name and is_valid_business_name(name):
                data.pop('_debug_h1', None)
                data['place_id'] = pid
                data['google_maps_url'] = f"https://www.google.com/maps/place/?q=place_id:{pid}"

                is_new = await self.data.add_business(pid, data)
                if is_new:
                    async with self._extract_lock:
                        self.extracted_count += 1
                        if self.extracted_count % 10 == 0:
                            logger.info(f"[EXTRACT SUCCESS] {self.extracted_count} businesses")
                        current = self.extracted_count + self.failed_count
                        phase2_progress = int((current / total_pids) * 40)
                        if current % 5 == 0:
                            emit("extraction_progress", {
                                "extracted": self.extracted_count,
                                "failed": self.failed_count,
                                "total": total_pids,
                                "global_percent": min(40 + phase2_progress, 80)
                            })
                    emit("business", data)
                success = True
            elif attempt < RETRY_ATTEMPTS:
                logger.warning(f"[EXTRACT FAIL] {pid[:20]} - invalid name: '{name}' h1: {data.get('_debug_h1', 'N/A')[:30]}")
                await page.close()
                await asyncio.sleep(1.0)
                return await self.extract_single_pid(context, pid, total_pids, attempt + 1)
            else:
                logger.warning(f"[EXTRACT FAILED] {pid[:20]} after {attempt} attempts - name: '{name}'")
                async with self._extract_lock:
                    self.failed_count += 1

        except Exception as e:
            logger.error(f"[EXTRACT ERROR] {pid[:20]}: {str(e)[:50]}")
            if attempt < RETRY_ATTEMPTS:
                try:
                    await page.close()
                except:
                    pass
                await asyncio.sleep(1.0)
                return await self.extract_single_pid(context, pid, total_pids, attempt + 1)
            async with self._extract_lock:
                self.failed_count += 1
        finally:
            try:
                await page.close()
            except:
                pass
        return success

    async def phase2_worker(self, browser, worker_id: int, pids: List[str], total_pids: int):
        logger.info(f"[PHASE2 WORKER {worker_id}] Starting with {len(pids)} PIDs")

        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            locale="fr-FR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        init_page = await context.new_page()
        try:
            await init_page.goto("https://www.google.com/maps", wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2.5)
            await self.accept_cookies(init_page)
            await asyncio.sleep(1.5)
        except Exception as e:
            logger.error(f"[PHASE2 WORKER {worker_id}] Init error: {str(e)[:50]}")
        finally:
            await init_page.close()

        for i, pid in enumerate(pids):
            await self.extract_single_pid(context, pid, total_pids)
            if (i + 1) % 10 == 0:
                logger.info(f"[PHASE2 WORKER {worker_id}] Progress: {i+1}/{len(pids)}")
            await asyncio.sleep(0.5)

        await context.close()

    async def crawl_website_for_email(self, session: aiohttp.ClientSession, pid: str, base_url: str) -> Optional[str]:
        try:
            parsed = urlparse(base_url)
            base = f"{parsed.scheme}://{parsed.netloc}"
            all_emails = []

            for page_path in CONTACT_PAGES[:5]:
                try:
                    url = urljoin(base, page_path) if page_path else base_url
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=EMAIL_TIMEOUT)) as r:
                        if r.status == 200:
                            html = await r.text()
                            emails = extract_emails_from_html(html)
                            all_emails.extend(emails)
                            if emails:
                                break
                except:
                    continue

            if all_emails:
                return all_emails[0]
        except:
            pass
        return None

    async def extract_emails_advanced(self, websites: List[tuple]) -> int:
        if not websites:
            return 0

        emit("email_extraction_start", {"total_sites": len(websites), "global_percent": 80})

        connector = aiohttp.TCPConnector(limit=EMAIL_CONCURRENT, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            found = 0
            semaphore = asyncio.Semaphore(EMAIL_CONCURRENT)

            async def process_site(pid: str, url: str):
                nonlocal found
                async with semaphore:
                    email = await self.crawl_website_for_email(session, pid, url)
                    if email and pid in self.data.businesses:
                        self.data.businesses[pid]['email'] = email
                        found += 1
                        emit("email_found", {"email": email, "total_found": found})

            await asyncio.gather(*[process_site(pid, url) for pid, url in websites])
            return found

    async def run(self):
        emit("geocoding", {"city": self.city, "lat": self.lat, "lng": self.lng})
        emit("start", {
            "activity": self.activity,
            "city": self.city,
            "grid_size": self.grid_size,
            "total_zones": self.total_zones,
            "version": "v5.1 - filter invalid names (Résultats)"
        })

        async with async_playwright() as p:
            logger.info(f"[BROWSER] Launching Chromium (headless={HEADLESS})")

            browser_args = [
                '--disable-blink-features=AutomationControlled',
                '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu',
                '--window-size=1920,1080', '--start-maximized',
                '--disable-extensions', '--no-first-run'
            ]

            browser = await p.chromium.launch(headless=HEADLESS, args=browser_args)
            logger.info("[BROWSER] Chromium launched")

            zones = []
            zone_id = 0
            offset = self.grid_size // 2
            for i in range(self.grid_size):
                for j in range(self.grid_size):
                    lat = self.lat + (i - offset) * self.cell_size
                    lng = self.lng + (j - offset) * self.cell_size
                    zones.append((lat, lng, zone_id))
                    zone_id += 1

            emit("status", {"message": f"Phase 1: Collecte des IDs ({self.total_zones} zones)...", "global_percent": 0})

            phase1_workers = min(PHASE1_WORKERS, max(3, self.total_zones // 20))
            chunk_size = (len(zones) + phase1_workers - 1) // phase1_workers
            zone_chunks = [zones[i:i+chunk_size] for i in range(0, len(zones), chunk_size)]

            await asyncio.gather(*[
                self.phase1_worker(browser, i, zone_chunks[i])
                for i in range(min(phase1_workers, len(zone_chunks)))
            ])

            logger.info(f"[PHASE 1 COMPLETE] Total IDs: {len(self.data.place_ids)}")

            all_pids = self.data.get_all_ids()
            if all_pids:
                emit("extraction_start", {"total": len(all_pids), "global_percent": 40})
                emit("status", {"message": f"Phase 2: Extraction ({len(all_pids)} fiches)...", "global_percent": 40})

                chunk_size = (len(all_pids) + PHASE2_WORKERS - 1) // PHASE2_WORKERS
                pid_chunks = [all_pids[i:i+chunk_size] for i in range(0, len(all_pids), chunk_size)]

                await asyncio.gather(*[
                    self.phase2_worker(browser, i, pid_chunks[i], len(all_pids))
                    for i in range(min(PHASE2_WORKERS, len(pid_chunks)))
                ])

            await browser.close()

        websites = [(pid, b['website']) for pid, b in self.data.businesses.items() if b.get('website') and not b.get('email')]
        if websites:
            emit("status", {"message": f"Phase 3: Emails ({len(websites)} sites)...", "global_percent": 80})
            await self.extract_emails_advanced(websites)

        businesses = list(self.data.businesses.values())
        duration = (datetime.now() - self.start_time).total_seconds()

        stats = {
            "total": len(businesses),
            "with_phone": sum(1 for b in businesses if b.get('phone')),
            "with_email": sum(1 for b in businesses if b.get('email')),
            "with_website": sum(1 for b in businesses if b.get('website')),
            "duration_seconds": int(duration)
        }

        emit("complete", {"stats": stats, "businesses": businesses})


async def main():
    if len(sys.argv) < 3:
        emit("error", {"message": "Usage: python script.py 'activity' 'city' [grid_size]"})
        sys.exit(1)

    activity = sys.argv[1]
    city = sys.argv[2]
    grid_size = int(sys.argv[3]) if len(sys.argv) > 3 else None

    scraper = GMBScraperProduction(activity, city, grid_size)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
