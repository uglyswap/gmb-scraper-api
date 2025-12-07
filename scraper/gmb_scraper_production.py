#!/usr/bin/env python3
"""
GMB Scraper PRODUCTION - Optimal Performance
Based on V39: 55 workers, 99.8% success rate, ~450 businesses/min
With SSE streaming support for real-time frontend updates
"""

import asyncio
import re
import json
import sys
from datetime import datetime
from typing import Dict, List, Set
from urllib.parse import quote
from playwright.async_api import async_playwright, Response
import aiohttp

# OPTIMAL CONFIG - Based on extensive testing
# V39: 55 workers = 99.8% success, 452/min
# V40: 58 workers = 97.2% success (degradation starts)
# V37: 60 workers = 0% (complete failure)
PHASE1_WORKERS = 15    # Workers for ID collection
PHASE2_WORKERS = 55    # Optimal extraction workers
GRID_SIZE = 10         # 10x10 = 100 zones
CELL_SIZE = 0.009      # ~1km zones
SCROLL_COUNT = 4
SCROLL_DELAY = 0.2
PAGE_DELAY = 0.8

# City coordinates (can be extended)
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
}


def emit(event_type: str, data: dict):
    """Emit SSE event as JSON line"""
    event = {"type": event_type, "timestamp": datetime.now().isoformat(), **data}
    print(json.dumps(event, ensure_ascii=False), flush=True)


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

    async def add_business(self, pid: str, data: Dict):
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
        self.grid_size = grid_size or GRID_SIZE
        self.data = DataStore()
        self.start_time = datetime.now()
        self.zones_done = 0
        self.total_zones = self.grid_size * self.grid_size
        self._zone_lock = asyncio.Lock()
        self.extracted_count = 0
        self.failed_count = 0
        self._extract_lock = asyncio.Lock()

        # Get city coordinates
        if self.city in CITY_COORDS:
            self.lat, self.lng = CITY_COORDS[self.city]
        else:
            # Default to Paris for unknown cities
            self.lat, self.lng = CITY_COORDS['paris']

    def extract_place_ids(self, body: str) -> List[str]:
        return list(set(re.findall(r'ChIJ[A-Za-z0-9_-]{20,50}', body)))

    async def create_response_handler(self):
        async def handler(response: Response):
            try:
                url = response.url
                if 'google' not in url:
                    return
                if '/search' in url or '/maps/preview/place' in url or '/place/' in url:
                    body = await response.text()
                    if len(body) > 500:
                        ids = self.extract_place_ids(body)
                        if ids:
                            new = await self.data.add_ids(ids)
                            if new > 0:
                                emit("zone_links", {
                                    "unique_new": new,
                                    "total_ids": len(self.data.place_ids)
                                })
            except:
                pass
        return handler

    async def accept_cookies(self, page, debug_prefix="") -> bool:
        try:
            selectors = [
                'button:has-text("Tout accepter")',
                'button:has-text("Accept all")',
                'button[aria-label*="accepter"]',
                '#L2AGLb',
            ]
            for selector in selectors:
                try:
                    btn = await page.query_selector(selector)
                    if btn:
                        await btn.click()
                        await asyncio.sleep(0.3)
                        return True
                except:
                    continue
            return False
        except:
            return False

    async def phase1_worker(self, browser, worker_id: int, zones: List[tuple]):
        """Worker for Phase 1 - ID collection"""
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="fr-FR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        page = await context.new_page()
        handler = await self.create_response_handler()
        page.on("response", handler)

        try:
            await page.goto(f"https://www.google.com/maps/search/{quote(f'{self.activity} {self.city}')}", timeout=25000)
            await asyncio.sleep(1.2)
            await self.accept_cookies(page)
        except:
            pass

        for lat, lng, zone_id in zones:
            try:
                emit("zone_start", {
                    "zone": zone_id + 1,
                    "total_zones": self.total_zones,
                    "lat": lat,
                    "lng": lng
                })

                url = f"https://www.google.com/maps/search/{quote(f'{self.activity} {self.city}')}/@{lat},{lng},15z"
                await page.goto(url, wait_until="domcontentloaded", timeout=15000)
                await asyncio.sleep(PAGE_DELAY)

                for _ in range(SCROLL_COUNT):
                    await page.evaluate('const f=document.querySelector(\'div[role="feed"]\');if(f)f.scrollTo(0,f.scrollHeight);')
                    await asyncio.sleep(SCROLL_DELAY)

                async with self._zone_lock:
                    self.zones_done += 1
                    emit("zone_complete", {
                        "zone": zone_id + 1,
                        "total_zones": self.total_zones,
                        "total_businesses": len(self.data.place_ids),
                        "percent": int(self.zones_done / self.total_zones * 100)
                    })

            except:
                pass

        await context.close()

    async def phase2_worker(self, browser, worker_id: int, pids: List[str]):
        """Worker for Phase 2 - CRITICAL: NEW PAGE PER PID"""
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            locale="fr-FR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        # Pre-accept cookies once
        init_page = await context.new_page()
        try:
            await init_page.goto("https://www.google.com/maps", timeout=15000)
            await asyncio.sleep(1.5)
            await self.accept_cookies(init_page)
            await asyncio.sleep(0.5)
        except:
            pass
        await init_page.close()

        for pid in pids:
            # CRITICAL: Create NEW page for each PID
            page = await context.new_page()
            try:
                url = f"https://www.google.com/maps/place/?q=place_id:{pid}"
                await page.goto(url, wait_until="load", timeout=12000)
                await asyncio.sleep(0.8)

                # Handle consent if redirected
                title = await page.title()
                current_url = page.url
                if "consent" in current_url.lower() or "acceder" in title.lower():
                    await self.accept_cookies(page)
                    await asyncio.sleep(1)
                    await page.goto(url, wait_until="load", timeout=12000)
                    await asyncio.sleep(0.8)

                # Extract data
                data = await page.evaluate('''() => {
                    const d = {};

                    // Name
                    const h1 = document.querySelector('h1.DUwDvf');
                    if (h1 && h1.textContent) {
                        const name = h1.textContent.trim();
                        if (name.length > 2 && !name.toLowerCase().includes("acceder")) {
                            d.name = name;
                        }
                    }

                    if (!d.name) {
                        const allH1 = document.querySelectorAll('h1');
                        for (const el of allH1) {
                            const txt = el.textContent?.trim() || '';
                            if (txt.length > 2 && !txt.toLowerCase().includes('acceder') && !txt.toLowerCase().includes('consent')) {
                                d.name = txt;
                                break;
                            }
                        }
                    }

                    // Phone
                    const phoneSelectors = [
                        'button[data-item-id*="phone"]',
                        'a[href^="tel:"]',
                        'button[aria-label*="telephone"]'
                    ];
                    for (const sel of phoneSelectors) {
                        const el = document.querySelector(sel);
                        if (el) {
                            const label = el.getAttribute('aria-label') || el.href || '';
                            const match = label.match(/[0-9+\\s\\.\\-]{10,}/);
                            if (match) {
                                d.phone = match[0].replace(/[\\s\\.\\-]/g, '');
                                d.phone_clean = d.phone.replace(/[^0-9+]/g, '');
                                break;
                            }
                        }
                    }

                    // Website
                    const webLink = document.querySelector('a[data-item-id="authority"]');
                    if (webLink && webLink.href && !webLink.href.includes('google.com')) {
                        d.website = webLink.href;
                    }

                    // Address
                    const addrBtn = document.querySelector('button[data-item-id="address"]');
                    if (addrBtn) {
                        const label = addrBtn.getAttribute('aria-label') || '';
                        d.address = label.replace(/^Adresse\\s*:\\s*/i, '').trim();
                    }

                    // Rating
                    const ratingEl = document.querySelector('div.F7nice span[aria-hidden="true"]');
                    if (ratingEl) {
                        const ratingText = ratingEl.textContent?.replace(',', '.');
                        const rating = parseFloat(ratingText);
                        if (!isNaN(rating)) d.rating = rating;
                    }

                    // Reviews
                    const reviewEl = document.querySelector('div.F7nice span[aria-label*="avis"]');
                    if (reviewEl) {
                        const reviewText = reviewEl.textContent?.match(/[\\d\\s]+/)?.[0]?.replace(/\\s/g, '');
                        const reviews = parseInt(reviewText);
                        if (!isNaN(reviews)) d.review_count = reviews;
                    }

                    // Category
                    const catEl = document.querySelector('button[jsaction*="category"]');
                    if (catEl) {
                        d.category = catEl.textContent?.trim() || '';
                    }

                    return d;
                }''')

                if data.get('name') and len(data['name']) > 2:
                    data['place_id'] = pid
                    data['google_maps_url'] = f"https://www.google.com/maps/place/?q=place_id:{pid}"

                    is_new = await self.data.add_business(pid, data)
                    if is_new:
                        async with self._extract_lock:
                            self.extracted_count += 1

                        # Emit business event
                        emit("business", data)
                else:
                    async with self._extract_lock:
                        self.failed_count += 1

            except:
                async with self._extract_lock:
                    self.failed_count += 1
            finally:
                # CRITICAL: Close page after each PID
                await page.close()

        await context.close()

    async def extract_emails_batch(self, websites: List[tuple]) -> int:
        """Phase 3 - Fast email extraction"""
        if not websites:
            return 0

        emit("email_extraction_start", {"total_sites": len(websites)})

        timeout = aiohttp.ClientTimeout(total=4)
        connector = aiohttp.TCPConnector(limit=100)

        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            found = 0
            processed = 0

            async def fetch_email(pid: str, url: str):
                nonlocal found, processed
                try:
                    async with session.get(url, ssl=False) as r:
                        if r.status == 200:
                            html = await r.text()
                            emails = re.findall(r'([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', html)
                            valid = [e for e in emails if not any(x in e.lower() for x in ['example', 'test', 'google', 'sentry', 'wix', 'wordpress', 'jquery', 'script'])]
                            if valid and pid in self.data.businesses:
                                self.data.businesses[pid]['email'] = valid[0]
                                found += 1
                                emit("email_found", {
                                    "email": valid[0],
                                    "business_key": pid,
                                    "total_found": found
                                })
                except:
                    pass
                processed += 1

            batch_size = 100
            for i in range(0, len(websites), batch_size):
                batch = websites[i:i+batch_size]
                await asyncio.gather(*[fetch_email(pid, url) for pid, url in batch])
                emit("email_extraction_progress", {
                    "processed": min(i + batch_size, len(websites)),
                    "total": len(websites),
                    "found": found
                })

            return found

    async def run(self):
        emit("geocoding", {
            "city": self.city,
            "display_name": self.city.title(),
            "lat": self.lat,
            "lng": self.lng
        })

        emit("start", {
            "activity": self.activity,
            "city": self.city,
            "grid_size": self.grid_size,
            "total_zones": self.total_zones,
            "version": "PRODUCTION v3.0 - 55 workers, ~450/min"
        })

        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                args=['--disable-blink-features=AutomationControlled', '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu']
            )

            # Generate grid
            zones = []
            zone_id = 0
            offset = self.grid_size // 2
            cell_size = CELL_SIZE

            for i in range(self.grid_size):
                for j in range(self.grid_size):
                    lat = self.lat + (i - offset) * cell_size
                    lng = self.lng + (j - offset) * cell_size
                    zones.append((lat, lng, zone_id))
                    zone_id += 1

            # PHASE 1 - ID Collection
            emit("status", {"message": "Phase 1: Collecte des IDs..."})

            chunk_size = (len(zones) + PHASE1_WORKERS - 1) // PHASE1_WORKERS
            zone_chunks = [zones[i:i+chunk_size] for i in range(0, len(zones), chunk_size)]

            await asyncio.gather(*[
                self.phase1_worker(browser, i, zone_chunks[i])
                for i in range(min(PHASE1_WORKERS, len(zone_chunks)))
            ])

            # PHASE 2 - Detail Extraction
            all_pids = self.data.get_all_ids()
            if all_pids:
                emit("extraction_start", {"total": len(all_pids)})
                emit("status", {"message": f"Phase 2: Extraction des details ({len(all_pids)} fiches)..."})

                chunk_size = (len(all_pids) + PHASE2_WORKERS - 1) // PHASE2_WORKERS
                pid_chunks = [all_pids[i:i+chunk_size] for i in range(0, len(all_pids), chunk_size)]

                await asyncio.gather(*[
                    self.phase2_worker(browser, i, pid_chunks[i])
                    for i in range(min(PHASE2_WORKERS, len(pid_chunks)))
                ])

            await browser.close()

        # PHASE 3 - Email Extraction
        websites = [(pid, b['website']) for pid, b in self.data.businesses.items() if b.get('website')]
        emails_found = 0
        if websites:
            emit("status", {"message": f"Phase 3: Extraction des emails ({len(websites)} sites)..."})
            emails_found = await self.extract_emails_batch(websites)

        # Final stats
        businesses = list(self.data.businesses.values())
        duration = (datetime.now() - self.start_time).total_seconds()

        stats = {
            "total": len(businesses),
            "with_phone": sum(1 for b in businesses if b.get('phone')),
            "with_email": sum(1 for b in businesses if b.get('email')),
            "with_website": sum(1 for b in businesses if b.get('website')),
            "with_address": sum(1 for b in businesses if b.get('address')),
            "with_category": sum(1 for b in businesses if b.get('category')),
            "with_rating": sum(1 for b in businesses if b.get('rating')),
            "filtered_out": self.failed_count,
            "duration_seconds": int(duration)
        }

        emit("complete", {
            "stats": stats,
            "businesses": businesses
        })


async def main():
    if len(sys.argv) < 3:
        emit("error", {"message": "Usage: python gmb_scraper_production.py 'activity' 'city' [grid_size]"})
        sys.exit(1)

    activity = sys.argv[1]
    city = sys.argv[2]
    grid_size = int(sys.argv[3]) if len(sys.argv) > 3 else None

    scraper = GMBScraperProduction(activity, city, grid_size)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())
