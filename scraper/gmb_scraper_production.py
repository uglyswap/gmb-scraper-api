#!/usr/bin/env python3
"""
GMB Scraper PRODUCTION v3.1 - Improved Reliability
Based on V39 with fixes for Phase 2 extraction failures
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

# OPTIMIZED CONFIG - Balance between speed and reliability
PHASE1_WORKERS = 15    # Workers for ID collection
PHASE2_WORKERS = 40    # Reduced for better success rate
GRID_SIZE = 10         # 10x10 = 100 zones
CELL_SIZE = 0.009      # ~1km zones
SCROLL_COUNT = 6       # More scrolls to find more results
SCROLL_DELAY = 0.3
PAGE_DELAY = 1.5       # Increased delay between pages
RETRY_ATTEMPTS = 2     # Retry failed extractions

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
                'button[aria-label*="Accept"]',
                '#L2AGLb',
                'button:has-text("J\'accepte")',
                'button:has-text("Accepter")',
            ]
            for selector in selectors:
                try:
                    btn = await page.query_selector(selector)
                    if btn:
                        await btn.click()
                        await asyncio.sleep(0.5)
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
            await page.goto(f"https://www.google.com/maps/search/{quote(f'{self.activity} {self.city}')}", timeout=30000)
            await asyncio.sleep(1.5)
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
                await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                await asyncio.sleep(PAGE_DELAY)

                # More scrolls to capture more results
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

    async def extract_single_pid(self, context, pid: str, attempt: int = 1) -> bool:
        """Extract data for a single PID with retry support"""
        page = await context.new_page()
        success = False
        
        try:
            url = f"https://www.google.com/maps/place/?q=place_id:{pid}"
            await page.goto(url, wait_until="load", timeout=20000)
            await asyncio.sleep(1.2)

            # Handle consent if redirected
            current_url = page.url
            if "consent" in current_url.lower():
                await self.accept_cookies(page)
                await asyncio.sleep(1.5)
                await page.goto(url, wait_until="load", timeout=20000)
                await asyncio.sleep(1.2)

            # Check if we're on the right page
            title = await page.title()
            if "acceder" in title.lower() or "consent" in title.lower():
                await self.accept_cookies(page)
                await asyncio.sleep(1.5)
                await page.goto(url, wait_until="load", timeout=20000)
                await asyncio.sleep(1.2)

            # Extract data
            data = await page.evaluate('''() => {
                const d = {};

                // Name - try multiple selectors
                const nameSelectors = [
                    'h1.DUwDvf',
                    'h1.fontHeadlineLarge',
                    'h1[data-attrid="title"]',
                    'div.fontHeadlineLarge',
                    'h1'
                ];
                
                for (const sel of nameSelectors) {
                    const el = document.querySelector(sel);
                    if (el && el.textContent) {
                        const name = el.textContent.trim();
                        if (name.length > 2 && 
                            !name.toLowerCase().includes("acceder") && 
                            !name.toLowerCase().includes("consent") &&
                            !name.toLowerCase().includes("cookie") &&
                            !name.toLowerCase().includes("google")) {
                            d.name = name;
                            break;
                        }
                    }
                }

                // Phone
                const phoneSelectors = [
                    'button[data-item-id*="phone"]',
                    'a[href^="tel:"]',
                    'button[aria-label*="telephone"]',
                    'button[aria-label*="Telephone"]',
                    'a[data-item-id*="phone"]'
                ];
                for (const sel of phoneSelectors) {
                    const el = document.querySelector(sel);
                    if (el) {
                        const label = el.getAttribute('aria-label') || el.href || el.textContent || '';
                        const match = label.match(/[0-9+][0-9\\s\\.\\-]{8,}/);
                        if (match) {
                            d.phone = match[0].replace(/[\\s\\.\\-]/g, '');
                            d.phone_clean = d.phone.replace(/[^0-9+]/g, '');
                            break;
                        }
                    }
                }

                // Website
                const webSelectors = [
                    'a[data-item-id="authority"]',
                    'a[data-tooltip="Ouvrir le site Web"]',
                    'a[aria-label*="site"]'
                ];
                for (const sel of webSelectors) {
                    const el = document.querySelector(sel);
                    if (el && el.href && !el.href.includes('google.com')) {
                        d.website = el.href;
                        break;
                    }
                }

                // Address
                const addrSelectors = [
                    'button[data-item-id="address"]',
                    'button[aria-label*="Adresse"]',
                    'div[data-item-id="address"]'
                ];
                for (const sel of addrSelectors) {
                    const el = document.querySelector(sel);
                    if (el) {
                        const label = el.getAttribute('aria-label') || el.textContent || '';
                        const addr = label.replace(/^Adresse\\s*:\\s*/i, '').trim();
                        if (addr.length > 5) {
                            d.address = addr;
                            break;
                        }
                    }
                }

                // Rating
                const ratingEl = document.querySelector('div.F7nice span[aria-hidden="true"]');
                if (ratingEl) {
                    const ratingText = ratingEl.textContent?.replace(',', '.');
                    const rating = parseFloat(ratingText);
                    if (!isNaN(rating) && rating > 0 && rating <= 5) d.rating = rating;
                }

                // Reviews
                const reviewSelectors = [
                    'div.F7nice span[aria-label*="avis"]',
                    'span[aria-label*="avis"]',
                    'button[aria-label*="avis"]'
                ];
                for (const sel of reviewSelectors) {
                    const el = document.querySelector(sel);
                    if (el) {
                        const reviewText = (el.getAttribute('aria-label') || el.textContent || '').match(/[\\d\\s]+/)?.[0]?.replace(/\\s/g, '');
                        const reviews = parseInt(reviewText);
                        if (!isNaN(reviews) && reviews > 0) {
                            d.review_count = reviews;
                            break;
                        }
                    }
                }

                // Category
                const catSelectors = [
                    'button[jsaction*="category"]',
                    'span.DkEaL',
                    'button.DkEaL'
                ];
                for (const sel of catSelectors) {
                    const el = document.querySelector(sel);
                    if (el && el.textContent) {
                        const cat = el.textContent.trim();
                        if (cat.length > 2) {
                            d.category = cat;
                            break;
                        }
                    }
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
                    emit("business", data)
                success = True
            else:
                # Retry if first attempt failed
                if attempt < RETRY_ATTEMPTS:
                    await page.close()
                    await asyncio.sleep(0.5)
                    return await self.extract_single_pid(context, pid, attempt + 1)
                else:
                    async with self._extract_lock:
                        self.failed_count += 1

        except Exception as e:
            if attempt < RETRY_ATTEMPTS:
                await page.close()
                await asyncio.sleep(0.5)
                return await self.extract_single_pid(context, pid, attempt + 1)
            else:
                async with self._extract_lock:
                    self.failed_count += 1
        finally:
            try:
                await page.close()
            except:
                pass
        
        return success

    async def phase2_worker(self, browser, worker_id: int, pids: List[str]):
        """Worker for Phase 2 - Detail extraction with retry"""
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            locale="fr-FR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        # Pre-accept cookies once
        init_page = await context.new_page()
        try:
            await init_page.goto("https://www.google.com/maps", timeout=20000)
            await asyncio.sleep(2)
            await self.accept_cookies(init_page)
            await asyncio.sleep(1)
        except:
            pass
        await init_page.close()

        for pid in pids:
            await self.extract_single_pid(context, pid)
            # Small delay between extractions to avoid rate limiting
            await asyncio.sleep(0.3)

        await context.close()

    async def extract_emails_batch(self, websites: List[tuple]) -> int:
        """Phase 3 - Fast email extraction"""
        if not websites:
            return 0

        emit("email_extraction_start", {"total_sites": len(websites)})

        timeout = aiohttp.ClientTimeout(total=5)
        connector = aiohttp.TCPConnector(limit=50)

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
                            # Filter out invalid emails (spam, images, etc.)
                            invalid_patterns = ['example', 'test', 'google', 'sentry', 'wix', 'wordpress', 'jquery', 'script', '.png', '.jpg', '.jpeg', '.gif', '.webp', '.svg', '.ico', '.css', '.js']
                            valid = [e for e in emails if not any(x in e.lower() for x in invalid_patterns)]
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

            batch_size = 50
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
            "version": "PRODUCTION v3.1 - 40 workers, improved reliability"
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
