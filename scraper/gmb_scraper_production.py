#!/usr/bin/env python3
"""
GMB Scraper PRODUCTION v4.2 - Robust Extraction
- Up to 3025 zones (55x55)
- Advanced email enrichment with page crawling
- 40 workers for optimal performance
- FIXED v4.1: Email filtering, progress bar accuracy
- FIXED v4.2: Robust place_id extraction (supports ChIJ + hex formats via stable patterns)
"""

import asyncio
import re
import json
import sys
from datetime import datetime
from typing import Dict, List, Set, Optional
from urllib.parse import quote, urljoin, urlparse
from playwright.async_api import async_playwright, Response
import aiohttp

# CONFIG
PHASE1_WORKERS = 15
PHASE2_WORKERS = 40
DEFAULT_GRID_SIZE = 10
BASE_COVERAGE = 0.09  # ~10km total coverage
SCROLL_COUNT = 6
SCROLL_DELAY = 0.3
PAGE_DELAY = 1.5
RETRY_ATTEMPTS = 2
EMAIL_TIMEOUT = 8
EMAIL_CONCURRENT = 30

# Pages to crawl for emails
CONTACT_PAGES = [
    '', '/contact', '/contacts', '/contactez-nous', '/nous-contacter',
    '/about', '/a-propos', '/qui-sommes-nous', '/about-us',
    '/mentions-legales', '/legal', '/cgv', '/cgu',
    '/footer', '/infos', '/informations'
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

# Email patterns to exclude - COMPREHENSIVE LIST (sans filtrer les numériques)
INVALID_EMAIL_PATTERNS = [
    # Placeholder/example emails (EN + FR)
    'example.', 'exemple.', '@sample.', '@demo.', '@fake.', '@test.',
    'your-email', 'votre-email', 'votre-mail', 'your-mail',
    'yourname@', 'votrenom@', 'votremail@', 'youremail@',
    'email@email', 'mail@mail', 'user@user',
    'info@example', 'contact@example',
    '@domain.', '@yourdomain', '@votredomaine', '@mondomaine',
    'placeholder', 'changeme', 'replace',
    
    # Big tech / platforms
    '@google.', '@facebook.', '@twitter.', '@instagram.', '@linkedin.',
    '@youtube.', '@tiktok.', '@pinterest.', '@snapchat.',
    'gstatic.com', 'googleapis.com',
    
    # CMS / Builders / Services
    '@sentry.', '@wix.', '@wordpress.', '@squarespace.', '@shopify.',
    '@webflow.', '@jimdo.', '@weebly.', '@godaddy.', '@ionos.',
    '@ovh.', '@cloudflare.', '@netlify.', '@vercel.', '@heroku.',
    'wixpress.com', 'squarespace.com',
    
    # System/automated emails
    'noreply@', 'no-reply@', 'donotreply@', 'do-not-reply@',
    'mailer-daemon@', 'postmaster@', 'webmaster@', 'hostmaster@',
    'abuse@', 'spam@', 'bounce@', 'return@', 'unsubscribe@',
    'newsletter@', 'notification@', 'alert@', 'system@', 'auto@',
    'robot@', 'bot@', 'daemon@', 'root@localhost', 'admin@localhost',
    
    # Invalid TLDs / patterns
    '.local', '.localhost', '.internal', '.invalid', '.test',
    '@127.', '@192.168.', '@10.0.',
    
    # File extensions accidentally captured
    '.png@', '.jpg@', '.gif@', '.svg@', '.css@', '.js@',
]

# Invalid email domains (exact match)
INVALID_DOMAINS = [
    'example.com', 'exemple.com', 'example.fr', 'exemple.fr',
    'test.com', 'test.fr', 'demo.com', 'fake.com',
    'domain.com', 'domain.fr', 'yourdomain.com', 'votredomaine.fr',
    'email.com', 'website.com', 'site.com',
    'company.com', 'entreprise.fr', 'societe.fr',
    'sentry.io', 'wix.com', 'squarespace.com',
]


def emit(event_type: str, data: dict):
    event = {"type": event_type, "timestamp": datetime.now().isoformat(), **data}
    print(json.dumps(event, ensure_ascii=False), flush=True)


def is_valid_email(email: str) -> bool:
    """
    Check if email is valid and not spam/system/placeholder.
    FIXED: Accepte les emails courts et numériques valides.
    """
    if not email or '@' not in email:
        return False
    
    email_lower = email.lower().strip()
    
    # Longueur max seulement (pas de min restrictif)
    # Un email valide minimum: a@b.co = 6 chars, mais on accepte tout ce qui passe la regex
    if len(email_lower) > 254:
        return False
    
    # Regex standard - ACCEPTE les chiffres dans la partie locale
    # Exemples valides: 123@domain.com, info2024@site.fr, 0612345678@sms.fr
    if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email_lower):
        return False
    
    # Check for invalid patterns
    for pattern in INVALID_EMAIL_PATTERNS:
        if pattern in email_lower:
            return False
    
    # Check domain
    try:
        domain = email_lower.split('@')[1]
        if domain in INVALID_DOMAINS:
            return False
        # Domaine trop court (minimum: x.co = 4 chars)
        if len(domain) < 4:
            return False
    except:
        return False
    
    return True


def extract_emails_from_html(html: str) -> List[str]:
    """Extract emails from HTML using multiple methods"""
    emails = set()
    
    # Method 1: Standard email regex
    standard_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', html)
    for email in standard_emails:
        if is_valid_email(email):
            emails.add(email.lower())
    
    # Method 2: mailto: links
    mailto_matches = re.findall(r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})', html, re.IGNORECASE)
    for email in mailto_matches:
        if is_valid_email(email):
            emails.add(email.lower())
    
    # Method 3: Obfuscated emails ([at], [dot], etc.)
    obfuscated = re.findall(r'([a-zA-Z0-9._%+-]+)\s*[\[\(]\s*(?:at|@|arobase)\s*[\]\)]\s*([a-zA-Z0-9.-]+)\s*[\[\(]\s*(?:dot|\.|point)\s*[\]\)]\s*([a-zA-Z]{2,})', html, re.IGNORECASE)
    for match in obfuscated:
        email = f"{match[0]}@{match[1]}.{match[2]}"
        if is_valid_email(email):
            emails.add(email.lower())
    
    # Method 4: data-email attributes
    data_emails = re.findall(r'data-email=["\']([^"\']*)["\']', html)
    for email in data_emails:
        if is_valid_email(email):
            emails.add(email.lower())
    
    # Method 5: JSON-LD structured data
    json_ld_emails = re.findall(r'"email"\s*:\s*"([^"]+)"', html)
    for email in json_ld_emails:
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
        self.grid_size = min(grid_size or DEFAULT_GRID_SIZE, 55)  # Max 55x55
        self.data = DataStore()
        self.start_time = datetime.now()
        self.zones_done = 0
        self.total_zones = self.grid_size * self.grid_size
        self._zone_lock = asyncio.Lock()
        self.extracted_count = 0
        self.failed_count = 0
        self._extract_lock = asyncio.Lock()
        self._last_progress_emit = 0  # Pour throttle les émissions

        # Dynamic cell size
        self.cell_size = BASE_COVERAGE / self.grid_size

        # City coordinates
        if self.city in CITY_COORDS:
            self.lat, self.lng = CITY_COORDS[self.city]
        else:
            self.lat, self.lng = CITY_COORDS['paris']

    def extract_place_ids(self, body: str) -> List[str]:
        """
        Extraction robuste des place_ids - supporte tous les formats Google:
        - ChIJ... (format classique)
        - 0x...:0x... (format hexadecimal/feature_id)
        Utilise des patterns STABLES (!1s, place_id, ftid) pour resister aux changements Google
        """
        ids = set()
        
        # Pattern 1: !1s suivi de l'ID (stable dans les URLs Google Maps)
        ids.update(re.findall(r'!1s([A-Za-z0-9_:-]+)', body))
        
        # Pattern 2: place_id, ftid, feature_id dans JSON/HTML
        ids.update(re.findall(r'(?:place_id|ftid|feature_id)["\']?\s*[:=]\s*["\']?([A-Za-z0-9_:-]+)', body, re.IGNORECASE))
        
        # Pattern 3: Anciens formats directs (fallback)
        ids.update(re.findall(r'ChIJ[A-Za-z0-9_-]{20,50}', body))
        ids.update(re.findall(r'0x[a-f0-9]+:0x[a-f0-9]+', body, re.IGNORECASE))
        
        # Filtrer les IDs trop courts (faux positifs) et nettoyer
        return [pid for pid in ids if len(pid) > 15]

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

    async def accept_cookies(self, page) -> bool:
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
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="fr-FR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
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
                    "lat": round(lat, 4),
                    "lng": round(lng, 4)
                })

                zoom = 15 if self.grid_size <= 10 else (16 if self.grid_size <= 25 else 17)
                url = f"https://www.google.com/maps/search/{quote(f'{self.activity} {self.city}')}/@{lat},{lng},{zoom}z"
                await page.goto(url, wait_until="domcontentloaded", timeout=20000)
                await asyncio.sleep(PAGE_DELAY)

                for _ in range(SCROLL_COUNT):
                    await page.evaluate('const f=document.querySelector(\'div[role="feed"]\');if(f)f.scrollTo(0,f.scrollHeight);')
                    await asyncio.sleep(SCROLL_DELAY)

                async with self._zone_lock:
                    self.zones_done += 1
                    # Phase 1 = 0-40% of total progress
                    phase1_percent = int((self.zones_done / self.total_zones) * 40)
                    emit("zone_complete", {
                        "zone": zone_id + 1,
                        "total_zones": self.total_zones,
                        "total_businesses": len(self.data.place_ids),
                        "percent": phase1_percent,
                        "global_percent": phase1_percent
                    })
            except:
                async with self._zone_lock:
                    self.zones_done += 1
                    # Émettre même en cas d'erreur pour maintenir la progression
                    phase1_percent = int((self.zones_done / self.total_zones) * 40)
                    emit("zone_complete", {
                        "zone": zone_id + 1,
                        "total_zones": self.total_zones,
                        "total_businesses": len(self.data.place_ids),
                        "percent": phase1_percent,
                        "global_percent": phase1_percent
                    })

        await context.close()

    async def extract_single_pid(self, context, pid: str, total_pids: int, attempt: int = 1) -> bool:
        page = await context.new_page()
        success = False
        
        try:
            url = f"https://www.google.com/maps/place/?q=place_id:{pid}"
            await page.goto(url, wait_until="load", timeout=20000)
            await asyncio.sleep(1.2)

            current_url = page.url
            if "consent" in current_url.lower():
                await self.accept_cookies(page)
                await asyncio.sleep(1.5)
                await page.goto(url, wait_until="load", timeout=20000)
                await asyncio.sleep(1.2)

            data = await page.evaluate('''() => {
                const d = {};
                const nameSelectors = ['h1.DUwDvf', 'h1.fontHeadlineLarge', 'h1'];
                for (const sel of nameSelectors) {
                    const el = document.querySelector(sel);
                    if (el && el.textContent) {
                        const name = el.textContent.trim();
                        if (name.length > 2 && !name.toLowerCase().includes("acceder") && 
                            !name.toLowerCase().includes("consent") && !name.toLowerCase().includes("google")) {
                            d.name = name;
                            break;
                        }
                    }
                }
                const phoneBtn = document.querySelector('button[data-item-id*="phone"], a[href^="tel:"]');
                if (phoneBtn) {
                    const label = phoneBtn.getAttribute('aria-label') || phoneBtn.href || '';
                    const match = label.match(/[0-9+][0-9\\s\\.\\-]{8,}/);
                    if (match) {
                        d.phone = match[0].replace(/[\\s\\.\\-]/g, '');
                        d.phone_clean = d.phone.replace(/[^0-9+]/g, '');
                    }
                }
                const webLink = document.querySelector('a[data-item-id="authority"]');
                if (webLink && webLink.href && !webLink.href.includes('google.com')) {
                    d.website = webLink.href;
                }
                const addrBtn = document.querySelector('button[data-item-id="address"]');
                if (addrBtn) {
                    const label = addrBtn.getAttribute('aria-label') || '';
                    d.address = label.replace(/^Adresse\\s*:\\s*/i, '').trim();
                }
                const ratingEl = document.querySelector('div.F7nice span[aria-hidden="true"]');
                if (ratingEl) {
                    const rating = parseFloat(ratingEl.textContent?.replace(',', '.'));
                    if (!isNaN(rating) && rating > 0 && rating <= 5) d.rating = rating;
                }
                const reviewEl = document.querySelector('div.F7nice span[aria-label*="avis"]');
                if (reviewEl) {
                    const text = (reviewEl.getAttribute('aria-label') || '').match(/[\\d\\s]+/);
                    if (text) d.review_count = parseInt(text[0].replace(/\\s/g, ''));
                }
                const catBtn = document.querySelector('button[jsaction*="category"]');
                if (catBtn) d.category = catBtn.textContent?.trim() || '';
                return d;
            }''')

            if data.get('name') and len(data['name']) > 2:
                data['place_id'] = pid
                data['google_maps_url'] = f"https://www.google.com/maps/place/?q=place_id:{pid}"
                is_new = await self.data.add_business(pid, data)
                if is_new:
                    async with self._extract_lock:
                        self.extracted_count += 1
                        # Émettre progression Phase 2 (40-80%)
                        current = self.extracted_count + self.failed_count
                        phase2_progress = int((current / total_pids) * 40)
                        global_percent = 40 + phase2_progress
                        # Émettre tous les 5 extractions ou si c'est la dernière
                        if current % 5 == 0 or current >= total_pids:
                            emit("extraction_progress", {
                                "extracted": self.extracted_count,
                                "failed": self.failed_count,
                                "total": total_pids,
                                "global_percent": min(global_percent, 80)
                            })
                    emit("business", data)
                success = True
            elif attempt < RETRY_ATTEMPTS:
                await page.close()
                await asyncio.sleep(0.5)
                return await self.extract_single_pid(context, pid, total_pids, attempt + 1)
            else:
                async with self._extract_lock:
                    self.failed_count += 1
        except:
            if attempt < RETRY_ATTEMPTS:
                try:
                    await page.close()
                except:
                    pass
                await asyncio.sleep(0.5)
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
        context = await browser.new_context(
            viewport={"width": 1280, "height": 720},
            locale="fr-FR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        )

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
            await self.extract_single_pid(context, pid, total_pids)
            await asyncio.sleep(0.2)

        await context.close()

    async def crawl_website_for_email(self, session: aiohttp.ClientSession, pid: str, base_url: str) -> Optional[str]:
        """Crawl multiple pages of a website to find emails"""
        try:
            parsed = urlparse(base_url)
            base = f"{parsed.scheme}://{parsed.netloc}"
            
            all_emails = []
            pages_crawled = 0
            
            for page_path in CONTACT_PAGES:
                if pages_crawled >= 5:  # Limit pages per site
                    break
                    
                try:
                    url = urljoin(base, page_path) if page_path else base_url
                    async with session.get(url, timeout=aiohttp.ClientTimeout(total=EMAIL_TIMEOUT)) as r:
                        if r.status == 200:
                            html = await r.text()
                            emails = extract_emails_from_html(html)
                            all_emails.extend(emails)
                            pages_crawled += 1
                            
                            if emails:  # Found emails, might be enough
                                break
                except:
                    continue
            
            if all_emails:
                # Return the most common email or first valid one
                email_counts = {}
                for email in all_emails:
                    email_counts[email] = email_counts.get(email, 0) + 1
                return max(email_counts, key=email_counts.get)
                
        except:
            pass
        return None

    async def extract_emails_advanced(self, websites: List[tuple]) -> int:
        """Advanced email extraction with multi-page crawling"""
        if not websites:
            return 0

        total_sites = len(websites)
        emit("email_extraction_start", {
            "total_sites": total_sites,
            "global_percent": 80
        })

        connector = aiohttp.TCPConnector(limit=EMAIL_CONCURRENT, ssl=False)
        timeout = aiohttp.ClientTimeout(total=EMAIL_TIMEOUT)

        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            found = 0
            processed = 0
            semaphore = asyncio.Semaphore(EMAIL_CONCURRENT)

            async def process_site(pid: str, url: str):
                nonlocal found, processed
                async with semaphore:
                    try:
                        email = await self.crawl_website_for_email(session, pid, url)
                        if email and pid in self.data.businesses:
                            self.data.businesses[pid]['email'] = email
                            found += 1
                            emit("email_found", {
                                "email": email,
                                "business_key": pid,
                                "total_found": found
                            })
                    except:
                        pass
                    finally:
                        processed += 1
                        # Phase 3 = 80-100% - Émettre à chaque site traité
                        phase3_progress = int((processed / total_sites) * 20)
                        global_percent = 80 + phase3_progress
                        emit("email_extraction_progress", {
                            "processed": processed,
                            "total": total_sites,
                            "found": found,
                            "global_percent": min(global_percent, 100)
                        })

            # Process in batches
            batch_size = 50
            for i in range(0, len(websites), batch_size):
                batch = websites[i:i+batch_size]
                await asyncio.gather(*[process_site(pid, url) for pid, url in batch])

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
            "cell_size_km": round(self.cell_size * 111, 2),
            "version": f"v4.2 Robust - 40 workers, {self.total_zones} zones"
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

            for i in range(self.grid_size):
                for j in range(self.grid_size):
                    lat = self.lat + (i - offset) * self.cell_size
                    lng = self.lng + (j - offset) * self.cell_size
                    zones.append((lat, lng, zone_id))
                    zone_id += 1

            # PHASE 1 - 0 to 40%
            emit("status", {
                "message": f"Phase 1: Collecte des IDs ({self.total_zones} zones)...",
                "global_percent": 0
            })

            phase1_workers = min(PHASE1_WORKERS, max(5, self.total_zones // 20))
            chunk_size = (len(zones) + phase1_workers - 1) // phase1_workers
            zone_chunks = [zones[i:i+chunk_size] for i in range(0, len(zones), chunk_size)]

            await asyncio.gather(*[
                self.phase1_worker(browser, i, zone_chunks[i])
                for i in range(min(phase1_workers, len(zone_chunks)))
            ])

            # PHASE 2 - 40 to 80%
            all_pids = self.data.get_all_ids()
            total_pids = len(all_pids)
            
            if all_pids:
                emit("extraction_start", {
                    "total": total_pids,
                    "global_percent": 40
                })
                emit("status", {
                    "message": f"Phase 2: Extraction ({total_pids} fiches)...",
                    "global_percent": 40
                })

                chunk_size = (len(all_pids) + PHASE2_WORKERS - 1) // PHASE2_WORKERS
                pid_chunks = [all_pids[i:i+chunk_size] for i in range(0, len(all_pids), chunk_size)]

                await asyncio.gather(*[
                    self.phase2_worker(browser, i, pid_chunks[i], total_pids)
                    for i in range(min(PHASE2_WORKERS, len(pid_chunks)))
                ])

            await browser.close()

        # PHASE 3 - 80 to 100%
        websites = [(pid, b['website']) for pid, b in self.data.businesses.items() 
                    if b.get('website') and not b.get('email')]
        
        emails_found = 0
        if websites:
            emit("status", {
                "message": f"Phase 3: Enrichissement emails ({len(websites)} sites)...",
                "global_percent": 80
            })
            emails_found = await self.extract_emails_advanced(websites)
        else:
            emit("status", {
                "message": "Phase 3: Aucun site web à crawler",
                "global_percent": 100
            })

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
