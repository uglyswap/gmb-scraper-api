#!/usr/bin/env python3
"""GMB Scraper PRODUCTION v6.1 - Fixed Batch Processing
FIXES: v6.0 bug where memory mode lost data between batches"""

import asyncio, re, json, sys, os, gc, logging
from datetime import datetime
from typing import Dict, List, Set, Optional, Tuple
from urllib.parse import quote, urljoin, urlparse
from playwright.async_api import async_playwright, Response, Page
import aiohttp

try:
    import redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

HEADLESS = os.environ.get("HEADLESS", "true").lower() == "true"
DEBUG_MODE = os.environ.get("DEBUG", "false").lower() == "true"

# v6.1 CONFIG
BATCH_SIZE = 200
BROWSER_RESTART_INTERVAL = 3
GC_INTERVAL = 1
MEMORY_LIMIT_MB = 1500
REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
REDIS_KEY_PREFIX = "gmb_scraper:"
REDIS_EXPIRE_HOURS = 24

# ORIGINAL CONFIG
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

INVALID_NAMES = ["résultats", "resultats", "results", "recherche", "search", "consent", "google maps", "google", "before you continue", "avant d'accéder", "maps", "plan", "itinéraire"]
CONTACT_PAGES = ["", "/contact", "/contacts", "/contactez-nous", "/nous-contacter", "/about", "/a-propos", "/qui-sommes-nous", "/about-us", "/mentions-legales", "/legal", "/cgv", "/cgu"]
CITY_COORDS = {"paris": (48.8566, 2.3522), "lyon": (45.7640, 4.8357), "marseille": (43.2965, 5.3698), "toulouse": (43.6047, 1.4442), "nice": (43.7102, 7.2620), "nantes": (47.2184, -1.5536), "strasbourg": (48.5734, 7.7521), "montpellier": (43.6108, 3.8767), "bordeaux": (44.8378, -0.5792), "lille": (50.6292, 3.0573), "rennes": (48.1173, -1.6778), "reims": (49.2583, 4.0317), "toulon": (43.1242, 5.9280), "grenoble": (45.1885, 5.7245), "dijon": (47.3220, 5.0415), "angers": (47.4784, -0.5632), "nimes": (43.8367, 4.3601)}
INVALID_EMAIL_PATTERNS = ["example.", "exemple.", "@sample.", "@demo.", "@fake.", "@test.", "your-email", "votre-email", "placeholder", "changeme", "@google.", "@facebook.", "@twitter.", "@instagram.", "gstatic.com", "googleapis.com", "@sentry.", "@wix.", "@wordpress.", "@squarespace.", "wixpress.com", "squarespace.com", "noreply@", "no-reply@", "donotreply@", "mailer-daemon@", "postmaster@", "webmaster@", ".local", ".localhost", ".internal", ".invalid"]
INVALID_DOMAINS = ["example.com", "exemple.com", "test.com", "demo.com", "domain.com", "email.com", "website.com", "sentry.io", "wix.com", "squarespace.com"]

def emit(event_type: str, data: dict):
    event = {"type": event_type, "timestamp": datetime.now().isoformat(), **data}
    print(json.dumps(event, ensure_ascii=False), flush=True)

def is_valid_business_name(name: str) -> bool:
    if not name or len(name) < 3: return False
    name_lower = name.lower().strip()
    for invalid in INVALID_NAMES:
        if name_lower == invalid or name_lower.startswith(invalid + " "): return False
    return True

def is_valid_email(email: str) -> bool:
    if not email or "@" not in email: return False
    email_lower = email.lower().strip()
    if len(email_lower) > 254: return False
    if not re.match(r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$", email_lower): return False
    for pattern in INVALID_EMAIL_PATTERNS:
        if pattern in email_lower: return False
    try:
        domain = email_lower.split("@")[1]
        if domain in INVALID_DOMAINS or len(domain) < 4: return False
    except: return False
    return True

def extract_emails_from_html(html: str) -> List[str]:
    emails = set()
    for email in re.findall(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}", html):
        if is_valid_email(email): emails.add(email.lower())
    for email in re.findall(r"mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})", html, re.IGNORECASE):
        if is_valid_email(email): emails.add(email.lower())
    return list(emails)

def get_memory_usage_mb() -> float:
    if PSUTIL_AVAILABLE:
        return psutil.Process().memory_info().rss / 1024 / 1024
    return 0.0

class RedisDataStore:
    def __init__(self, session_id: str = None):
        self.session_id = session_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self._lock = asyncio.Lock()
        self._ids_key = f"{REDIS_KEY_PREFIX}{self.session_id}:place_ids"
        self._businesses_key = f"{REDIS_KEY_PREFIX}{self.session_id}:businesses"
        self._processed_key = f"{REDIS_KEY_PREFIX}{self.session_id}:processed"
        self.redis_client = None
        self.use_redis = False
        if REDIS_AVAILABLE:
            try:
                self.redis_client = redis.from_url(REDIS_URL, decode_responses=True)
                self.redis_client.ping()
                self.use_redis = True
                logger.info(f"[REDIS] Connected. Session: {self.session_id}")
                for key in [self._ids_key, self._businesses_key, self._processed_key]:
                    self.redis_client.expire(key, REDIS_EXPIRE_HOURS * 3600)
            except Exception as e:
                logger.warning(f"[REDIS] Connection failed: {e}. Using in-memory.")
        else:
            logger.warning("[REDIS] redis-py not installed. Using in-memory.")
        self._memory_place_ids: Set[str] = set()
        self._memory_businesses: Dict[str, Dict] = {}
        self._memory_processed: Set[str] = set()  # v6.1 FIX: Track processed IDs in memory mode

    async def add_ids(self, ids: List[str]) -> int:
        if not ids: return 0
        async with self._lock:
            if self.use_redis:
                try:
                    before = self.redis_client.scard(self._ids_key)
                    self.redis_client.sadd(self._ids_key, *ids)
                    return self.redis_client.scard(self._ids_key) - before
                except: pass
            before = len(self._memory_place_ids)
            self._memory_place_ids.update(ids)
            return len(self._memory_place_ids) - before

    async def add_business(self, pid: str, data: Dict) -> bool:
        async with self._lock:
            if self.use_redis:
                try:
                    if not self.redis_client.hexists(self._businesses_key, pid):
                        self.redis_client.hset(self._businesses_key, pid, json.dumps(data, ensure_ascii=False))
                        return True
                    return False
                except: pass
            if pid not in self._memory_businesses:
                self._memory_businesses[pid] = data
                return True
            return False

    def get_all_ids(self) -> List[str]:
        if self.use_redis:
            try: return list(self.redis_client.smembers(self._ids_key))
            except: pass
        return list(self._memory_place_ids)

    def get_ids_count(self) -> int:
        if self.use_redis:
            try: return self.redis_client.scard(self._ids_key)
            except: pass
        return len(self._memory_place_ids)

    def get_unprocessed_ids(self, limit: int = None) -> List[str]:
        if self.use_redis:
            try:
                ids = list(self.redis_client.sdiff(self._ids_key, self._processed_key))
                return ids[:limit] if limit else ids
            except: pass
        # v6.1 FIX: Subtract processed IDs in memory mode
        ids = list(self._memory_place_ids - self._memory_processed)
        logger.info(f"[DATASTORE] Unprocessed: {len(ids)} (total: {len(self._memory_place_ids)}, processed: {len(self._memory_processed)})")
        return ids[:limit] if limit else ids

    async def mark_ids_processed(self, ids: List[str]):
        if not ids: return
        async with self._lock:
            if self.use_redis:
                try: self.redis_client.sadd(self._processed_key, *ids)
                except: pass
            # v6.1 FIX: Always track processed IDs in memory
            self._memory_processed.update(ids)
            logger.info(f"[DATASTORE] Marked {len(ids)} IDs as processed (total processed: {len(self._memory_processed)})")

    def get_business(self, pid: str) -> Optional[Dict]:
        if self.use_redis:
            try:
                data = self.redis_client.hget(self._businesses_key, pid)
                if data: return json.loads(data)
            except: pass
        return self._memory_businesses.get(pid)

    def update_business(self, pid: str, data: Dict):
        if self.use_redis:
            try: self.redis_client.hset(self._businesses_key, pid, json.dumps(data, ensure_ascii=False))
            except: self._memory_businesses[pid] = data
        else: self._memory_businesses[pid] = data

    def get_all_businesses(self) -> List[Dict]:
        if self.use_redis:
            try: return [json.loads(v) for v in self.redis_client.hgetall(self._businesses_key).values()]
            except: pass
        return list(self._memory_businesses.values())

    def get_businesses_dict(self) -> Dict[str, Dict]:
        if self.use_redis:
            try: return {k: json.loads(v) for k, v in self.redis_client.hgetall(self._businesses_key).items()}
            except: pass
        return self._memory_businesses.copy()

class GMBScraperProduction:
    def __init__(self, activity: str, city: str, grid_size: int = None):
        self.activity = activity
        self.city = city.lower()
        self.grid_size = min(grid_size or DEFAULT_GRID_SIZE, 55)
        session_id = f"{self.city}_{self.activity}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.data = RedisDataStore(session_id)
        self.start_time = datetime.now()
        self.zones_done = 0
        self.total_zones = self.grid_size * self.grid_size
        self._zone_lock = asyncio.Lock()
        self.extracted_count = 0
        self.failed_count = 0
        self._extract_lock = asyncio.Lock()
        self.cell_size = BASE_COVERAGE / self.grid_size
        self.lat, self.lng = CITY_COORDS.get(self.city, CITY_COORDS['paris'])
        self.total_batches = (self.total_zones + BATCH_SIZE - 1) // BATCH_SIZE
        self.use_batch_processing = self.total_zones > BATCH_SIZE
        logger.info(f"[INIT] Grid: {self.grid_size}x{self.grid_size} = {self.total_zones} zones, Batches: {self.total_batches}, Redis: {self.data.use_redis}")

    def _generate_zones(self) -> List[Tuple[float, float, int]]:
        zones = []
        offset = self.grid_size // 2
        for i in range(self.grid_size):
            for j in range(self.grid_size):
                zones.append((self.lat + (i - offset) * self.cell_size, self.lng + (j - offset) * self.cell_size, i * self.grid_size + j))
        return zones

    def _chunk_list(self, lst: List, size: int) -> List[List]:
        return [lst[i:i + size] for i in range(0, len(lst), size)]

    async def _create_browser(self, playwright):
        return await playwright.chromium.launch(headless=HEADLESS, args=['--disable-blink-features=AutomationControlled', '--no-sandbox', '--disable-dev-shm-usage', '--disable-gpu', '--disable-extensions'])

    def extract_place_ids(self, body: str) -> List[str]:
        ids = set()
        ids.update(re.findall(r'ChIJ[A-Za-z0-9_-]{23}', body))
        ids.update(re.findall(r'0x[a-f0-9]{14,16}:0x[a-f0-9]{14,16}', body, re.IGNORECASE))
        return [pid for pid in ids if (pid.startswith('ChIJ') and len(pid) == 27) or (pid.startswith('0x') and ':0x' in pid)]

    async def create_response_handler(self):
        async def handler(response):
            try:
                url = response.url
                if 'google' in url and any(x in url for x in ['/search', '/maps/rpc', '/maps/preview/place', '/place/']):
                    body = await response.text()
                    if len(body) > 500:
                        ids = self.extract_place_ids(body)
                        if ids:
                            new = await self.data.add_ids(ids)
                            if new > 0:
                                emit("zone_links", {"unique_new": new, "total_ids": self.data.get_ids_count()})
            except: pass
        return handler

    async def accept_cookies(self, page) -> bool:
        try:
            if 'consent' not in page.url.lower(): return True
            await asyncio.sleep(1.0)
            for sel in ['[aria-label="Tout accepter"]', '[aria-label="Accept all"]', 'button:has-text("Tout accepter")']:
                try:
                    btn = await page.query_selector(sel)
                    if btn and await btn.is_visible():
                        await btn.click()
                        await asyncio.sleep(1.5)
                        if 'consent' not in page.url.lower(): return True
                except: continue
            return False
        except: return False

    async def phase1_worker(self, browser, worker_id: int, zones: List[tuple], batch_num: int = 0):
        context = await browser.new_context(viewport={"width": 1920, "height": 1080}, locale="fr-FR", user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        page = await context.new_page()
        page.on("response", await self.create_response_handler())
        initial_url = f"https://www.google.com/maps/search/{quote(f'{self.activity} {self.city}')}"
        try:
            await page.goto(initial_url, wait_until="networkidle", timeout=45000)
            await asyncio.sleep(INITIAL_PAGE_DELAY)
            await self.accept_cookies(page)
        except: pass
        for lat, lng, zone_id in zones:
            try:
                emit("zone_start", {"zone": zone_id + 1, "total_zones": self.total_zones})
                zoom = 15 if self.grid_size <= 10 else (16 if self.grid_size <= 25 else 17)
                url = f"https://www.google.com/maps/search/{quote(f'{self.activity} {self.city}')}/@{lat},{lng},{zoom}z"
                await page.goto(url, wait_until="networkidle", timeout=30000)
                await asyncio.sleep(PAGE_DELAY)
                if 'consent' in page.url.lower():
                    await self.accept_cookies(page)
                    await page.goto(url, wait_until="networkidle", timeout=30000)
                for _ in range(SCROLL_COUNT):
                    await page.evaluate("const f=document.querySelector('div[role=\"feed\"]');if(f)f.scrollTo(0,f.scrollHeight);")
                    await asyncio.sleep(SCROLL_DELAY)
                async with self._zone_lock:
                    self.zones_done += 1
                    emit("zone_complete", {"zone": zone_id + 1, "total_zones": self.total_zones, "total_businesses": self.data.get_ids_count(), "global_percent": int((self.zones_done / self.total_zones) * 40), "memory_mb": round(get_memory_usage_mb(), 1)})
            except:
                async with self._zone_lock: self.zones_done += 1
        await context.close()

    async def extract_single_pid(self, context, pid: str, total_pids: int, attempt: int = 1) -> bool:
        page = await context.new_page()
        try:
            url = f"https://www.google.com/maps/search/?api=1&query=Google&query_place_id={pid}" if attempt == 1 else f"https://www.google.com/maps/place/?q=place_id:{pid}"
            await page.goto(url, wait_until="networkidle", timeout=30000)
            await asyncio.sleep(2.5)
            if "consent" in page.url.lower():
                await self.accept_cookies(page)
                await page.goto(url, wait_until="networkidle", timeout=30000)
            data = await page.evaluate('''() => {
                const d = {};
                for (const sel of ["h1.DUwDvf", "h1.fontHeadlineLarge", "div[role=\\"main\\"] h1", "h1"]) {
                    const el = document.querySelector(sel);
                    if (el?.textContent?.trim().length > 2) { d.name = el.textContent.trim(); break; }
                }
                for (const sel of ["button[data-item-id*=\\"phone\\"]", "a[href^=\\"tel:\\"]"]) {
                    const el = document.querySelector(sel);
                    if (el) {
                        const m = (el.getAttribute("aria-label") || el.href || "").match(/[0-9+][0-9\\s.-]{8,}/);
                        if (m) { d.phone = m[0].replace(/[\\s.-]/g, ""); break; }
                    }
                }
                const web = document.querySelector("a[data-item-id=\\"authority\\"]");
                if (web?.href && !web.href.includes("google.com")) d.website = web.href;
                const addr = document.querySelector("button[data-item-id=\\"address\\"]");
                if (addr) d.address = (addr.getAttribute("aria-label") || "").replace(/^Adresse\\s*:\\s*/i, "").trim();
                const rating = document.querySelector("div.F7nice span[aria-hidden=\\"true\\"]");
                if (rating) { const r = parseFloat(rating.textContent?.replace(",", ".")); if (r > 0 && r <= 5) d.rating = r; }
                const cat = document.querySelector("button[jsaction*=\\"category\\"], span.DkEaL");
                if (cat) d.category = cat.textContent?.trim() || "";
                return d;
            }''')
            name = data.get("name", "")
            if name and is_valid_business_name(name):
                data["place_id"] = pid
                data["google_maps_url"] = f"https://www.google.com/maps/place/?q=place_id:{pid}"
                if await self.data.add_business(pid, data):
                    async with self._extract_lock:
                        self.extracted_count += 1
                        if self.extracted_count % 5 == 0:
                            emit("extraction_progress", {"extracted": self.extracted_count, "total": total_pids, "global_percent": min(40 + int(((self.extracted_count + self.failed_count) / total_pids) * 40), 80)})
                    emit("business", data)
                return True
            elif attempt < RETRY_ATTEMPTS:
                await page.close()
                await asyncio.sleep(1.0)
                return await self.extract_single_pid(context, pid, total_pids, attempt + 1)
            else:
                async with self._extract_lock: self.failed_count += 1
        except:
            if attempt < RETRY_ATTEMPTS:
                try: await page.close()
                except: pass
                return await self.extract_single_pid(context, pid, total_pids, attempt + 1)
            async with self._extract_lock: self.failed_count += 1
        finally:
            try: await page.close()
            except: pass
        return False

    async def phase2_worker(self, browser, worker_id: int, pids: List[str], total_pids: int):
        context = await browser.new_context(viewport={"width": 1280, "height": 720}, locale="fr-FR", user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        init_page = await context.new_page()
        try:
            await init_page.goto("https://www.google.com/maps", wait_until="networkidle", timeout=30000)
            await self.accept_cookies(init_page)
        except: pass
        finally: await init_page.close()
        for pid in pids:
            await self.extract_single_pid(context, pid, total_pids)
            await asyncio.sleep(0.5)
        await context.close()

    async def crawl_website_for_email(self, session, pid: str, base_url: str) -> Optional[str]:
        try:
            parsed = urlparse(base_url)
            base = f"{parsed.scheme}://{parsed.netloc}"
            for path in ["", "/contact", "/about"]:
                try:
                    async with session.get(urljoin(base, path), timeout=aiohttp.ClientTimeout(total=EMAIL_TIMEOUT)) as r:
                        if r.status == 200:
                            emails = extract_emails_from_html(await r.text())
                            if emails: return emails[0]
                except: continue
        except: pass
        return None

    async def extract_emails_advanced(self, websites: List[tuple]) -> int:
        if not websites: return 0
        emit("email_extraction_start", {"total_sites": len(websites), "global_percent": 80})
        connector = aiohttp.TCPConnector(limit=EMAIL_CONCURRENT, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            found = 0
            sem = asyncio.Semaphore(EMAIL_CONCURRENT)
            async def process(pid, url):
                nonlocal found
                async with sem:
                    email = await self.crawl_website_for_email(session, pid, url)
                    if email:
                        b = self.data.get_business(pid)
                        if b:
                            b["email"] = email
                            self.data.update_business(pid, b)
                            found += 1
                            emit("email_found", {"email": email, "total_found": found})
            await asyncio.gather(*[process(p, u) for p, u in websites])
            return found

    async def _run_phase1_batch(self, browser, zones, batch_num):
        workers = min(PHASE1_WORKERS, max(3, len(zones) // 20))
        chunks = self._chunk_list(zones, (len(zones) + workers - 1) // workers)
        await asyncio.gather(*[self.phase1_worker(browser, i, chunks[i], batch_num) for i in range(len(chunks))])

    async def _run_phase2_batch(self, browser, pids):
        if not pids: return []
        workers = min(PHASE2_WORKERS, max(3, len(pids) // 10))
        chunks = self._chunk_list(pids, (len(pids) + workers - 1) // workers)
        await asyncio.gather(*[self.phase2_worker(browser, i, chunks[i], len(pids)) for i in range(len(chunks))])
        return [(p, b["website"]) for p, b in self.data.get_businesses_dict().items() if b.get("website") and not b.get("email")]

    async def run(self):
        emit("start", {"activity": self.activity, "city": self.city, "grid_size": self.grid_size, "total_zones": self.total_zones, "total_batches": self.total_batches, "redis_enabled": self.data.use_redis, "version": "v6.1 - Fixed Batch Processing"})
        all_zones = self._generate_zones()
        all_websites = []
        async with async_playwright() as p:
            browser = await self._create_browser(p)
            if self.use_batch_processing:
                batches = self._chunk_list(all_zones, BATCH_SIZE)
                for batch_num, zone_batch in enumerate(batches):
                    batch_start = datetime.now()
                    emit("batch_start", {"batch": batch_num + 1, "total_batches": len(batches), "zones_in_batch": len(zone_batch), "memory_mb": round(get_memory_usage_mb(), 1)})
                    await self._run_phase1_batch(browser, zone_batch, batch_num)
                    batch_pids = self.data.get_unprocessed_ids()
                    if batch_pids:
                        websites = await self._run_phase2_batch(browser, batch_pids)
                        all_websites.extend(websites)
                        await self.data.mark_ids_processed(batch_pids)
                    if (batch_num + 1) % GC_INTERVAL == 0:
                        gc.collect()
                        logger.info(f"[GC] Memory: {get_memory_usage_mb():.0f}MB")
                    if (batch_num + 1) % BROWSER_RESTART_INTERVAL == 0 and batch_num < len(batches) - 1:
                        logger.info("[BROWSER] Restarting...")
                        await browser.close()
                        await asyncio.sleep(2)
                        browser = await self._create_browser(p)
                    if get_memory_usage_mb() > MEMORY_LIMIT_MB:
                        logger.warning(f"[MEMORY] Limit exceeded. Restarting browser...")
                        await browser.close()
                        gc.collect()
                        browser = await self._create_browser(p)
                    emit("batch_complete", {"batch": batch_num + 1, "duration_seconds": int((datetime.now() - batch_start).total_seconds()), "memory_mb": round(get_memory_usage_mb(), 1)})
                    logger.info(f"[BATCH {batch_num + 1}/{len(batches)}] Complete. Memory: {get_memory_usage_mb():.0f}MB")
            else:
                chunks = self._chunk_list(all_zones, (len(all_zones) + PHASE1_WORKERS - 1) // PHASE1_WORKERS)
                await asyncio.gather(*[self.phase1_worker(browser, i, chunks[i]) for i in range(len(chunks))])
                pids = self.data.get_all_ids()
                if pids:
                    emit("extraction_start", {"total": len(pids), "global_percent": 40})
                    chunks = self._chunk_list(pids, (len(pids) + PHASE2_WORKERS - 1) // PHASE2_WORKERS)
                    await asyncio.gather(*[self.phase2_worker(browser, i, chunks[i], len(pids)) for i in range(len(chunks))])
                all_websites = [(p, b["website"]) for p, b in self.data.get_businesses_dict().items() if b.get("website") and not b.get("email")]
            await browser.close()
        if all_websites:
            emit("status", {"message": f"Phase 3: Emails ({len(all_websites)} sites)...", "global_percent": 80})
            await self.extract_emails_advanced(all_websites)
        businesses = self.data.get_all_businesses()
        emit("complete", {"stats": {"total": len(businesses), "with_phone": sum(1 for b in businesses if b.get("phone")), "with_email": sum(1 for b in businesses if b.get("email")), "with_website": sum(1 for b in businesses if b.get("website")), "duration_seconds": int((datetime.now() - self.start_time).total_seconds()), "redis_used": self.data.use_redis}, "businesses": businesses})
        logger.info(f"[COMPLETE] {len(businesses)} businesses in {(datetime.now() - self.start_time).total_seconds():.0f}s")

async def main():
    if len(sys.argv) < 3:
        emit("error", {"message": "Usage: python script.py 'activity' 'city' [grid_size]"})
        sys.exit(1)
    scraper = GMBScraperProduction(sys.argv[1], sys.argv[2], int(sys.argv[3]) if len(sys.argv) > 3 else None)
    await scraper.run()

if __name__ == "__main__":
    asyncio.run(main())
