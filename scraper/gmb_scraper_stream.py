#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GMB Scraper v15 - Version Production
=====================================
- Extraction SYSTEMATIQUE des details depuis chaque fiche GMB
- Telephone, website, adresse extraits de la fiche detaillee
- Extraction emails depuis les sites web en parallele
- Zone de recherche precise par arrondissements/quartiers
- Streaming NDJSON temps reel
"""

import asyncio
import json
import re
import math
import sys
import io
import aiohttp
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Tuple, Set
from datetime import datetime
from urllib.parse import quote, urlparse
from collections import defaultdict

# Force UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

try:
    from playwright.async_api import async_playwright, Page, Browser, BrowserContext
except ImportError:
    print(json.dumps({"type": "error", "message": "Playwright requis: pip install playwright && playwright install chromium"}), flush=True)
    sys.exit(1)


# ============================================================================
# CONFIGURATION PRECISE DES VILLES
# ============================================================================

# Paris: grille dense sur les arrondissements, pas de banlieue
PARIS_ZONES = [
    # Arrondissements centraux (1-4)
    (48.8606, 2.3376),  # 1er - Les Halles
    (48.8663, 2.3411),  # 2eme - Bourse
    (48.8638, 2.3611),  # 3eme - Marais Nord
    (48.8546, 2.3571),  # 4eme - Marais Sud
    # Rive Gauche (5-7)
    (48.8462, 2.3451),  # 5eme - Quartier Latin
    (48.8499, 2.3324),  # 6eme - Saint-Germain
    (48.8566, 2.3150),  # 7eme - Tour Eiffel
    # Rive Droite Ouest (8-9, 16-17)
    (48.8744, 2.3118),  # 8eme - Champs-Elysees
    (48.8769, 2.3372),  # 9eme - Opera
    (48.8637, 2.2769),  # 16eme - Trocadero
    (48.8867, 2.3166),  # 17eme - Batignolles
    # Nord (10, 18-19)
    (48.8767, 2.3599),  # 10eme - Gare du Nord
    (48.8918, 2.3444),  # 18eme - Montmartre
    (48.8839, 2.3822),  # 19eme - Buttes-Chaumont
    # Est (11-12, 20)
    (48.8589, 2.3781),  # 11eme - Bastille
    (48.8413, 2.3876),  # 12eme - Nation
    (48.8640, 2.3982),  # 20eme - Belleville
    # Sud (13-15)
    (48.8322, 2.3561),  # 13eme - Place d'Italie
    (48.8331, 2.3266),  # 14eme - Montparnasse
    (48.8421, 2.2920),  # 15eme - Convention
]

CITY_DATA = {
    "paris": {"zones": PARIS_ZONES, "radius_km": 2},
    "lyon": {"center": (45.7640, 4.8357), "radius_km": 5},
    "marseille": {"center": (43.2965, 5.3698), "radius_km": 6},
    "toulouse": {"center": (43.6047, 1.4442), "radius_km": 5},
    "nice": {"center": (43.7102, 7.2620), "radius_km": 4},
    "nantes": {"center": (47.2184, -1.5536), "radius_km": 5},
    "bordeaux": {"center": (44.8378, -0.5792), "radius_km": 5},
    "lille": {"center": (50.6292, 3.0573), "radius_km": 4},
    "strasbourg": {"center": (48.5734, 7.7521), "radius_km": 4},
    "montpellier": {"center": (43.6108, 3.8767), "radius_km": 4},
    "rennes": {"center": (48.1173, -1.6778), "radius_km": 4},
    "reims": {"center": (49.2583, 4.0317), "radius_km": 4},
    "toulon": {"center": (43.1242, 5.9280), "radius_km": 4},
    "grenoble": {"center": (45.1885, 5.7245), "radius_km": 4},
    "dijon": {"center": (47.3220, 5.0415), "radius_km": 4},
    "angers": {"center": (47.4784, -0.5632), "radius_km": 4},
    "nimes": {"center": (43.8367, 4.3601), "radius_km": 4},
    "aix-en-provence": {"center": (43.5297, 5.4474), "radius_km": 4},
    "clermont-ferrand": {"center": (45.7772, 3.0870), "radius_km": 4},
    "le havre": {"center": (49.4944, 0.1079), "radius_km": 4},
    "rouen": {"center": (49.4432, 1.0999), "radius_km": 4},
    "brest": {"center": (48.3904, -4.4861), "radius_km": 4},
    "tours": {"center": (47.3941, 0.6848), "radius_km": 4},
    "amiens": {"center": (49.8941, 2.2958), "radius_km": 4},
    "limoges": {"center": (45.8336, 1.2611), "radius_km": 4},
    "metz": {"center": (49.1193, 6.1757), "radius_km": 4},
    "nancy": {"center": (48.6921, 6.1844), "radius_km": 4},
}

INVALID_PATTERNS = [
    r'^[\d\.,\s]+$',
    r'^[a-f0-9]{20,}$',
    r'^0x[a-f0-9]+',
    r'^ChIJ',
    r'^0ahUKE',
    r'^results?$',
    r'^https?://',
    r'^\d+\.\d+$',
]

EMAIL_BLACKLIST = [
    'google', 'gstatic', 'schema', 'sentry', 'cloudflare',
    'facebook', 'twitter', 'instagram', 'test@', 'demo@',
    'noreply', 'no-reply', 'example', 'googleapis', 'w3.org',
    'googleusercontent', 'wixpress', 'squarespace', 'wordpress',
    'mailchimp', 'sendgrid', 'hubspot', 'protection'
]


def emit(event_type: str, data: dict):
    """Emet un evenement NDJSON"""
    event = {"type": event_type, "timestamp": datetime.now().isoformat(), **data}
    print(json.dumps(event, ensure_ascii=False), flush=True)


@dataclass
class Business:
    name: str = ""
    place_id: str = ""
    address: str = ""
    phone: str = ""
    phone_clean: str = ""
    email: str = ""
    website: str = ""
    rating: Optional[float] = None
    review_count: int = 0
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    category: str = ""
    google_maps_url: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)

    def is_valid(self) -> bool:
        if not self.name or len(self.name) < 2:
            return False
        for pattern in INVALID_PATTERNS:
            if re.match(pattern, self.name, re.I):
                return False
        if not re.search(r'[a-zA-Z\u00e0\u00e2\u00e4\u00e9\u00e8\u00ea\u00eb\u00ef\u00ee\u00f4\u00f9\u00fb\u00fc\u00ff\u0153\u00e6\u00c0\u00c2\u00c4\u00c9\u00c8\u00ca\u00cb\u00cf\u00ce\u00d4\u00d9\u00db\u00dc\u0178\u0152\u00c6]', self.name):
            return False
        return True


class GMBScraperPro:
    """Scraper GMB professionnel avec extraction complete"""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

        self.businesses: Dict[str, Business] = {}
        self.seen_names: Set[str] = set()
        self.filtered_out = 0
        self.start_time: Optional[datetime] = None

        self.stats = {
            'total': 0,
            'with_phone': 0,
            'with_website': 0,
            'with_email': 0,
            'with_address': 0
        }

    def _normalize_name(self, name: str) -> str:
        return re.sub(r'[\s\-\.\'\"]+',' ', name.lower().strip())

    def _clean_phone(self, phone: str) -> str:
        return re.sub(r'[\s\.\-\(\)]', '', phone)

    def _is_valid_email(self, email: str) -> bool:
        if not email or '@' not in email:
            return False
        email_lower = email.lower()
        if any(x in email_lower for x in EMAIL_BLACKLIST):
            return False
        if not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', email):
            return False
        return True

    def _get_search_zones(self, city: str, grid_size: int) -> List[Tuple[float, float]]:
        city_key = city.lower().strip().replace('-', ' ').replace('_', ' ')

        if 'paris' in city_key:
            if grid_size <= 2:
                return PARIS_ZONES[:9]
            elif grid_size <= 4:
                return PARIS_ZONES[:16]
            else:
                return PARIS_ZONES

        city_data = CITY_DATA.get(city_key, {"center": (48.8566, 2.3522), "radius_km": 5})

        if "zones" in city_data:
            return city_data["zones"]

        center_lat, center_lng = city_data["center"]
        radius_km = city_data["radius_km"]

        lat_delta = radius_km / 111.0
        lng_delta = radius_km / (111.0 * math.cos(math.radians(center_lat)))

        points = []
        actual_grid = min(grid_size, 5)

        for i in range(actual_grid):
            for j in range(actual_grid):
                if actual_grid > 1:
                    lat_norm = (i / (actual_grid - 1)) * 2 - 1
                    lng_norm = (j / (actual_grid - 1)) * 2 - 1
                else:
                    lat_norm = lng_norm = 0
                points.append((
                    center_lat + lat_norm * lat_delta,
                    center_lng + lng_norm * lng_delta
                ))

        return points

    async def _init_browser(self):
        playwright = await async_playwright().start()

        self.browser = await playwright.chromium.launch(
            headless=self.headless,
            args=['--disable-blink-features=AutomationControlled']
        )

        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="fr-FR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        self.page = await self.context.new_page()
        await self._handle_consent()

    async def _handle_consent(self):
        try:
            await self.page.goto("https://www.google.com/maps", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(1.5)

            for selector in [
                'button:has-text("Tout accepter")',
                'button:has-text("Accept all")',
                'button:has-text("Accepter tout")'
            ]:
                try:
                    btn = await self.page.wait_for_selector(selector, timeout=3000)
                    if btn:
                        await btn.click()
                        await asyncio.sleep(1)
                        return
                except:
                    continue
        except Exception as e:
            emit("warning", {"message": f"Consentement non trouve: {e}"})

    async def _scroll_results(self) -> int:
        links_found = set()
        stable_count = 0
        last_count = 0

        for _ in range(20):
            await self.page.evaluate('''
                const feed = document.querySelector('div[role="feed"]');
                if (feed) feed.scrollBy(0, 800);
            ''')
            await asyncio.sleep(0.3)

            new_links = await self.page.evaluate('''
                () => {
                    const links = [];
                    document.querySelectorAll('a[href*="/maps/place/"]').forEach(a => {
                        const href = a.getAttribute('href');
                        const name = a.getAttribute('aria-label');
                        if (href && name && name.length > 2) {
                            links.push({href, name});
                        }
                    });
                    return links;
                }
            ''')

            for link in new_links:
                key = self._normalize_name(link['name'])
                if key not in self.seen_names:
                    links_found.add((link['name'], link['href']))

            end_reached = await self.page.evaluate('''
                () => {
                    const feed = document.querySelector('div[role="feed"]');
                    if (!feed) return false;
                    const text = feed.innerText || '';
                    return text.includes("Vous avez fait le tour") ||
                           text.includes("plus de r\u00e9sultats") ||
                           text.includes("No more results");
                }
            ''')

            if end_reached:
                break

            if len(links_found) == last_count:
                stable_count += 1
                if stable_count >= 3:
                    break
            else:
                stable_count = 0
            last_count = len(links_found)

        return len(links_found)

    async def _extract_business_details(self, name: str, url: str) -> Optional[Business]:
        try:
            await self.page.goto(url, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(1.5)

            biz = Business(name=name, google_maps_url=url)

            place_match = re.search(r'!1s(0x[a-f0-9]+:0x[a-f0-9]+)', url)
            if place_match:
                biz.place_id = place_match[1]
            else:
                place_match = re.search(r'!1s(ChIJ[A-Za-z0-9_-]+)', url)
                if place_match:
                    biz.place_id = place_match[1]

            coord_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', url)
            if coord_match:
                biz.latitude = float(coord_match[1])
                biz.longitude = float(coord_match[2])

            try:
                phone_buttons = await self.page.query_selector_all('button[data-item-id*="phone"]')
                for btn in phone_buttons:
                    aria = await btn.get_attribute('aria-label')
                    if aria:
                        phone_match = re.search(r'(0[1-9](?:[\s\.]?\d{2}){4}|\+33[\s\.]?\d(?:[\s\.]?\d{2}){4})', aria)
                        if phone_match:
                            biz.phone = phone_match[1]
                            biz.phone_clean = self._clean_phone(biz.phone)
                            break
            except:
                pass

            if not biz.phone:
                try:
                    content = await self.page.content()
                    phone_match = re.search(r'(?:tel:|phone["\s:]+)(0[1-9](?:[\s\.]?\d{2}){4})', content, re.I)
                    if phone_match:
                        biz.phone = phone_match[1]
                        biz.phone_clean = self._clean_phone(biz.phone)
                except:
                    pass

            try:
                website_el = await self.page.query_selector('a[data-item-id="authority"]')
                if website_el:
                    href = await website_el.get_attribute('href')
                    if href and not any(x in href for x in ['google.com', 'gstatic']):
                        biz.website = href
            except:
                pass

            if not biz.website:
                try:
                    links = await self.page.query_selector_all('a[href^="http"]')
                    for link in links:
                        href = await link.get_attribute('href')
                        if href and not any(x in href for x in ['google', 'gstatic', 'facebook.com', 'instagram.com', 'twitter.com']):
                            domain = urlparse(href).netloc
                            if domain and '.' in domain:
                                biz.website = href
                                break
                except:
                    pass

            try:
                addr_btn = await self.page.query_selector('button[data-item-id="address"]')
                if addr_btn:
                    aria = await addr_btn.get_attribute('aria-label')
                    if aria:
                        biz.address = aria.replace('Adresse:', '').replace('Address:', '').strip()
            except:
                pass

            try:
                cat_el = await self.page.query_selector('button[jsaction*="category"]')
                if cat_el:
                    text = await cat_el.inner_text()
                    if text:
                        biz.category = text.strip()
            except:
                pass

            if not biz.category:
                try:
                    spans = await self.page.query_selector_all('span')
                    for span in spans[:30]:
                        text = await span.inner_text()
                        if text and 'agence' in text.lower() or 'immobili' in text.lower():
                            biz.category = text.strip()[:100]
                            break
                except:
                    pass

            try:
                rating_el = await self.page.query_selector('div[role="img"][aria-label*="\u00e9toile"]')
                if rating_el:
                    aria = await rating_el.get_attribute('aria-label')
                    if aria:
                        rating_match = re.search(r'(\d)[,\.](\d)', aria)
                        if rating_match:
                            biz.rating = float(f"{rating_match[1]}.{rating_match[2]}")
                        review_match = re.search(r'(\d[\d\s\u202f]*)\s*avis', aria, re.I)
                        if review_match:
                            biz.review_count = int(re.sub(r'[\s\u202f]', '', review_match[1]))
            except:
                pass

            return biz

        except Exception as e:
            emit("warning", {"message": f"Erreur extraction {name}: {str(e)[:100]}"})
            return None

    async def _extract_emails_from_websites(self, businesses: List[Business]) -> Dict[str, str]:
        websites_to_check = [
            (b.place_id or b.name, b.website)
            for b in businesses
            if b.website and not b.email
        ]

        if not websites_to_check:
            return {}

        emit("email_extraction_start", {
            "total_sites": len(websites_to_check),
            "message": f"Extraction emails depuis {len(websites_to_check)} sites web..."
        })

        emails_found = {}

        async def fetch_email(session: aiohttp.ClientSession, key: str, url: str) -> Tuple[str, Optional[str]]:
            try:
                if not url.startswith('http'):
                    url = 'https://' + url

                async with session.get(url, timeout=aiohttp.ClientTimeout(total=10), ssl=False) as resp:
                    if resp.status != 200:
                        return (key, None)

                    html = await resp.text()

                    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', html)

                    for email in emails:
                        if self._is_valid_email(email):
                            return (key, email)

                    contact_links = re.findall(r'href=["\']([^"\']*contact[^"\']*)["\']', html, re.I)
                    for contact_link in contact_links[:2]:
                        try:
                            if contact_link.startswith('/'):
                                base = urlparse(url)
                                contact_url = f"{base.scheme}://{base.netloc}{contact_link}"
                            elif contact_link.startswith('http'):
                                contact_url = contact_link
                            else:
                                continue

                            async with session.get(contact_url, timeout=aiohttp.ClientTimeout(total=8), ssl=False) as contact_resp:
                                if contact_resp.status == 200:
                                    contact_html = await contact_resp.text()
                                    contact_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', contact_html)
                                    for email in contact_emails:
                                        if self._is_valid_email(email):
                                            return (key, email)
                        except:
                            continue

                    return (key, None)

            except Exception:
                return (key, None)

        connector = aiohttp.TCPConnector(limit=10, force_close=True)
        async with aiohttp.ClientSession(
            connector=connector,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
        ) as session:

            batch_size = 10
            for i in range(0, len(websites_to_check), batch_size):
                batch = websites_to_check[i:i+batch_size]
                tasks = [fetch_email(session, key, url) for key, url in batch]
                results = await asyncio.gather(*tasks, return_exceptions=True)

                for result in results:
                    if isinstance(result, tuple) and result[1]:
                        key, email = result
                        emails_found[key] = email
                        emit("email_found", {
                            "business_key": key,
                            "email": email,
                            "total_found": len(emails_found)
                        })

                emit("email_extraction_progress", {
                    "processed": min(i + batch_size, len(websites_to_check)),
                    "total": len(websites_to_check),
                    "found": len(emails_found)
                })

        return emails_found

    def _add_business(self, biz: Business) -> bool:
        if not biz.is_valid():
            self.filtered_out += 1
            return False

        norm_name = self._normalize_name(biz.name)

        if norm_name in self.seen_names:
            return False

        key = biz.place_id if biz.place_id else f"name_{norm_name}"

        if key in self.businesses:
            existing = self.businesses[key]
            updated = False
            if biz.phone and not existing.phone:
                existing.phone = biz.phone
                existing.phone_clean = biz.phone_clean
                updated = True
            if biz.website and not existing.website:
                existing.website = biz.website
                updated = True
            if biz.email and not existing.email:
                existing.email = biz.email
                updated = True
            if biz.address and not existing.address:
                existing.address = biz.address
                updated = True
            return updated

        self.businesses[key] = biz
        self.seen_names.add(norm_name)

        self.stats['total'] = len(self.businesses)
        if biz.phone:
            self.stats['with_phone'] += 1
        if biz.website:
            self.stats['with_website'] += 1
        if biz.email:
            self.stats['with_email'] += 1
        if biz.address:
            self.stats['with_address'] += 1

        return True

    async def _search_zone(self, query: str, lat: float, lng: float, zone_num: int, total_zones: int) -> int:
        initial_count = len(self.businesses)

        search_url = f"https://www.google.com/maps/search/{quote(query)}/@{lat},{lng},15z"

        try:
            await self.page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(1.5)
        except Exception as e:
            emit("warning", {"message": f"Zone {zone_num}: erreur navigation - {str(e)[:50]}"})
            return 0

        await self._scroll_results()

        links = await self.page.evaluate('''
            () => {
                const results = [];
                document.querySelectorAll('a[href*="/maps/place/"]').forEach(a => {
                    const href = a.getAttribute('href');
                    const name = a.getAttribute('aria-label');
                    if (href && name && name.length > 2) {
                        results.push({name, href});
                    }
                });
                return results;
            }
        ''')

        unique_links = []
        for link in links:
            norm = self._normalize_name(link['name'])
            if norm not in self.seen_names:
                unique_links.append(link)

        emit("zone_links", {
            "zone": zone_num,
            "total_zones": total_zones,
            "links_found": len(links),
            "unique_new": len(unique_links)
        })

        for idx, link in enumerate(unique_links):
            biz = await self._extract_business_details(link['name'], link['href'])

            if biz and self._add_business(biz):
                emit("business", {
                    **biz.to_dict(),
                    "zone": zone_num,
                    "index_in_zone": idx + 1
                })

                emit("stats_update", {
                    "total": self.stats['total'],
                    "with_phone": self.stats['with_phone'],
                    "with_website": self.stats['with_website'],
                    "with_email": self.stats['with_email'],
                    "with_address": self.stats['with_address']
                })

            await asyncio.sleep(0.3)

        return len(self.businesses) - initial_count

    async def scrape(self, business_type: str, city: str, grid_size: int = 4) -> List[Business]:
        self.start_time = datetime.now()

        zones = self._get_search_zones(city, grid_size)
        total_zones = len(zones)

        emit("start", {
            "activity": business_type,
            "city": city,
            "grid_size": grid_size,
            "total_zones": total_zones,
            "zones": [(round(lat, 4), round(lng, 4)) for lat, lng in zones],
            "version": "v15-pro"
        })

        try:
            emit("status", {"message": "Initialisation du navigateur..."})
            await self._init_browser()

            for zone_num, (lat, lng) in enumerate(zones, 1):
                emit("zone_start", {
                    "zone": zone_num,
                    "total_zones": total_zones,
                    "lat": round(lat, 5),
                    "lng": round(lng, 5)
                })

                new_count = await self._search_zone(
                    f"{business_type} {city}",
                    lat, lng,
                    zone_num, total_zones
                )

                emit("zone_complete", {
                    "zone": zone_num,
                    "total_zones": total_zones,
                    "new_businesses": new_count,
                    "total_businesses": len(self.businesses),
                    "percent": round(100 * zone_num / total_zones, 1)
                })

            businesses_list = list(self.businesses.values())

            if any(b.website for b in businesses_list):
                emit("status", {"message": "Phase 2: Extraction des emails depuis les sites web..."})

                emails_found = await self._extract_emails_from_websites(businesses_list)

                for key, email in emails_found.items():
                    if key in self.businesses:
                        self.businesses[key].email = email
                        self.stats['with_email'] += 1
                    else:
                        for biz_key, biz in self.businesses.items():
                            if key == biz.name and not biz.email:
                                biz.email = email
                                self.stats['with_email'] += 1
                                break

            elapsed = (datetime.now() - self.start_time).total_seconds()
            businesses_list = list(self.businesses.values())

            final_stats = {
                "total": len(businesses_list),
                "with_phone": sum(1 for b in businesses_list if b.phone),
                "with_website": sum(1 for b in businesses_list if b.website),
                "with_email": sum(1 for b in businesses_list if b.email),
                "with_address": sum(1 for b in businesses_list if b.address),
                "with_category": sum(1 for b in businesses_list if b.category),
                "with_rating": sum(1 for b in businesses_list if b.rating),
                "filtered_out": self.filtered_out,
                "duration_seconds": round(elapsed, 1)
            }

            emit("complete", {
                "stats": final_stats,
                "businesses": [b.to_dict() for b in businesses_list]
            })

            return businesses_list

        except Exception as e:
            emit("error", {"message": f"Erreur fatale: {str(e)}"})
            return list(self.businesses.values())

        finally:
            if self.browser:
                await self.browser.close()


async def main():
    if len(sys.argv) < 3:
        emit("error", {"message": "Usage: python gmb_scraper_stream.py <activity> <city> [grid_size]"})
        sys.exit(1)

    activity = sys.argv[1]
    city = sys.argv[2]
    grid_size = int(sys.argv[3]) if len(sys.argv) > 3 else 4

    scraper = GMBScraperPro(headless=True)
    await scraper.scrape(activity, city, grid_size)


if __name__ == "__main__":
    asyncio.run(main())
