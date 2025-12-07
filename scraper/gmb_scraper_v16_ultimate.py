#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GMB Scraper v16 ULTIMATE - API Interception Edition
====================================================
Version professionnelle exploitant les APIs internes de Google Maps:
- Interception des reponses /maps/preview/place pour extraction directe
- Geocodage dynamique Nominatim pour TOUTES les villes
- Extraction parallele ultra-rapide
- Streaming NDJSON temps reel

Cette version est 3-5x plus rapide que le parsing DOM classique.
"""

import asyncio
import json
import re
import math
import sys
import io
import aiohttp
from dataclasses import dataclass, asdict, field
from typing import Optional, List, Dict, Tuple, Set, Any
from datetime import datetime
from urllib.parse import quote, urlparse, unquote
from collections import defaultdict

# Force UTF-8
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

try:
    from playwright.async_api import async_playwright, Page, Browser, BrowserContext, Response
except ImportError:
    print(json.dumps({"type": "error", "message": "Playwright requis: pip install playwright && playwright install chromium"}), flush=True)
    sys.exit(1)


# ============================================================================
# CONFIGURATION
# ============================================================================

VERSION = "v16-ultimate"

# Patterns pour filtrer les noms invalides
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

# Blacklist pour emails
EMAIL_BLACKLIST = [
    'google', 'gstatic', 'schema', 'sentry', 'cloudflare',
    'facebook', 'twitter', 'instagram', 'test@', 'demo@',
    'noreply', 'no-reply', 'example', 'googleapis', 'w3.org',
    'googleusercontent', 'wixpress', 'squarespace', 'wordpress',
    'mailchimp', 'sendgrid', 'hubspot', 'protection'
]

# Cache geocodage
_GEOCODE_CACHE = {}


def emit(event_type: str, data: dict):
    """Emet un evenement NDJSON"""
    event = {"type": event_type, "timestamp": datetime.now().isoformat(), **data}
    print(json.dumps(event, ensure_ascii=False), flush=True)


async def geocode_city(city_name: str) -> Tuple[float, float, str]:
    """
    Geocode une ville via Nominatim (OpenStreetMap)
    Retourne (latitude, longitude, display_name)
    """
    cache_key = city_name.lower().strip()
    if cache_key in _GEOCODE_CACHE:
        return _GEOCODE_CACHE[cache_key]

    try:
        query = f"{city_name}, France"
        url = f"https://nominatim.openstreetmap.org/search?q={quote(query)}&format=json&limit=1&countrycodes=fr"

        async with aiohttp.ClientSession() as session:
            async with session.get(
                url,
                headers={'User-Agent': 'GMBScraperUltimate/16.0'},
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data and len(data) > 0:
                        result = (
                            float(data[0]['lat']),
                            float(data[0]['lon']),
                            data[0].get('display_name', city_name)
                        )
                        _GEOCODE_CACHE[cache_key] = result
                        return result
    except Exception as e:
        emit("warning", {"message": f"Geocodage echoue pour {city_name}: {str(e)[:50]}"})

    # Fallback Paris
    return (48.8566, 2.3522, "Paris, France")


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
    hours: str = ""

    def to_dict(self) -> Dict:
        return asdict(self)

    def is_valid(self) -> bool:
        if not self.name or len(self.name) < 2:
            return False
        for pattern in INVALID_PATTERNS:
            if re.match(pattern, self.name, re.I):
                return False
        if not re.search(r'[a-zA-Z\u00e0-\u00ff]', self.name):
            return False
        return True


class GMBScraperUltimate:
    """Scraper GMB Ultimate avec interception API"""

    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

        self.businesses: Dict[str, Business] = {}
        self.seen_names: Set[str] = set()
        self.filtered_out = 0
        self.start_time: Optional[datetime] = None

        # Donnees interceptees
        self.intercepted_data: Dict[str, Dict] = {}
        self.place_data_queue: asyncio.Queue = asyncio.Queue()

        # Stats temps reel
        self.stats = {
            'total': 0,
            'with_phone': 0,
            'with_website': 0,
            'with_email': 0,
            'with_address': 0
        }

    def _normalize_name(self, name: str) -> str:
        return re.sub(r'[\s\-\.\'\"]+', ' ', name.lower().strip())

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

    async def _handle_response(self, response: Response):
        """Intercepte les reponses API pour extraction directe"""
        url = response.url

        try:
            # Intercepter /maps/preview/place - Contient toutes les donnees d'une fiche
            if '/maps/preview/place' in url:
                try:
                    body = await response.text()
                    await self._parse_place_response(body, url)
                except:
                    pass

            # Intercepter /search?tbm=map - Liste des resultats
            elif '/search' in url and 'tbm=map' in url:
                try:
                    body = await response.text()
                    await self._parse_search_response(body)
                except:
                    pass

        except Exception as e:
            pass  # Ignorer les erreurs d'interception

    async def _parse_place_response(self, body: str, url: str):
        """Parse la reponse /maps/preview/place pour extraire les donnees"""
        try:
            # Extraire les telephones
            phones = re.findall(r'"(0[1-9](?:[\s\.]?\d{2}){4})"', body)
            phones += re.findall(r'"(\+33[\s\.]?\d(?:[\s\.]?\d{2}){4})"', body)

            # Extraire les websites (pas google/gstatic)
            websites = re.findall(r'"(https?://(?!.*google|.*gstatic)[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}[^"]*)"', body)

            # Extraire les Place IDs
            place_ids = re.findall(r'"(ChIJ[A-Za-z0-9_-]{20,})"', body)
            place_ids += re.findall(r'(0x[a-f0-9]+:0x[a-f0-9]+)', body)

            # Extraire coordonnees
            coords = re.findall(r'\[(-?\d+\.\d{5,}),\s*(-?\d+\.\d{5,})\]', body)

            if phones or websites:
                data = {
                    'phones': list(set(phones))[:3],
                    'websites': [w for w in websites if len(w) < 200][:3],
                    'place_ids': list(set(place_ids))[:5],
                    'coords': coords[:3]
                }

                # Stocker pour utilisation ulterieure
                for pid in data['place_ids']:
                    self.intercepted_data[pid] = data

                emit("api_intercept", {
                    "type": "place",
                    "phones": len(data['phones']),
                    "websites": len(data['websites']),
                    "place_ids": len(data['place_ids'])
                })

        except Exception as e:
            pass

    async def _parse_search_response(self, body: str):
        """Parse la reponse de recherche"""
        try:
            # Compter les resultats
            place_ids = re.findall(r'"(ChIJ[A-Za-z0-9_-]{20,})"', body)
            if place_ids:
                emit("api_intercept", {
                    "type": "search",
                    "results": len(set(place_ids))
                })
        except:
            pass

    async def _generate_search_zones(self, city: str, grid_size: int) -> List[Tuple[float, float]]:
        """Genere une grille de recherche TOUJOURS via geocodage"""
        emit("status", {"message": f"Geocodage de '{city}'..."})

        lat, lng, display_name = await geocode_city(city)

        emit("status", {"message": f"Ville: {display_name.split(',')[0]} ({lat:.4f}, {lng:.4f})"})

        # Calculer le rayon en fonction de la taille de la grille
        # Plus la grille est grande, plus on couvre de surface
        base_radius = 3.0  # km
        radius_km = base_radius * (grid_size / 3)

        lat_delta = radius_km / 111.0
        lng_delta = radius_km / (111.0 * math.cos(math.radians(lat)))

        points = []
        actual_grid = min(grid_size, 5)  # Max 5x5

        for i in range(actual_grid):
            for j in range(actual_grid):
                if actual_grid > 1:
                    lat_norm = (i / (actual_grid - 1)) * 2 - 1
                    lng_norm = (j / (actual_grid - 1)) * 2 - 1
                else:
                    lat_norm = lng_norm = 0

                points.append((
                    lat + lat_norm * lat_delta,
                    lng + lng_norm * lng_delta
                ))

        return points

    async def _init_browser(self):
        """Initialise le navigateur avec interception"""
        playwright = await async_playwright().start()

        self.browser = await playwright.chromium.launch(
            headless=self.headless,
            args=[
                '--disable-blink-features=AutomationControlled',
                '--disable-dev-shm-usage',
                '--no-sandbox'
            ]
        )

        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="fr-FR",
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )

        self.page = await self.context.new_page()

        # Activer l'interception des reponses
        self.page.on("response", self._handle_response)

        # Accepter les cookies
        await self._handle_consent()

    async def _handle_consent(self):
        """Gere le popup de consentement Google"""
        try:
            await self.page.goto("https://www.google.com/maps", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(2)

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
            emit("warning", {"message": f"Consentement: {str(e)[:50]}"})

    async def _scroll_and_collect_links(self) -> List[Dict[str, str]]:
        """Scrolle et collecte tous les liens des resultats"""
        all_links = []
        seen_hrefs = set()
        stable_count = 0
        last_count = 0

        for scroll_num in range(25):  # Max 25 scrolls
            # Scroll
            await self.page.evaluate('''
                const feed = document.querySelector('div[role="feed"]');
                if (feed) feed.scrollBy(0, 800);
            ''')
            await asyncio.sleep(0.4)

            # Collecter les liens
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

            for link in links:
                if link['href'] not in seen_hrefs:
                    seen_hrefs.add(link['href'])
                    all_links.append(link)

            # Verifier fin de liste
            end_reached = await self.page.evaluate('''
                () => {
                    const feed = document.querySelector('div[role="feed"]');
                    if (!feed) return false;
                    const text = feed.innerText || '';
                    return text.includes("Vous avez fait le tour") ||
                           text.includes("plus de résultats") ||
                           text.includes("No more results");
                }
            ''')

            if end_reached:
                break

            # Stabilite
            if len(all_links) == last_count:
                stable_count += 1
                if stable_count >= 4:
                    break
            else:
                stable_count = 0
            last_count = len(all_links)

        return all_links

    async def _extract_business_fast(self, name: str, url: str) -> Optional[Business]:
        """Extraction rapide combinant DOM + donnees interceptees"""
        try:
            await self.page.goto(url, wait_until="domcontentloaded", timeout=15000)
            await asyncio.sleep(1)

            biz = Business(name=name, google_maps_url=url)

            # Place ID depuis l'URL
            place_match = re.search(r'!1s(0x[a-f0-9]+:0x[a-f0-9]+)', url)
            if place_match:
                biz.place_id = place_match[1]

                # Verifier si on a des donnees interceptees
                if biz.place_id in self.intercepted_data:
                    intercepted = self.intercepted_data[biz.place_id]
                    if intercepted.get('phones'):
                        biz.phone = intercepted['phones'][0]
                        biz.phone_clean = self._clean_phone(biz.phone)
                    if intercepted.get('websites'):
                        biz.website = intercepted['websites'][0]

            if not biz.place_id:
                place_match = re.search(r'!1s(ChIJ[A-Za-z0-9_-]+)', url)
                if place_match:
                    biz.place_id = place_match[1]

            # Coordonnees depuis URL
            coord_match = re.search(r'@(-?\d+\.\d+),(-?\d+\.\d+)', url)
            if coord_match:
                biz.latitude = float(coord_match[1])
                biz.longitude = float(coord_match[2])

            # ===== TELEPHONE (si pas deja intercepte) =====
            if not biz.phone:
                try:
                    phone_btns = await self.page.query_selector_all('button[data-item-id*="phone"]')
                    for btn in phone_btns:
                        aria = await btn.get_attribute('aria-label')
                        if aria:
                            phone_match = re.search(r'(0[1-9](?:[\s\.]?\d{2}){4}|\+33[\s\.]?\d(?:[\s\.]?\d{2}){4})', aria)
                            if phone_match:
                                biz.phone = phone_match[1]
                                biz.phone_clean = self._clean_phone(biz.phone)
                                break
                except:
                    pass

            # ===== WEBSITE (si pas deja intercepte) =====
            if not biz.website:
                try:
                    website_el = await self.page.query_selector('a[data-item-id="authority"]')
                    if website_el:
                        href = await website_el.get_attribute('href')
                        if href and 'google.com' not in href:
                            biz.website = href
                except:
                    pass

            # ===== ADRESSE =====
            try:
                addr_btn = await self.page.query_selector('button[data-item-id="address"]')
                if addr_btn:
                    aria = await addr_btn.get_attribute('aria-label')
                    if aria:
                        biz.address = aria.replace('Adresse:', '').replace('Address:', '').strip()
            except:
                pass

            # ===== CATEGORIE =====
            try:
                cat_el = await self.page.query_selector('button[jsaction*="category"]')
                if cat_el:
                    text = await cat_el.inner_text()
                    if text:
                        biz.category = text.strip()
            except:
                pass

            # ===== RATING =====
            try:
                rating_el = await self.page.query_selector('div[role="img"][aria-label*="toile"]')
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
            return None

    def _add_business(self, biz: Business) -> bool:
        """Ajoute un business si valide et unique"""
        if not biz.is_valid():
            self.filtered_out += 1
            return False

        norm_name = self._normalize_name(biz.name)

        if norm_name in self.seen_names:
            return False

        key = biz.place_id if biz.place_id else f"name_{norm_name}"

        if key in self.businesses:
            # Merge des donnees
            existing = self.businesses[key]
            if biz.phone and not existing.phone:
                existing.phone = biz.phone
                existing.phone_clean = biz.phone_clean
            if biz.website and not existing.website:
                existing.website = biz.website
            if biz.email and not existing.email:
                existing.email = biz.email
            if biz.address and not existing.address:
                existing.address = biz.address
            return False

        self.businesses[key] = biz
        self.seen_names.add(norm_name)

        # Stats
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

    async def _extract_emails_parallel(self, businesses: List[Business]) -> Dict[str, str]:
        """Extraction emails ultra-rapide en parallele"""
        websites = [(b.place_id or b.name, b.website) for b in businesses if b.website and not b.email]

        if not websites:
            return {}

        emit("email_extraction_start", {"total_sites": len(websites)})

        emails_found = {}

        async def fetch_email(session: aiohttp.ClientSession, key: str, url: str) -> Tuple[str, Optional[str]]:
            try:
                if not url.startswith('http'):
                    url = 'https://' + url

                async with session.get(url, timeout=aiohttp.ClientTimeout(total=8), ssl=False) as resp:
                    if resp.status != 200:
                        return (key, None)

                    html = await resp.text()
                    emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', html)

                    for email in emails:
                        if self._is_valid_email(email):
                            return (key, email)

                    # Page contact
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

                            async with session.get(contact_url, timeout=aiohttp.ClientTimeout(total=6), ssl=False) as contact_resp:
                                if contact_resp.status == 200:
                                    contact_html = await contact_resp.text()
                                    contact_emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', contact_html)
                                    for email in contact_emails:
                                        if self._is_valid_email(email):
                                            return (key, email)
                        except:
                            continue

                    return (key, None)
            except:
                return (key, None)

        connector = aiohttp.TCPConnector(limit=15, force_close=True)
        async with aiohttp.ClientSession(
            connector=connector,
            headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0'}
        ) as session:

            batch_size = 15
            for i in range(0, len(websites), batch_size):
                batch = websites[i:i+batch_size]
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
                    "processed": min(i + batch_size, len(websites)),
                    "total": len(websites),
                    "found": len(emails_found)
                })

        return emails_found

    async def _search_zone(self, query: str, lat: float, lng: float, zone_num: int, total_zones: int) -> int:
        """Recherche dans une zone avec interception API"""
        initial_count = len(self.businesses)

        search_url = f"https://www.google.com/maps/search/{quote(query)}/@{lat},{lng},15z"

        try:
            await self.page.goto(search_url, wait_until="domcontentloaded", timeout=20000)
            await asyncio.sleep(2)
        except Exception as e:
            emit("warning", {"message": f"Zone {zone_num}: navigation error"})
            return 0

        # Collecter les liens
        links = await self._scroll_and_collect_links()

        # Filtrer les nouveaux
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

        # Extraire les details
        for idx, link in enumerate(unique_links):
            biz = await self._extract_business_fast(link['name'], link['href'])

            if biz and self._add_business(biz):
                emit("business", {
                    **biz.to_dict(),
                    "zone": zone_num,
                    "index": idx + 1
                })

                emit("stats_update", self.stats)

            await asyncio.sleep(0.2)

        return len(self.businesses) - initial_count

    async def scrape(self, activity: str, city: str, grid_size: int = 4) -> List[Business]:
        """Lance le scraping ULTIMATE"""
        self.start_time = datetime.now()

        # TOUJOURS geocoder pour precision maximale
        zones = await self._generate_search_zones(city, grid_size)
        total_zones = len(zones)

        emit("start", {
            "activity": activity,
            "city": city,
            "grid_size": grid_size,
            "total_zones": total_zones,
            "version": VERSION
        })

        try:
            emit("status", {"message": "Initialisation navigateur avec interception API..."})
            await self._init_browser()

            # Phase 1: Scraping par zone
            for zone_num, (lat, lng) in enumerate(zones, 1):
                emit("zone_start", {
                    "zone": zone_num,
                    "total_zones": total_zones,
                    "lat": round(lat, 5),
                    "lng": round(lng, 5)
                })

                new_count = await self._search_zone(
                    f"{activity} {city}",
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

            # Phase 2: Extraction emails
            businesses_list = list(self.businesses.values())

            if any(b.website for b in businesses_list):
                emit("status", {"message": "Phase 2: Extraction emails ultra-rapide..."})

                emails = await self._extract_emails_parallel(businesses_list)

                for key, email in emails.items():
                    if key in self.businesses:
                        self.businesses[key].email = email
                        self.stats['with_email'] += 1

            # Stats finales
            elapsed = (datetime.now() - self.start_time).total_seconds()
            businesses_list = list(self.businesses.values())

            final_stats = {
                "total": len(businesses_list),
                "with_phone": sum(1 for b in businesses_list if b.phone),
                "with_website": sum(1 for b in businesses_list if b.website),
                "with_email": sum(1 for b in businesses_list if b.email),
                "with_address": sum(1 for b in businesses_list if b.address),
                "filtered_out": self.filtered_out,
                "api_intercepts": len(self.intercepted_data),
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
        emit("error", {"message": "Usage: python gmb_scraper_v16_ultimate.py <activity> <city> [grid_size]"})
        sys.exit(1)

    activity = sys.argv[1]
    city = sys.argv[2]
    grid_size = int(sys.argv[3]) if len(sys.argv) > 3 else 4

    scraper = GMBScraperUltimate(headless=True)
    await scraper.scrape(activity, city, grid_size)


if __name__ == "__main__":
    asyncio.run(main())
