#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GMB Scraper v14 - Version API avec streaming NDJSON + Email extraction
Utilise l'interception de l'API interne Google Maps pour extraction complete
Inclut: extraction d'emails depuis les sites web des entreprises
"""

import asyncio
import json
import re
import math
import sys
import io
import aiohttp
from aiohttp import ClientTimeout

# Force UTF-8 output pour eviter les problemes d'encodage
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')

from dataclasses import dataclass, asdict
from typing import Optional, List, Dict, Tuple, Set
from datetime import datetime
from urllib.parse import quote, urljoin, urlparse

try:
    from playwright.async_api import async_playwright, Page, Browser, BrowserContext
except ImportError:
    print(json.dumps({"type": "error", "message": "Playwright requis"}), flush=True)
    sys.exit(1)


# ============================================================================
# CONFIGURATION
# ============================================================================

CITY_DATA = {
    "paris": {"center": (48.8566, 2.3522), "radius_km": 12},
    "lyon": {"center": (45.7640, 4.8357), "radius_km": 8},
    "marseille": {"center": (43.2965, 5.3698), "radius_km": 10},
    "toulouse": {"center": (43.6047, 1.4442), "radius_km": 8},
    "nice": {"center": (43.7102, 7.2620), "radius_km": 6},
    "nantes": {"center": (47.2184, -1.5536), "radius_km": 7},
    "strasbourg": {"center": (48.5734, 7.7521), "radius_km": 6},
    "montpellier": {"center": (43.6108, 3.8767), "radius_km": 6},
    "bordeaux": {"center": (44.8378, -0.5792), "radius_km": 7},
    "lille": {"center": (50.6292, 3.0573), "radius_km": 6},
    "rennes": {"center": (48.1173, -1.6778), "radius_km": 6},
    "reims": {"center": (49.2583, 4.0317), "radius_km": 5},
    "toulon": {"center": (43.1242, 5.9280), "radius_km": 5},
    "grenoble": {"center": (45.1885, 5.7245), "radius_km": 5},
    "dijon": {"center": (47.3220, 5.0415), "radius_km": 5},
    "angers": {"center": (47.4784, -0.5632), "radius_km": 5},
    "nimes": {"center": (43.8367, 4.3601), "radius_km": 5},
    "aix-en-provence": {"center": (43.5297, 5.4474), "radius_km": 5},
    "clermont-ferrand": {"center": (45.7772, 3.0870), "radius_km": 5},
    "le havre": {"center": (49.4944, 0.1079), "radius_km": 5},
    "rouen": {"center": (49.4432, 1.0999), "radius_km": 5},
    "brest": {"center": (48.3904, -4.4861), "radius_km": 5},
    "tours": {"center": (47.3941, 0.6848), "radius_km": 5},
    "amiens": {"center": (49.8941, 2.2958), "radius_km": 5},
    "limoges": {"center": (45.8336, 1.2611), "radius_km": 5},
    "metz": {"center": (49.1193, 6.1757), "radius_km": 5},
    "besancon": {"center": (47.2378, 6.0241), "radius_km": 5},
    "perpignan": {"center": (42.6887, 2.8948), "radius_km": 5},
    "orleans": {"center": (47.9029, 1.9093), "radius_km": 5},
    "caen": {"center": (49.1829, -0.3707), "radius_km": 5},
    "mulhouse": {"center": (47.7508, 7.3359), "radius_km": 5},
    "nancy": {"center": (48.6921, 6.1844), "radius_km": 5},
    "saint-etienne": {"center": (45.4397, 4.3872), "radius_km": 5},
    "avignon": {"center": (43.9493, 4.8055), "radius_km": 5},
    "cannes": {"center": (43.5528, 7.0174), "radius_km": 4},
    "antibes": {"center": (43.5808, 7.1239), "radius_km": 4},
}

ZOOM_LEVELS = [15, 14, 13]

INVALID_PATTERNS = [
    r'^[\d\.,\s]+$', r'^[a-f0-9]{20,}$', r'^0x[a-f0-9]+', r'^ChIJ',
    r'^0ahUKE', r'^categorical-', r'injection', r'^results?$',
    r'^https?://', r'^\d+\.\d+$', r'^sponsored$',
]

# Patterns pour filtrer les faux emails
EMAIL_BLACKLIST = [
    'google', 'gstatic', 'schema', 'sentry', 'cloudflare',
    'facebook', 'twitter', 'instagram', 'test@', 'demo@',
    'noreply', 'no-reply', 'example', 'googleapis', 'w3.org',
    'googleusercontent', 'wixpress', 'squarespace', 'wordpress',
    'sentry.io', 'jquery', 'bootstrap', 'fontawesome', 'cdn',
    'privacy@', 'abuse@', 'postmaster@', 'webmaster@', 'hostmaster@',
    'placeholder', 'your-email', 'email@', 'name@', 'info@example',
    'user@', 'admin@example', 'support@example'
]

# Pages a visiter pour trouver les emails
EMAIL_PAGES = [
    '', '/contact', '/contact-us', '/contactez-nous', '/nous-contacter',
    '/mentions-legales', '/legal', '/mentions', '/cgv', '/cgu',
    '/about', '/about-us', '/a-propos', '/qui-sommes-nous',
    '/footer', '/infos', '/informations'
]

# Concurrent requests for email extraction
MAX_CONCURRENT_REQUESTS = 20
REQUEST_TIMEOUT = 8


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
        if not self.name or len(self.name) < 3:
            return False
        for pattern in INVALID_PATTERNS:
            if re.match(pattern, self.name, re.I):
                return False
        if not re.search(r'[a-zA-ZàâäéèêëïîôùûüÿœæÀÂÄÉÈÊËÏÎÔÙÛÜŸŒÆ]', self.name):
            return False
        return True


class EmailExtractor:
    """Extracteur d'emails optimise avec requetes paralleles"""
    
    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
        self.email_pattern = re.compile(
            r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
            re.IGNORECASE
        )
        self.mailto_pattern = re.compile(
            r'mailto:([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})',
            re.IGNORECASE
        )
    
    async def __aenter__(self):
        timeout = ClientTimeout(total=REQUEST_TIMEOUT)
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
                'Accept-Language': 'fr-FR,fr;q=0.9,en;q=0.8',
            }
        )
        return self
    
    async def __aexit__(self, *args):
        if self.session:
            await self.session.close()
    
    def _is_valid_email(self, email: str) -> bool:
        """Verifie si l'email est valide et pas dans la blacklist"""
        email_lower = email.lower()
        if len(email) < 6 or len(email) > 100:
            return False
        if any(x in email_lower for x in EMAIL_BLACKLIST):
            return False
        # Verifier le domaine
        parts = email.split('@')
        if len(parts) != 2:
            return False
        domain = parts[1].lower()
        if domain.count('.') < 1:
            return False
        # Exclure les extensions de fichiers
        if any(domain.endswith(x) for x in ['.png', '.jpg', '.gif', '.js', '.css']):
            return False
        return True
    
    def _extract_emails_from_html(self, html: str) -> Set[str]:
        """Extrait tous les emails valides d'un HTML"""
        emails = set()
        
        # Mailto links (priorite haute)
        for match in self.mailto_pattern.finditer(html):
            email = match.group(1).strip()
            if self._is_valid_email(email):
                emails.add(email.lower())
        
        # Tous les patterns email dans le HTML
        for match in self.email_pattern.finditer(html):
            email = match.group(0).strip()
            if self._is_valid_email(email):
                emails.add(email.lower())
        
        return emails
    
    async def _fetch_page(self, url: str) -> Optional[str]:
        """Fetch une page avec timeout et gestion d'erreurs"""
        async with self.semaphore:
            try:
                async with self.session.get(url, allow_redirects=True, ssl=False) as response:
                    if response.status == 200:
                        content_type = response.headers.get('content-type', '')
                        if 'text/html' in content_type or 'text/plain' in content_type:
                            return await response.text(errors='ignore')
            except Exception:
                pass
            return None
    
    async def extract_emails_from_website(self, website: str) -> Set[str]:
        """Extrait les emails d'un site web en visitant plusieurs pages"""
        emails = set()
        
        # Normaliser l'URL
        if not website.startswith('http'):
            website = 'https://' + website
        
        parsed = urlparse(website)
        base_url = f"{parsed.scheme}://{parsed.netloc}"
        
        # Creer les URLs a visiter
        urls_to_visit = []
        for page in EMAIL_PAGES:
            url = urljoin(base_url, page)
            if url not in urls_to_visit:
                urls_to_visit.append(url)
        
        # Fetch toutes les pages en parallele
        tasks = [self._fetch_page(url) for url in urls_to_visit[:8]]  # Max 8 pages par site
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Extraire les emails de chaque page
        for result in results:
            if isinstance(result, str) and result:
                page_emails = self._extract_emails_from_html(result)
                emails.update(page_emails)
        
        return emails
    
    async def extract_emails_batch(self, businesses: List[Business], on_progress=None) -> Dict[str, str]:
        """Extrait les emails pour un batch d'entreprises avec sites web"""
        results = {}
        
        # Filtrer les entreprises avec site web et sans email
        to_process = [(b.place_id or b.name, b.website) for b in businesses 
                      if b.website and not b.email]
        
        if not to_process:
            return results
        
        total = len(to_process)
        processed = 0
        
        # Traiter par batches de 50 pour le feedback
        batch_size = 50
        for i in range(0, total, batch_size):
            batch = to_process[i:i+batch_size]
            tasks = []
            
            for biz_id, website in batch:
                tasks.append(self._process_single(biz_id, website))
            
            batch_results = await asyncio.gather(*tasks, return_exceptions=True)
            
            for (biz_id, _), result in zip(batch, batch_results):
                if isinstance(result, str) and result:
                    results[biz_id] = result
            
            processed += len(batch)
            if on_progress:
                on_progress(processed, total, len(results))
        
        return results
    
    async def _process_single(self, biz_id: str, website: str) -> Optional[str]:
        """Traite un seul site web et retourne le meilleur email trouve"""
        try:
            emails = await self.extract_emails_from_website(website)
            if emails:
                # Privilegier les emails avec le domaine du site
                parsed = urlparse(website)
                domain = parsed.netloc.replace('www.', '')
                
                # Chercher un email avec le meme domaine
                for email in emails:
                    if domain in email:
                        return email
                
                # Sinon prendre le premier email "professionnel"
                for email in emails:
                    local = email.split('@')[0]
                    if any(x in local for x in ['contact', 'info', 'hello', 'bonjour', 'accueil']):
                        return email
                
                # Sinon prendre n'importe lequel
                return list(emails)[0]
        except Exception:
            pass
        return None


class GMBScraperAPI:
    def __init__(self, headless: bool = True):
        self.headless = headless
        self.browser: Optional[Browser] = None
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None

        self.businesses: Dict[str, Business] = {}
        self.seen_names: set = set()
        self.filtered_out = 0
        self.start_time: Optional[datetime] = None

        # Stockage des reponses API interceptees
        self.api_responses: List[str] = []
        self.api_data_cache: Dict = {'phones': [], 'emails': [], 'websites': []}

    def _get_city_data(self, city: str) -> dict:
        city_key = city.lower().strip().replace('-', ' ').replace('_', ' ')
        for key, data in CITY_DATA.items():
            if key.replace('-', ' ') == city_key:
                return data
        return {"center": (48.8566, 2.3522), "radius_km": 10}

    def _generate_grid(self, city: str, grid_size: int = 4) -> List[Tuple[float, float]]:
        city_data = self._get_city_data(city)
        center_lat, center_lng = city_data["center"]
        radius_km = city_data["radius_km"]
        lat_delta = radius_km / 111.0
        lng_delta = radius_km / (111.0 * math.cos(math.radians(center_lat)))

        points = []
        for i in range(grid_size):
            for j in range(grid_size):
                if grid_size > 1:
                    lat_norm = (i / (grid_size - 1)) * 2 - 1
                    lng_norm = (j / (grid_size - 1)) * 2 - 1
                else:
                    lat_norm = lng_norm = 0
                points.append((center_lat + lat_norm * lat_delta, center_lng + lng_norm * lng_delta))
        return points

    def _normalize_name(self, name: str) -> str:
        return re.sub(r'[\s\-\.]+', ' ', name.lower().strip())

    def _clean_phone(self, phone: str) -> str:
        """Nettoie le numero de telephone"""
        return re.sub(r'[\s\.\-]', '', phone)

    def _extract_email(self, text: str) -> Optional[str]:
        """Extrait un email valide d'un texte"""
        emails = re.findall(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}', text)
        for email in emails:
            email_lower = email.lower()
            if not any(x in email_lower for x in EMAIL_BLACKLIST):
                return email
        return None

    def _parse_api_response(self, body: str) -> Dict:
        """Parse les donnees de l'API search?tbm=map"""
        result = {'phones': [], 'emails': [], 'websites': []}

        # Supprimer le prefixe anti-XSSI
        if body.startswith(")]}'"):
            body = body[4:].strip()

        try:
            # Telephones
            phones = re.findall(r'"(0[1-9][\s\.]?\d{2}[\s\.]?\d{2}[\s\.]?\d{2}[\s\.]?\d{2})"', body)
            result['phones'] = list(set(phones))

            # Emails (dans les publications GMB)
            emails = re.findall(r'"([a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,})"', body)
            valid_emails = [e for e in emails if not any(x in e.lower() for x in EMAIL_BLACKLIST)]
            result['emails'] = list(set(valid_emails))

            # Websites
            websites = re.findall(r'"(https?://(?!maps\.google|www\.google|gstatic|schema\.org|googleapis|googleusercontent)[^"]{10,})"', body)
            valid_websites = [w for w in websites if not any(x in w for x in ['.png', '.jpg', '.gif', 'aclk?', 'adview?'])]
            result['websites'] = list(set(valid_websites))[:20]

        except Exception:
            pass

        return result

    def _add_business(self, biz: Business) -> bool:
        if not biz.is_valid():
            self.filtered_out += 1
            return False

        if biz.place_id and biz.place_id in self.businesses:
            # Mettre a jour si on a plus d'infos
            existing = self.businesses[biz.place_id]
            if biz.email and not existing.email:
                existing.email = biz.email
            if biz.website and not existing.website:
                existing.website = biz.website
            if biz.phone and not existing.phone:
                existing.phone = biz.phone
                existing.phone_clean = self._clean_phone(biz.phone)
            return False

        norm_name = self._normalize_name(biz.name)
        if norm_name in self.seen_names:
            return False

        # Nettoyer le telephone
        if biz.phone:
            biz.phone_clean = self._clean_phone(biz.phone)

        key = biz.place_id if biz.place_id else f"name_{norm_name}"
        self.businesses[key] = biz
        self.seen_names.add(norm_name)
        return True

    async def _init_browser(self):
        playwright = await async_playwright().start()
        self.browser = await playwright.chromium.launch(headless=self.headless)
        self.context = await self.browser.new_context(
            viewport={"width": 1920, "height": 1080},
            locale="fr-FR",
        )
        self.page = await self.context.new_page()

        # Intercepter les reponses API
        async def capture_api_response(response):
            url = response.url
            if 'search?tbm=map' in url or 'preview/place' in url:
                try:
                    content_type = response.headers.get('content-type', '')
                    if 'json' in content_type or 'text' in content_type:
                        body = await response.text()
                        if len(body) > 500:
                            self.api_responses.append(body)
                            # Parser immediatement
                            parsed = self._parse_api_response(body)
                            self.api_data_cache['phones'].extend(parsed['phones'])
                            self.api_data_cache['emails'].extend(parsed['emails'])
                            self.api_data_cache['websites'].extend(parsed['websites'])
                except:
                    pass

        self.page.on('response', capture_api_response)

        await self._handle_consent()

    async def _handle_consent(self):
        try:
            await self.page.goto("https://www.google.com/maps", wait_until="domcontentloaded", timeout=30000)
            await asyncio.sleep(1)
            for selector in ['button:has-text("Tout accepter")', 'button:has-text("Accept all")']:
                try:
                    btn = await self.page.wait_for_selector(selector, timeout=3000)
                    if btn:
                        await btn.click()
                        await asyncio.sleep(1)
                        return
                except:
                    continue
        except:
            pass

    async def _smart_scroll_and_extract(self) -> int:
        """Scroll adaptatif avec extraction par batch"""
        new_count = 0
        last_extract_count = 0
        stable = 0

        for _ in range(5):
            await self.page.evaluate('document.querySelector(\'div[role="feed"]\')?.scrollBy(0, 1500)')
            await asyncio.sleep(0.15)

        for cycle in range(8):
            for _ in range(5):
                await self.page.evaluate('document.querySelector(\'div[role="feed"]\')?.scrollBy(0, 1500)')
                await asyncio.sleep(0.15)

            extracted = await self._batch_extract()
            new_count += extracted

            end_check = await self.page.evaluate('''
                (() => {
                    const feed = document.querySelector('div[role="feed"]');
                    if (!feed) return false;
                    const text = feed.innerText || '';
                    return text.includes("Vous avez fait le tour") || text.includes("plus de resultats");
                })()
            ''')

            if end_check:
                break

            current_total = len(self.businesses)
            if current_total == last_extract_count:
                stable += 1
                if stable >= 2:
                    break
            else:
                stable = 0
            last_extract_count = current_total

        return new_count

    async def _batch_extract(self) -> int:
        """Extraction batch avec donnees API enrichies"""
        new_count = 0

        # Dedupliquer les donnees API
        api_phones = list(set(self.api_data_cache['phones']))
        api_emails = list(set(self.api_data_cache['emails']))
        api_websites = list(set(self.api_data_cache['websites']))

        try:
            results = await self.page.evaluate('''
                (() => {
                    const items = document.querySelectorAll('div[role="feed"] > div > div[jsaction]');
                    const data = [];

                    items.forEach(item => {
                        try {
                            const link = item.querySelector('a[href*="/maps/place/"]');
                            if (!link) return;

                            const href = link.getAttribute('href') || '';
                            const name = link.getAttribute('aria-label') || '';
                            if (!name || name.length < 3) return;

                            let placeId = '';
                            const match1 = href.match(/!1s(0x[a-f0-9]+:0x[a-f0-9]+)/);
                            if (match1) placeId = match1[1];
                            else {
                                const match2 = href.match(/!1s(ChIJ[A-Za-z0-9_-]+)/);
                                if (match2) placeId = match2[1];
                            }

                            let lat = null, lng = null;
                            const coordMatch = href.match(/@(-?\\d+\\.\\d+),(-?\\d+\\.\\d+)/);
                            if (coordMatch) {
                                lat = parseFloat(coordMatch[1]);
                                lng = parseFloat(coordMatch[2]);
                            }

                            let rating = null, reviewCount = 0;
                            const ratingEl = item.querySelector('span[role="img"]');
                            if (ratingEl) {
                                const ratingText = ratingEl.getAttribute('aria-label') || '';
                                const ratingMatch = ratingText.match(/(\\d)[,\\.](\\d)\\s*[eE]toile/);
                                if (ratingMatch) {
                                    rating = parseFloat(ratingMatch[1] + '.' + ratingMatch[2]);
                                }
                                const reviewMatch = ratingText.match(/(\\d[\\d\\s\\u202f]*)\\s*avis/i);
                                if (reviewMatch) {
                                    reviewCount = parseInt(reviewMatch[1].replace(/[\\s\\u202f]/g, ''));
                                }
                            }

                            const fullText = item.innerText || '';

                            data.push({
                                name: name.trim(),
                                placeId,
                                href,
                                lat,
                                lng,
                                rating,
                                reviewCount,
                                fullText
                            });
                        } catch (e) {}
                    });

                    return data;
                })()
            ''')

            for r in results:
                try:
                    if r['placeId'] and r['placeId'] in self.businesses:
                        continue

                    norm_name = self._normalize_name(r['name'])
                    if norm_name in self.seen_names:
                        continue

                    biz = Business(
                        name=r['name'],
                        place_id=r['placeId'],
                        google_maps_url=r['href'],
                        latitude=r['lat'],
                        longitude=r['lng'],
                        rating=r['rating'],
                        review_count=r['reviewCount'] or 0
                    )

                    lines = [l.strip() for l in r['fullText'].split('\n') if l.strip()]

                    # Phone depuis le DOM
                    for line in lines:
                        if not biz.phone:
                            phone_match = re.search(r'(0[1-9](?:[\s\.]?\d{2}){4})', line)
                            if phone_match:
                                biz.phone = phone_match.group(1)
                                break

                    # Si pas de phone dans le DOM, chercher dans l'API
                    if not biz.phone and api_phones:
                        for phone in api_phones:
                            used = any(b.phone == phone for b in self.businesses.values())
                            if not used:
                                biz.phone = phone
                                break

                    # Category
                    for line in lines[1:6]:
                        if re.match(r'^[\d,\.]+$', line):
                            continue
                        if any(x in line.lower() for x in ['avis', 'ouvert', 'ferme']):
                            continue
                        if re.search(r'\d{2}[\s\.]\d{2}[\s\.]\d{2}', line):
                            continue
                        if line.startswith('.'):
                            continue
                        if len(line) < 60:
                            biz.category = line
                            break

                    # Address
                    for line in lines:
                        if re.search(r'\d+[,\s]*(rue|avenue|av\.|boulevard|bd\.|place|chemin|allee)', line, re.I):
                            biz.address = line[:150]
                            break

                    # Email depuis l'API (dans les publications GMB)
                    if api_emails and not biz.email:
                        name_words = set(biz.name.lower().split())
                        for email in api_emails:
                            email_parts = email.lower().split('@')
                            if any(word in email_parts[0] or word in email_parts[1] for word in name_words if len(word) > 3):
                                biz.email = email
                                break

                    # Website depuis l'API
                    if api_websites and not biz.website:
                        name_words = set(biz.name.lower().split())
                        for website in api_websites:
                            domain_match = re.search(r'https?://(?:www\.)?([^/]+)', website)
                            if domain_match:
                                domain = domain_match.group(1).lower()
                                if any(word in domain for word in name_words if len(word) > 3):
                                    biz.website = website
                                    break

                    if self._add_business(biz):
                        new_count += 1
                        emit("business", biz.to_dict())

                except:
                    continue

        except Exception:
            pass

        return new_count

    async def _extract_detail_from_page(self, biz: Business) -> Business:
        """Extrait les details depuis la fiche GMB ouverte"""
        try:
            # Chercher email dans la page
            html = await self.page.content()
            email = self._extract_email(html)
            if email and not biz.email:
                biz.email = email

            # Website depuis le DOM
            try:
                website_el = await self.page.query_selector('a[data-item-id="authority"]')
                if website_el:
                    website = await website_el.get_attribute('href')
                    if website and not biz.website:
                        biz.website = website
            except:
                pass

            # Telephone depuis le DOM
            try:
                phone_els = await self.page.query_selector_all('button[data-item-id*="phone"]')
                for el in phone_els:
                    label = await el.get_attribute('aria-label')
                    if label:
                        phone_match = re.search(r'(0[1-9](?:[\s\.]?\d{2}){4})', label)
                        if phone_match and not biz.phone:
                            biz.phone = phone_match.group(1)
                            biz.phone_clean = self._clean_phone(biz.phone)
                            break
            except:
                pass

            # Adresse complete depuis le DOM
            try:
                addr_el = await self.page.query_selector('button[data-item-id="address"]')
                if addr_el:
                    label = await addr_el.get_attribute('aria-label')
                    if label and (not biz.address or len(label) > len(biz.address)):
                        biz.address = label.replace('Adresse:', '').strip()
            except:
                pass

        except Exception:
            pass

        return biz

    async def _search_zone(self, query: str, lat: float, lng: float, zoom: int) -> int:
        initial = len(self.businesses)

        # Reset le cache API pour cette zone
        self.api_responses.clear()
        self.api_data_cache = {'phones': [], 'emails': [], 'websites': []}

        search_url = f"https://www.google.com/maps/search/{quote(query)}/@{lat},{lng},{zoom}z"

        try:
            await self.page.goto(search_url, wait_until="domcontentloaded", timeout=15000)
        except:
            return 0

        await asyncio.sleep(0.7)
        await self._smart_scroll_and_extract()

        return len(self.businesses) - initial

    async def scrape(self, business_type: str, city: str, grid_size: int = 4) -> List[Business]:
        self.start_time = datetime.now()

        emit("start", {
            "activity": business_type,
            "city": city,
            "grid_size": grid_size,
            "total_zones": grid_size * grid_size * len(ZOOM_LEVELS),
            "version": "v14-email"
        })

        try:
            emit("status", {"message": "Initialisation du navigateur..."})
            await self._init_browser()

            search_points = self._generate_grid(city, grid_size)
            total = len(search_points) * len(ZOOM_LEVELS)
            search_num = 0

            for zoom in ZOOM_LEVELS:
                emit("zoom_start", {"zoom": zoom})
                zoom_new = 0

                for idx, (lat, lng) in enumerate(search_points, 1):
                    search_num += 1
                    zone_new = await self._search_zone(business_type, lat, lng, zoom)
                    zoom_new += zone_new

                    emit("progress", {
                        "zone": search_num,
                        "total_zones": total,
                        "zoom": zoom,
                        "new_businesses": zone_new,
                        "total_businesses": len(self.businesses),
                        "percent": round(100 * search_num / total, 1)
                    })

                emit("zoom_complete", {"zoom": zoom, "new_businesses": zoom_new})

            # Phase 2: Enrichissement GMB (SANS LIMITE)
            incomplete = [
                (k, b) for k, b in self.businesses.items()
                if not b.phone or not b.website
            ]

            if incomplete:
                emit("status", {"message": f"Enrichissement de {len(incomplete)} fiches GMB..."})

                for i, (key, biz) in enumerate(incomplete):
                    try:
                        if biz.google_maps_url:
                            await self.page.goto(biz.google_maps_url, wait_until="domcontentloaded", timeout=10000)
                            await asyncio.sleep(0.8)
                            self.businesses[key] = await self._extract_detail_from_page(biz)

                            if (i + 1) % 20 == 0:
                                emit("enrichment_progress", {
                                    "enriched": i + 1,
                                    "total": len(incomplete)
                                })
                    except:
                        continue

            # Phase 3: Extraction d'emails depuis les sites web (NOUVELLE PHASE)
            businesses_with_website = [b for b in self.businesses.values() if b.website and not b.email]
            
            if businesses_with_website:
                emit("status", {"message": f"Extraction d'emails depuis {len(businesses_with_website)} sites web..."})
                
                async with EmailExtractor() as extractor:
                    def on_email_progress(processed, total, found):
                        emit("email_extraction_progress", {
                            "processed": processed,
                            "total": total,
                            "emails_found": found
                        })
                    
                    email_results = await extractor.extract_emails_batch(
                        list(self.businesses.values()),
                        on_progress=on_email_progress
                    )
                    
                    # Mettre a jour les businesses avec les emails trouves
                    for key, biz in self.businesses.items():
                        biz_id = biz.place_id or biz.name
                        if biz_id in email_results:
                            biz.email = email_results[biz_id]
                            emit("business_updated", {
                                **biz.to_dict(),
                                "email_source": "website"
                            })
                
                emit("email_extraction_complete", {"emails_found": len(email_results)})

            # Fermer le navigateur
            if self.browser:
                await self.browser.close()

            elapsed = (datetime.now() - self.start_time).total_seconds()
            businesses_list = list(self.businesses.values())

            stats = {
                "total": len(businesses_list),
                "with_phone": sum(1 for b in businesses_list if b.phone),
                "with_email": sum(1 for b in businesses_list if b.email),
                "with_website": sum(1 for b in businesses_list if b.website),
                "with_address": sum(1 for b in businesses_list if b.address),
                "with_category": sum(1 for b in businesses_list if b.category),
                "with_rating": sum(1 for b in businesses_list if b.rating),
                "filtered_out": self.filtered_out,
                "duration_seconds": round(elapsed, 1)
            }

            emit("complete", {
                "stats": stats,
                "businesses": [b.to_dict() for b in businesses_list]
            })

            return businesses_list

        except Exception as e:
            emit("error", {"message": str(e)})
            return []

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

    scraper = GMBScraperAPI(headless=True)
    await scraper.scrape(activity, city, grid_size)


if __name__ == "__main__":
    asyncio.run(main())
