# GMB Scraper API v3.0 PRODUCTION

API de scraping Google My Business avec streaming SSE pour des performances optimales.

## Performance

| Metrique | Valeur |
|----------|--------|
| **Workers paralleles** | 55 |
| **Vitesse extraction** | ~450 businesses/min |
| **Taux de succes** | 99.8% |
| **Temps pour 500 fiches** | ~1 min |

### Historique des tests

| Version | Workers | Succes | Vitesse |
|---------|---------|--------|---------|
| V35 | 40 | 100% | 397/min |
| V38 | 50 | 96% | 320/min |
| **V39 (PROD)** | **55** | **99.8%** | **452/min** |
| V40 | 58 | 97.2% | 393/min |
| V37 | 60 | 0% | FAIL |

## Installation

```bash
cd "API GMB"
npm install
```

### Dependances Python

```bash
pip install playwright aiohttp
playwright install chromium
```

## Configuration

Copier `.env.example` vers `.env` et configurer:

```env
PORT=3000
API_KEYS=votre_cle_api_1,votre_cle_api_2
PYTHON_PATH=python
```

## Demarrage

```bash
# Mode developpement (hot reload)
npm run dev

# Mode production
npm run build && npm start
```

## Endpoints

### Health Check
```
GET /health
```

### Scraping avec Streaming SSE (recommande)
```
GET /api/scrape/stream?activity=restaurant&city=paris&grid_size=4
```

Retourne un flux SSE avec les evenements:
- `geocoding`: Geocodage de la ville
- `start`: Debut du scraping
- `zone_start`: Debut d'une zone
- `zone_links`: Nouveaux IDs trouves
- `zone_complete`: Zone terminee
- `business`: Chaque business trouve
- `extraction_start`: Debut extraction details
- `email_extraction_start`: Debut extraction emails
- `email_extraction_progress`: Progression emails
- `email_found`: Email trouve
- `complete`: Fin avec stats et tous les resultats

### Scraping Synchrone
```
POST /api/scrape
Content-Type: application/json
X-API-Key: votre_cle

{
  "activity": "agence immobiliere",
  "city": "lyon",
  "grid_size": 4
}
```

### Mode Webhook (n8n)
```
POST /api/scrape/webhook
Content-Type: application/json
X-API-Key: votre_cle

{
  "activity": "restaurant",
  "city": "paris",
  "grid_size": 3,
  "webhook_url": "https://votre-n8n.com/webhook/xxx"
}
```

### Recuperer un Job
```
GET /api/scrape/:jobId
```

### Villes Supportees
```
GET /api/scrape/info/cities
```

## Authentification

La cle API peut etre fournie via:
- Header: `X-API-Key: votre_cle`
- Header: `Authorization: Bearer votre_cle`
- Query param: `?api_key=votre_cle`

## Format de Reponse

```json
{
  "success": true,
  "data": {
    "job_id": "uuid",
    "query": {
      "activity": "restaurant",
      "city": "paris",
      "grid_size": 4
    },
    "stats": {
      "total": 2402,
      "with_phone": 2354,
      "with_email": 845,
      "with_website": 1945,
      "with_address": 2320,
      "with_rating": 2380,
      "filtered_out": 12,
      "duration_seconds": 320
    },
    "businesses": [
      {
        "name": "Restaurant Example",
        "place_id": "ChIJ...",
        "phone": "01 23 45 67 89",
        "phone_clean": "0123456789",
        "email": "contact@example.com",
        "address": "123 Rue Example, 75001 Paris",
        "website": "https://example.com",
        "category": "Restaurant francais",
        "rating": 4.5,
        "review_count": 123,
        "google_maps_url": "https://..."
      }
    ]
  },
  "timestamp": "2024-01-01T12:00:00.000Z"
}
```

## Architecture

```
API GMB/
├── src/
│   ├── index.ts          # Point d'entree API Hono
│   ├── config.ts         # Configuration
│   ├── routes/
│   │   └── scrape.ts     # Routes scraping
│   ├── services/
│   │   └── scraper.ts    # Service scraper Python
│   ├── middleware/
│   │   └── auth.ts       # Authentification API key
│   └── types/
│       └── index.ts      # Types TypeScript
├── scraper/
│   └── gmb_scraper_production.py  # Scraper Python optimise
├── public/
│   └── index.html        # Interface web
└── package.json
```

## Principe de fonctionnement

### 3 Phases

1. **Phase 1 - Collecte IDs** (15 workers)
   - Scan grille 10x10 zones (~1km chaque)
   - Interception reponses API Google Maps
   - Extraction place_ids via regex

2. **Phase 2 - Extraction details** (55 workers)
   - **CRITIQUE**: Nouvelle page par place_id
   - Timeout 12s, sleep 0.8s
   - Extraction: nom, telephone, site, adresse, note, avis

3. **Phase 3 - Emails** (100 connexions paralleles)
   - Scan sites web trouves
   - Extraction emails via regex
   - Filtrage spam/invalides

### Pattern critique

```python
# FONCTIONNE: Nouvelle page par PID
for pid in pids:
    page = await context.new_page()
    try:
        await page.goto(url)
        # extraction...
    finally:
        await page.close()  # CRITIQUE

# NE FONCTIONNE PAS: Reutilisation page
page = await context.new_page()
for pid in pids:
    await page.goto(url)  # 0% success rate!
```

## Exemple avec cURL

```bash
# Streaming SSE
curl -N "http://localhost:3000/api/scrape/stream?activity=boulangerie&city=nice&grid_size=2&api_key=votre_cle"

# Synchrone
curl -X POST http://localhost:3000/api/scrape \
  -H "Content-Type: application/json" \
  -H "X-API-Key: votre_cle" \
  -d '{"activity": "coiffeur", "city": "lyon", "grid_size": 3}'
```

## Exemple JavaScript (SSE)

```javascript
const eventSource = new EventSource(
  'http://localhost:3000/api/scrape/stream?activity=restaurant&city=paris&grid_size=3&api_key=votre_cle'
);

eventSource.addEventListener('business', (e) => {
  const data = JSON.parse(e.data);
  console.log('Nouveau business:', data.name, data.phone);
});

eventSource.addEventListener('complete', (e) => {
  const data = JSON.parse(e.data);
  console.log('Termine!', data.stats.total, 'businesses');
  console.log('Vitesse:', Math.round(data.stats.total / (data.stats.duration_seconds / 60)), '/min');
  eventSource.close();
});

eventSource.onerror = () => {
  console.error('Erreur SSE');
  eventSource.close();
};
```

## Tests

```bash
npm run test
```

## License

MIT
