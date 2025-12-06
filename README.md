# GMB Scraper API

API de scraping Google My Business avec streaming SSE pour éviter les timeouts.

## Installation

```bash
cd "API GMB"
npm install
```

## Configuration

Copier `.env.example` vers `.env` et configurer:

```env
PORT=3000
API_KEYS=votre_cle_api_1,votre_cle_api_2
PYTHON_PATH=python
```

## Démarrage

```bash
# Mode développement (hot reload)
npm run dev

# Mode production
npm run build && npm start
```

## Endpoints

### Health Check
```
GET /health
```

### Scraping avec Streaming SSE (recommandé)
```
GET /api/scrape/stream?activity=restaurant&city=paris&grid_size=4
```

Retourne un flux SSE avec les événements:
- `job`: ID du job
- `start`: Début du scraping
- `progress`: Progression (zone, %)
- `business`: Chaque business trouvé
- `complete`: Fin avec stats et tous les résultats

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

### Récupérer un Job
```
GET /api/scrape/:jobId
```

### Villes Supportées
```
GET /api/scrape/info/cities
```

## Authentification

La clé API peut être fournie via:
- Header: `X-API-Key: votre_cle`
- Header: `Authorization: Bearer votre_cle`
- Query param: `?api_key=votre_cle`

## Format de Réponse

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
      "with_address": 1945,
      "with_category": 2320,
      "with_rating": 2380,
      "filtered_out": 12,
      "duration_seconds": 396
    },
    "businesses": [
      {
        "name": "Restaurant Example",
        "place_id": "0x...:0x...",
        "phone": "01 23 45 67 89",
        "phone_clean": "0123456789",
        "address": "123 Rue Example, 75001 Paris",
        "category": "Restaurant français",
        "rating": 4.5,
        "review_count": 123,
        "latitude": 48.8566,
        "longitude": 2.3522,
        "google_maps_url": "https://..."
      }
    ]
  },
  "timestamp": "2024-01-01T12:00:00.000Z"
}
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

eventSource.addEventListener('progress', (e) => {
  const data = JSON.parse(e.data);
  console.log(`Progression: ${data.percent}% - ${data.total_businesses} businesses`);
});

eventSource.addEventListener('business', (e) => {
  const data = JSON.parse(e.data);
  console.log('Nouveau business:', data.data.name);
});

eventSource.addEventListener('complete', (e) => {
  const data = JSON.parse(e.data);
  console.log('Terminé!', data.stats.total, 'businesses');
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
