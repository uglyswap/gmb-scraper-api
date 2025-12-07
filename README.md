# GMB Scraper API

API de scraping Google My Business avec streaming SSE (Server-Sent Events).

## Fonctionnalites

- Scraping Google Maps par activite et ville
- Streaming temps reel via SSE (pas de timeout)
- Extraction des emails et sites web via interception API Google
- Support de 36 villes francaises
- Authentification par cle API

---

## Endpoint Principal : Streaming SSE

```
GET /api/scrape/stream
```

### Parametres

| Parametre | Type | Requis | Description |
|-----------|------|--------|-------------|
| `activity` | string | Oui | Type d'activite (ex: "restaurant", "coiffeur", "agence immobiliere") |
| `city` | string | Oui | Ville (ex: "paris", "lyon", "marseille") |
| `grid_size` | number | Non | Taille de la grille de recherche (1-6, defaut: 4) |
| `api_key` | string | Oui | Cle API (peut aussi etre en header) |

### Parametre `grid_size` - Guide de choix

| grid_size | Zones | Temps approx. | Resultats | Cas d'usage |
|-----------|-------|---------------|-----------|-------------|
| **1** | 1 | ~2 min | ~100-300 | Test rapide, petite ville |
| **2** | 4 | ~5 min | ~300-600 | Recherche rapide |
| **3** | 9 | ~10 min | ~500-1000 | Bon compromis vitesse/resultats |
| **4** | 16 | ~15-20 min | ~800-1500 | **Recommande** - Equilibre optimal |
| **5** | 25 | ~25-30 min | ~1000-2000 | Couverture large |
| **6** | 36 | ~40-50 min | ~1500-2500 | Couverture maximale |

**Recommandation:**
- `grid_size=3` : Le plus rapide avec des resultats corrects
- `grid_size=4` : Meilleur rapport qualite/temps (defaut)
- `grid_size=5-6` : Maximum de resultats, mais plus lent

### Exemple de requete

```bash
curl -N "http://votre-serveur:3000/api/scrape/stream?activity=restaurant&city=paris&grid_size=3&api_key=VOTRE_CLE"
```

### Evenements SSE retournes

Le flux retourne des evenements au format `event: type\ndata: json\n\n`

| Event | Description | Donnees |
|-------|-------------|---------|
| `job` | ID du job demarre | `{ job_id: "uuid" }` |
| `start` | Debut du scraping | `{ activity, city, grid_size, total_zones }` |
| `progress` | Progression | `{ zone, total_zones, percent, new_businesses, total_businesses }` |
| `business` | Business trouve | `{ data: { name, phone, address, email, website, ... } }` |
| `complete` | Fin du scraping | `{ stats: {...}, businesses: [...] }` |
| `error` | Erreur | `{ message: "..." }` |

### Exemple JavaScript

```javascript
const eventSource = new EventSource(
  'http://votre-serveur:3000/api/scrape/stream?activity=coiffeur&city=lyon&grid_size=3&api_key=VOTRE_CLE'
);

// Progression
eventSource.addEventListener('progress', (e) => {
  const data = JSON.parse(e.data);
  console.log(`${data.percent}% - ${data.total_businesses} businesses`);
});

// Chaque business en temps reel
eventSource.addEventListener('business', (e) => {
  const { data } = JSON.parse(e.data);
  console.log(`${data.name} - ${data.phone} - ${data.email}`);
});

// Fin avec tous les resultats
eventSource.addEventListener('complete', (e) => {
  const { stats, businesses } = JSON.parse(e.data);
  console.log(`Termine: ${stats.total} businesses`);
  eventSource.close();
});

eventSource.onerror = () => eventSource.close();
```

---

## Autres Endpoints

### Health Check
```
GET /health
```
Retourne le statut de l'API.

### Scraping Synchrone (non recommande pour gros volumes)
```
POST /api/scrape
Content-Type: application/json
X-API-Key: VOTRE_CLE

{
  "activity": "boulangerie",
  "city": "nice",
  "grid_size": 2
}
```

### Liste des villes supportees
```
GET /api/scrape/info/cities
```

---

## Authentification

La cle API peut etre fournie de 3 facons:

1. **Query parameter** (recommande pour SSE):
   ```
   ?api_key=VOTRE_CLE
   ```

2. **Header X-API-Key**:
   ```
   X-API-Key: VOTRE_CLE
   ```

3. **Header Authorization**:
   ```
   Authorization: Bearer VOTRE_CLE
   ```

---

## Format des donnees Business

```json
{
  "name": "Restaurant Le Petit Bistrot",
  "place_id": "ChIJ...",
  "address": "123 Rue du General de Gaulle, 75001 Paris",
  "phone": "01 23 45 67 89",
  "phone_clean": "0123456789",
  "email": "contact@petitbistrot.fr",
  "website": "https://www.petitbistrot.fr",
  "category": "Restaurant francais",
  "rating": 4.5,
  "review_count": 234,
  "latitude": 48.8566,
  "longitude": 2.3522,
  "google_maps_url": "https://www.google.com/maps/place/..."
}
```

---

## Villes Supportees

Paris, Lyon, Marseille, Toulouse, Nice, Nantes, Strasbourg, Montpellier, Bordeaux, Lille, Rennes, Reims, Toulon, Grenoble, Dijon, Angers, Nimes, Aix-en-Provence, Clermont-Ferrand, Le Havre, Rouen, Brest, Tours, Amiens, Limoges, Metz, Besancon, Perpignan, Orleans, Caen, Mulhouse, Nancy, Saint-Etienne, Avignon, Cannes, Antibes

---

## Installation locale

```bash
# Cloner le repo
git clone https://github.com/uglyswap/gmb-scraper-api.git
cd gmb-scraper-api

# Installer les dependances
npm install

# Configurer l'environnement
cp .env.example .env
# Editer .env avec vos cles API

# Lancer en dev
npm run dev

# Ou build + production
npm run build && npm start
```

### Variables d'environnement

```env
PORT=3000
API_KEYS=cle1,cle2,cle3
PYTHON_PATH=python3
NODE_ENV=production
```

---

## Deploiement Docker

```bash
docker build -t gmb-scraper-api .
docker run -p 3000:3000 -e API_KEYS=votre_cle gmb-scraper-api
```
