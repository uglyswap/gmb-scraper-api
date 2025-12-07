import { serve } from '@hono/node-server';
import { serveStatic } from '@hono/node-server/serve-static';
import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { logger } from 'hono/logger';
import { prettyJSON } from 'hono/pretty-json';

import { loadEnv, config, checkPython } from './config.js';
import { apiKeyAuth, generateApiKey } from './middleware/auth.js';
import { scrapeRouter } from './routes/scrape.js';
import { SUPPORTED_CITIES } from './types/index.js';

// Charger les variables d'environnement
loadEnv();

// ============================================================================
// APP HONO
// ============================================================================

const app = new Hono();

// Middleware globaux
app.use('*', cors());
app.use('*', logger());
app.use('*', prettyJSON());

// ============================================================================
// ROUTES PUBLIQUES
// ============================================================================

// Servir l'application web (page d'accueil)
app.get('/', serveStatic({ path: './public/index.html' }));
app.get('/app', serveStatic({ path: './public/index.html' }));

// Health check
app.get('/health', async (c) => {
  const pythonOk = await checkPython();
  const uptimeSeconds = Math.floor((Date.now() - config.startTime) / 1000);

  return c.json({
    success: true,
    data: {
      status: pythonOk ? 'ok' : 'degraded',
      version: config.version,
      uptime_seconds: uptimeSeconds,
      python_available: pythonOk
    },
    timestamp: new Date().toISOString()
  });
});

// Documentation API (JSON)
app.get('/api/docs', (c) => {
  return c.json({
    name: 'GMB Scraper API',
    version: config.version,
    description: 'API de scraping Google My Business avec streaming SSE',
    endpoints: {
      'GET /health': 'Vérification de l\'etat de l\'API',
      'GET /api/scrape/stream?activity=...&city=...': 'Scraping avec streaming SSE (temps reel)',
      'POST /api/scrape': 'Scraping synchrone (attend la fin)',
      'POST /api/scrape/webhook': 'Mode webhook pour n8n (async + callback)',
      'GET /api/scrape/:jobId': 'Recuperer les resultats d\'un job',
      'GET /api/scrape/info/cities': 'Liste des villes supportees',
      'POST /api/generate-key': 'Generer une nouvelle cle API (dev only)'
    },
    authentication: {
      methods: [
        'Header: X-API-Key: <votre_cle>',
        'Header: Authorization: Bearer <votre_cle>',
        'Query param: ?api_key=<votre_cle>'
      ]
    },
    examples: {
      stream: 'GET /api/scrape/stream?activity=restaurant&city=paris&grid_size=3',
      sync: {
        method: 'POST',
        url: '/api/scrape',
        body: { activity: 'agence immobiliere', city: 'lyon', grid_size: 4 }
      },
      webhook_n8n: {
        method: 'POST',
        url: '/api/scrape/webhook',
        body: {
          activity: 'restaurant',
          city: 'paris',
          grid_size: 3,
          webhook_url: 'https://votre-n8n.com/webhook/xxx'
        },
        note: 'Retour immediat + callback webhook quand termine'
      }
    },
    supported_cities: SUPPORTED_CITIES.slice(0, 10).concat(['...'] as any),
    timestamp: new Date().toISOString()
  });
});

// Generer une cle API (mode dev uniquement)
app.post('/api/generate-key', (c) => {
  const newKey = generateApiKey();
  return c.json({
    success: true,
    data: {
      api_key: newKey,
      note: 'Ajoutez cette cle a votre variable d\'environnement API_KEYS'
    },
    timestamp: new Date().toISOString()
  });
});

// ============================================================================
// ROUTES PROTEGEES
// ============================================================================

// Appliquer l'auth sur /api/*
app.use('/api/*', apiKeyAuth);

// Routes de scraping
app.route('/api/scrape', scrapeRouter);

// ============================================================================
// GESTION DES ERREURS
// ============================================================================

app.notFound((c) => {
  return c.json({
    success: false,
    error: 'Route non trouvee',
    available_routes: ['/', '/health', '/api/scrape', '/api/scrape/stream'],
    timestamp: new Date().toISOString()
  }, 404);
});

app.onError((err, c) => {
  console.error('Erreur serveur:', err);
  return c.json({
    success: false,
    error: err.message || 'Erreur interne du serveur',
    timestamp: new Date().toISOString()
  }, 500);
});

// ============================================================================
// DEMARRAGE DU SERVEUR
// ============================================================================

const port = config.port;

console.log(`
======================================================================
                     GMB SCRAPER API v${config.version}
======================================================================
  Endpoints:
  - GET  /                          Application Web
  - GET  /health                    Health check
  - GET  /api/docs                  Documentation API
  - GET  /api/scrape/stream         Streaming SSE
  - POST /api/scrape                Scraping synchrone
  - POST /api/scrape/webhook        Mode webhook (n8n)
  - GET  /api/scrape/:jobId         Recuperer un job
======================================================================
  Serveur demarre sur http://localhost:${port}
======================================================================
`);

serve({
  fetch: app.fetch,
  port
});

export default app;
