import { serve } from '@hono/node-server';
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

// Documentation
app.get('/', (c) => {
  return c.json({
    name: 'GMB Scraper API',
    version: config.version,
    description: 'API de scraping Google My Business avec streaming SSE',
    endpoints: {
      'GET /health': 'Vérification de l\'état de l\'API',
      'GET /api/scrape/stream?activity=...&city=...': 'Scraping avec streaming SSE (temps réel)',
      'POST /api/scrape': 'Scraping synchrone (attend la fin)',
      'POST /api/scrape/webhook': '⭐ Mode webhook pour n8n (async + callback)',
      'GET /api/scrape/:jobId': 'Récupérer les résultats d\'un job',
      'GET /api/scrape/info/cities': 'Liste des villes supportées',
      'POST /api/generate-key': 'Générer une nouvelle clé API (dev only)'
    },
    authentication: {
      methods: [
        'Header: X-API-Key: <votre_clé>',
        'Header: Authorization: Bearer <votre_clé>',
        'Query param: ?api_key=<votre_clé>'
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
        note: 'Retour immédiat + callback webhook quand terminé'
      }
    },
    supported_cities: SUPPORTED_CITIES.slice(0, 10).concat(['...'] as any),
    timestamp: new Date().toISOString()
  });
});

// Générer une clé API (mode dev uniquement)
app.post('/api/generate-key', (c) => {
  const newKey = generateApiKey();
  return c.json({
    success: true,
    data: {
      api_key: newKey,
      note: 'Ajoutez cette clé à votre variable d\'environnement API_KEYS'
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
    error: 'Route non trouvée',
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
╔══════════════════════════════════════════════════════════════════╗
║                     GMB SCRAPER API v${config.version}                       ║
╠══════════════════════════════════════════════════════════════════╣
║  Endpoints:                                                      ║
║  • GET  /                          Documentation                 ║
║  • GET  /health                    Health check                  ║
║  • GET  /api/scrape/stream         Streaming SSE                 ║
║  • POST /api/scrape                Scraping synchrone            ║
║  • POST /api/scrape/webhook        Mode webhook (n8n)            ║
║  • GET  /api/scrape/:jobId         Récupérer un job              ║
║  • GET  /api/scrape/info/cities    Villes supportées             ║
╠══════════════════════════════════════════════════════════════════╣
║  Serveur démarré sur http://localhost:${port.toString().padEnd(25, ' ')}║
╚══════════════════════════════════════════════════════════════════╝
`);

serve({
  fetch: app.fetch,
  port
});

export default app;
