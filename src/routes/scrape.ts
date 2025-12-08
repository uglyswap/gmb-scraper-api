import { Hono } from 'hono';
import { streamSSE } from 'hono/streaming';
import { v4 as uuidv4 } from 'uuid';
import { ScraperService } from '../services/scraper.js';
import { ScrapeQuerySchema, SUPPORTED_CITIES } from '../types/index.js';
import type { ScrapeResult, Business, SSEEvent } from '../types/index.js';
import { z } from 'zod';

const scrapeRouter = new Hono();

// Schema pour l'endpoint webhook
const WebhookScrapeSchema = z.object({
  activity: z.string().min(2, 'Activité requise (min 2 caractères)'),
  city: z.string().min(2, 'Ville requise (min 2 caractères)'),
  grid_size: z.coerce.number().int().min(1).max(55).default(4),
  webhook_url: z.string().url('URL webhook invalide'),
});

// Store des jobs en cours et terminés
const jobStore = new Map<string, {
  status: 'running' | 'completed' | 'error';
  result?: ScrapeResult;
  error?: string;
  startedAt: Date;
  completedAt?: Date;
}>();

// ============================================================================
// GET /scrape/stream - Streaming SSE
// ============================================================================

scrapeRouter.get('/stream', async (c) => {
  const activity = c.req.query('activity');
  const city = c.req.query('city');
  const gridSize = c.req.query('grid_size') || '4';

  const validation = ScrapeQuerySchema.safeParse({
    activity,
    city,
    grid_size: gridSize
  });

  if (!validation.success) {
    return c.json({
      success: false,
      error: 'Paramètres invalides',
      details: validation.error.flatten(),
      timestamp: new Date().toISOString()
    }, 400);
  }

  const query = validation.data;
  const jobId = uuidv4();

  jobStore.set(jobId, {
    status: 'running',
    startedAt: new Date()
  });

  return streamSSE(c, async (stream) => {
    const scraper = new ScraperService();
    const businesses: Business[] = [];

    await stream.writeSSE({
      event: 'job',
      data: JSON.stringify({ job_id: jobId })
    });

    try {
      for await (const event of scraper.stream({
        activity: query.activity,
        city: query.city,
        gridSize: query.grid_size
      })) {
        if (event.type === 'business' && (event as any).data) {
          businesses.push((event as any).data);
        }

        await stream.writeSSE({
          event: event.type,
          data: JSON.stringify(event)
        });

        if (event.type === 'complete') {
          const completeEvent = event as any;
          jobStore.set(jobId, {
            status: 'completed',
            result: {
              job_id: jobId,
              query,
              stats: completeEvent.stats,
              businesses: completeEvent.businesses || businesses
            },
            startedAt: jobStore.get(jobId)!.startedAt,
            completedAt: new Date()
          });
        }

        if (event.type === 'error') {
          jobStore.set(jobId, {
            status: 'error',
            error: (event as any).message,
            startedAt: jobStore.get(jobId)!.startedAt,
            completedAt: new Date()
          });
        }
      }
    } catch (e) {
      const errorMsg = e instanceof Error ? e.message : String(e);
      await stream.writeSSE({
        event: 'error',
        data: JSON.stringify({ type: 'error', message: errorMsg })
      });
      jobStore.set(jobId, {
        status: 'error',
        error: errorMsg,
        startedAt: jobStore.get(jobId)!.startedAt,
        completedAt: new Date()
      });
    }
  });
});

// ============================================================================
// POST /scrape - Lancement synchrone
// ============================================================================

scrapeRouter.post('/', async (c) => {
  const body = await c.req.json().catch(() => ({}));

  const validation = ScrapeQuerySchema.safeParse(body);

  if (!validation.success) {
    return c.json({
      success: false,
      error: 'Paramètres invalides',
      details: validation.error.flatten(),
      usage: {
        activity: 'Type de business (ex: "restaurant", "coiffeur", "agence immobiliere")',
        city: `Ville (${SUPPORTED_CITIES.slice(0, 5).join(', ')}...)`,
        grid_size: 'Taille de la grille 1-55 (défaut: 4)'
      },
      timestamp: new Date().toISOString()
    }, 400);
  }

  const query = validation.data;
  const jobId = uuidv4();

  jobStore.set(jobId, {
    status: 'running',
    startedAt: new Date()
  });

  const scraper = new ScraperService();

  try {
    const result = await scraper.scrape({
      activity: query.activity,
      city: query.city,
      gridSize: query.grid_size
    });

    const scrapeResult: ScrapeResult = {
      job_id: jobId,
      query,
      stats: result.stats!,
      businesses: result.businesses
    };

    jobStore.set(jobId, {
      status: 'completed',
      result: scrapeResult,
      startedAt: jobStore.get(jobId)!.startedAt,
      completedAt: new Date()
    });

    return c.json({
      success: true,
      job_id: scrapeResult.job_id,
      query: scrapeResult.query,
      stats: scrapeResult.stats,
      businesses: scrapeResult.businesses,
      total_businesses: scrapeResult.businesses.length,
      timestamp: new Date().toISOString()
    });

  } catch (e) {
    const errorMsg = e instanceof Error ? e.message : String(e);
    jobStore.set(jobId, {
      status: 'error',
      error: errorMsg,
      startedAt: jobStore.get(jobId)!.startedAt,
      completedAt: new Date()
    });

    return c.json({
      success: false,
      error: errorMsg,
      timestamp: new Date().toISOString()
    }, 500);
  }
});

// ============================================================================
// POST /scrape/webhook - Mode webhook pour n8n
// ============================================================================

scrapeRouter.post('/webhook', async (c) => {
  const body = await c.req.json().catch(() => ({}));

  const validation = WebhookScrapeSchema.safeParse(body);

  if (!validation.success) {
    return c.json({
      success: false,
      error: 'Paramètres invalides',
      details: validation.error.flatten(),
      usage: {
        activity: 'Type de business (ex: "restaurant", "coiffeur")',
        city: `Ville (${SUPPORTED_CITIES.slice(0, 5).join(', ')}...)`,
        grid_size: 'Taille de la grille 1-55 (défaut: 4)',
        webhook_url: 'URL du webhook n8n à appeler quand terminé'
      },
      timestamp: new Date().toISOString()
    }, 400);
  }

  const { activity, city, grid_size, webhook_url } = validation.data;
  const jobId = uuidv4();

  jobStore.set(jobId, {
    status: 'running',
    startedAt: new Date()
  });

  (async () => {
    const scraper = new ScraperService();

    try {
      const result = await scraper.scrape({
        activity,
        city,
        gridSize: grid_size
      });

      const scrapeResult: ScrapeResult = {
        job_id: jobId,
        query: { activity, city, grid_size },
        stats: result.stats!,
        businesses: result.businesses
      };

      jobStore.set(jobId, {
        status: 'completed',
        result: scrapeResult,
        startedAt: jobStore.get(jobId)!.startedAt,
        completedAt: new Date()
      });

      await fetch(webhook_url, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          success: true,
          job_id: jobId,
          activity,
          city,
          grid_size,
          stats: scrapeResult.stats,
          businesses: scrapeResult.businesses,
          total_businesses: scrapeResult.businesses.length,
          timestamp: new Date().toISOString()
        })
      });

    } catch (e) {
      const errorMsg = e instanceof Error ? e.message : String(e);

      jobStore.set(jobId, {
        status: 'error',
        error: errorMsg,
        startedAt: jobStore.get(jobId)!.startedAt,
        completedAt: new Date()
      });

      try {
        await fetch(webhook_url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            success: false,
            job_id: jobId,
            activity,
            city,
            error: errorMsg,
            timestamp: new Date().toISOString()
          })
        });
      } catch {
        console.error(`Impossible d'appeler le webhook: ${webhook_url}`);
      }
    }
  })();

  return c.json({
    success: true,
    job_id: jobId,
    message: 'Scraping lancé en arrière-plan. Les résultats seront envoyés au webhook.',
    webhook_url,
    timestamp: new Date().toISOString()
  });
});

// ============================================================================
// GET /scrape/:jobId - Récupérer un job
// ============================================================================

scrapeRouter.get('/:jobId', (c) => {
  const jobId = c.req.param('jobId');
  const job = jobStore.get(jobId);

  if (!job) {
    return c.json({
      success: false,
      error: 'Job non trouvé',
      timestamp: new Date().toISOString()
    }, 404);
  }

  return c.json({
    success: true,
    data: {
      job_id: jobId,
      status: job.status,
      started_at: job.startedAt.toISOString(),
      completed_at: job.completedAt?.toISOString(),
      result: job.result,
      error: job.error
    },
    timestamp: new Date().toISOString()
  });
});

// ============================================================================
// GET /scrape/info/cities - Liste des villes
// ============================================================================

scrapeRouter.get('/info/cities', (c) => {
  return c.json({
    success: true,
    data: {
      cities: SUPPORTED_CITIES,
      count: SUPPORTED_CITIES.length
    },
    timestamp: new Date().toISOString()
  });
});

export { scrapeRouter };
