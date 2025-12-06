import { spawn, ChildProcess } from 'child_process';
import * as path from 'path';
import * as readline from 'readline';
import { config } from '../config.js';
import type { SSEEvent, Business, Stats, ScrapeQuery } from '../types/index.js';

// ============================================================================
// TYPES INTERNES
// ============================================================================

interface ScraperOptions {
  activity: string;
  city: string;
  gridSize?: number;
  onEvent?: (event: SSEEvent) => void;
  onComplete?: (result: ScraperResult) => void;
  onError?: (error: Error) => void;
}

export interface ScraperResult {
  success: boolean;
  query: ScrapeQuery;
  stats?: Stats;
  businesses: Business[];
  error?: string;
}

// ============================================================================
// SCRAPER SERVICE
// ============================================================================

export class ScraperService {
  private process: ChildProcess | null = null;
  private businesses: Business[] = [];
  private stats: Stats | null = null;
  private isRunning = false;

  /**
   * Lance le scraper Python et retourne un AsyncGenerator pour le streaming
   */
  async *stream(options: ScraperOptions): AsyncGenerator<SSEEvent> {
    const { activity, city, gridSize = 4 } = options;

    const scraperPath = path.join(__dirname, '..', '..', 'scraper', 'gmb_scraper_stream.py');

    this.process = spawn(config.pythonPath, [
      '-u', // Unbuffered output
      scraperPath,
      activity,
      city,
      gridSize.toString()
    ], {
      stdio: ['ignore', 'pipe', 'pipe']
    });

    this.isRunning = true;
    this.businesses = [];
    this.stats = null;

    const rl = readline.createInterface({
      input: this.process.stdout!,
      crlfDelay: Infinity
    });

    // Gérer les erreurs stderr
    let stderrBuffer = '';
    this.process.stderr?.on('data', (data) => {
      stderrBuffer += data.toString();
    });

    // Créer une queue d'événements
    const eventQueue: SSEEvent[] = [];
    let resolveNext: ((value: IteratorResult<SSEEvent>) => void) | null = null;
    let isDone = false;

    rl.on('line', (line) => {
      try {
        const event = JSON.parse(line) as SSEEvent;

        // Collecter les businesses et stats
        if (event.type === 'business' && (event as any).data) {
          this.businesses.push((event as any).data);
        }
        if (event.type === 'complete' && (event as any).stats) {
          this.stats = (event as any).stats;
        }

        if (resolveNext) {
          resolveNext({ value: event, done: false });
          resolveNext = null;
        } else {
          eventQueue.push(event);
        }
      } catch {
        // Ignorer les lignes non-JSON
      }
    });

    const cleanup = () => {
      isDone = true;
      this.isRunning = false;
      if (resolveNext) {
        resolveNext({ value: undefined as any, done: true });
        resolveNext = null;
      }
    };

    this.process.on('close', cleanup);
    this.process.on('error', cleanup);

    // Yield les événements
    while (!isDone) {
      if (eventQueue.length > 0) {
        yield eventQueue.shift()!;
      } else {
        const event = await new Promise<SSEEvent | null>((resolve) => {
          if (isDone) {
            resolve(null);
            return;
          }
          resolveNext = (result) => {
            if (result.done) {
              resolve(null);
            } else {
              resolve(result.value);
            }
          };
        });

        if (event === null) break;
        yield event;
      }
    }

    // Yield les événements restants
    while (eventQueue.length > 0) {
      yield eventQueue.shift()!;
    }
  }

  /**
   * Lance le scraper et attend la fin (mode synchrone)
   */
  async scrape(options: ScraperOptions): Promise<ScraperResult> {
    const { activity, city, gridSize = 4 } = options;

    const businesses: Business[] = [];
    let stats: Stats | null = null;
    let error: string | null = null;

    try {
      for await (const event of this.stream({ activity, city, gridSize })) {
        if (event.type === 'business' && (event as any).data) {
          businesses.push((event as any).data);
        }
        if (event.type === 'complete' && (event as any).stats) {
          stats = (event as any).stats;
        }
        if (event.type === 'error') {
          error = (event as any).message;
        }

        // Callback optionnel
        options.onEvent?.(event);
      }
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }

    const result: ScraperResult = {
      success: !error && businesses.length > 0,
      query: { activity, city, grid_size: gridSize },
      stats: stats || undefined,
      businesses,
      error: error || undefined
    };

    return result;
  }

  /**
   * Arrête le scraper en cours
   */
  stop(): void {
    if (this.process) {
      this.process.kill('SIGTERM');
      this.process = null;
    }
    this.isRunning = false;
  }

  /**
   * Vérifie si un scrape est en cours
   */
  get running(): boolean {
    return this.isRunning;
  }
}

// Singleton pour l'API
export const scraperService = new ScraperService();
