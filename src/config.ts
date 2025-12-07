import * as fs from 'fs';
import * as path from 'path';

// ============================================================================
// CONFIGURATION
// ============================================================================

export const config = {
  port: parseInt(process.env.PORT || '3000', 10),
  pythonPath: process.env.PYTHON_PATH || 'python',
  debug: process.env.DEBUG === 'true',
  version: '2.0.0', // V16 ULTIMATE - API Interception + Dynamic Geocoding + Email Extraction
  startTime: Date.now(),
};

/**
 * Récupère les clés API depuis les variables d'environnement
 */
export function getApiKeys(): string[] {
  const keysEnv = process.env.API_KEYS || '';
  if (!keysEnv.trim()) {
    return [];
  }
  return keysEnv.split(',').map(k => k.trim()).filter(Boolean);
}

/**
 * Chemin vers le script Python du scraper
 */
export function getScraperPath(): string {
  return path.join(__dirname, '..', 'scraper', 'gmb_scraper_stream.py');
}

/**
 * Vérifie si Python est disponible
 */
export async function checkPython(): Promise<boolean> {
  const { spawn } = await import('child_process');

  return new Promise((resolve) => {
    const proc = spawn(config.pythonPath, ['--version']);

    proc.on('error', () => resolve(false));
    proc.on('close', (code) => resolve(code === 0));
  });
}

/**
 * Charge les variables d'environnement depuis .env si présent
 */
export function loadEnv(): void {
  const envPath = path.join(__dirname, '..', '.env');

  if (!fs.existsSync(envPath)) {
    return;
  }

  const content = fs.readFileSync(envPath, 'utf-8');
  const lines = content.split('\n');

  for (const line of lines) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#')) continue;

    const [key, ...valueParts] = trimmed.split('=');
    if (key && valueParts.length > 0) {
      const value = valueParts.join('=').trim();
      // Enlever les guillemets si présents
      const cleanValue = value.replace(/^["']|["']$/g, '');
      if (!process.env[key]) {
        process.env[key] = cleanValue;
      }
    }
  }
}
