import { Context, Next } from 'hono';
import { getApiKeys } from '../config.js';

/**
 * Middleware d'authentification par clé API
 *
 * La clé peut être fournie via:
 * - Header: X-API-Key
 * - Header: Authorization: Bearer <key>
 * - Query param: ?api_key=<key>
 */
export async function apiKeyAuth(c: Context, next: Next) {
  const apiKeys = getApiKeys();

  // Si aucune clé configurée, bypass l'auth (mode dev)
  if (apiKeys.length === 0) {
    console.warn('[AUTH] Aucune clé API configurée - mode développement');
    return next();
  }

  // Récupérer la clé depuis différentes sources
  let providedKey: string | undefined;

  // 1. Header X-API-Key
  providedKey = c.req.header('X-API-Key');

  // 2. Header Authorization: Bearer <key>
  if (!providedKey) {
    const authHeader = c.req.header('Authorization');
    if (authHeader?.startsWith('Bearer ')) {
      providedKey = authHeader.slice(7);
    }
  }

  // 3. Query param api_key
  if (!providedKey) {
    providedKey = c.req.query('api_key');
  }

  // Vérification
  if (!providedKey) {
    return c.json({
      success: false,
      error: 'Clé API requise. Utilisez le header X-API-Key, Authorization: Bearer <key>, ou le paramètre ?api_key=<key>',
      timestamp: new Date().toISOString()
    }, 401);
  }

  if (!apiKeys.includes(providedKey)) {
    return c.json({
      success: false,
      error: 'Clé API invalide',
      timestamp: new Date().toISOString()
    }, 403);
  }

  // Clé valide
  return next();
}

/**
 * Génère une nouvelle clé API aléatoire
 */
export function generateApiKey(): string {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  let key = 'gmb_';
  for (let i = 0; i < 32; i++) {
    key += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return key;
}
