import { z } from 'zod';

// ============================================================================
// SCHEMAS ZOD
// ============================================================================

export const ScrapeQuerySchema = z.object({
  activity: z.string().min(2, 'Activité requise (min 2 caractères)'),
  city: z.string().min(2, 'Ville requise (min 2 caractères)'),
  grid_size: z.coerce.number().int().min(1).max(55).default(4),
});

export const BusinessSchema = z.object({
  name: z.string(),
  place_id: z.string(),
  address: z.string(),
  phone: z.string(),
  phone_clean: z.string(),
  email: z.string(),
  website: z.string(),
  rating: z.number().nullable(),
  review_count: z.number(),
  latitude: z.number().nullable(),
  longitude: z.number().nullable(),
  category: z.string(),
  google_maps_url: z.string(),
});

export const StatsSchema = z.object({
  total: z.number(),
  with_phone: z.number(),
  with_email: z.number(),
  with_website: z.number(),
  with_address: z.number(),
  with_category: z.number(),
  with_rating: z.number(),
  filtered_out: z.number(),
  duration_seconds: z.number(),
});

// ============================================================================
// TYPES DERIVES
// ============================================================================

export type ScrapeQuery = z.infer<typeof ScrapeQuerySchema>;
export type Business = z.infer<typeof BusinessSchema>;
export type Stats = z.infer<typeof StatsSchema>;

// ============================================================================
// TYPES D'EVENEMENTS SSE
// ============================================================================

export interface SSEEvent {
  type: string;
  timestamp: string;
  [key: string]: unknown;
}

export interface StartEvent extends SSEEvent {
  type: 'start';
  activity: string;
  city: string;
  grid_size: number;
  total_zones: number;
}

export interface ProgressEvent extends SSEEvent {
  type: 'progress';
  zone: number;
  total_zones: number;
  zoom: number;
  new_businesses: number;
  total_businesses: number;
  percent: number;
}

export interface BusinessEvent extends SSEEvent {
  type: 'business';
  data: Business;
}

export interface CompleteEvent extends SSEEvent {
  type: 'complete';
  stats: Stats;
  businesses: Business[];
}

export interface ErrorEvent extends SSEEvent {
  type: 'error';
  message: string;
}

// ============================================================================
// TYPES DE REPONSE API
// ============================================================================

export interface ApiResponse<T> {
  success: boolean;
  data?: T;
  error?: string;
  timestamp: string;
}

export interface ScrapeResult {
  job_id: string;
  query: ScrapeQuery;
  stats: Stats;
  businesses: Business[];
}

export interface HealthResponse {
  status: 'ok' | 'error';
  version: string;
  uptime_seconds: number;
  python_available: boolean;
}

// ============================================================================
// VILLES SUPPORTEES
// ============================================================================

export const SUPPORTED_CITIES = [
  'paris', 'lyon', 'marseille', 'toulouse', 'nice', 'nantes',
  'strasbourg', 'montpellier', 'bordeaux', 'lille', 'rennes',
  'reims', 'toulon', 'grenoble', 'dijon', 'angers', 'nimes',
  'aix-en-provence', 'clermont-ferrand', 'le havre', 'rouen',
  'brest', 'tours', 'amiens', 'limoges', 'metz', 'besancon',
  'perpignan', 'orleans', 'caen', 'mulhouse', 'nancy',
  'saint-etienne', 'avignon', 'cannes', 'antibes'
] as const;

export type SupportedCity = typeof SUPPORTED_CITIES[number];
