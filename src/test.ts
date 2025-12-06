/**
 * Script de test de l'API GMB
 *
 * Usage: npm run test
 */

const API_BASE = 'http://localhost:3000';
const API_KEY = process.env.TEST_API_KEY || '';

interface TestResult {
  name: string;
  success: boolean;
  duration: number;
  error?: string;
  data?: any;
}

async function runTest(name: string, fn: () => Promise<any>): Promise<TestResult> {
  const start = Date.now();
  try {
    const data = await fn();
    return {
      name,
      success: true,
      duration: Date.now() - start,
      data
    };
  } catch (e) {
    return {
      name,
      success: false,
      duration: Date.now() - start,
      error: e instanceof Error ? e.message : String(e)
    };
  }
}

async function fetchApi(path: string, options: RequestInit = {}): Promise<any> {
  const headers: Record<string, string> = {
    'Content-Type': 'application/json',
    ...(options.headers as Record<string, string> || {})
  };

  if (API_KEY) {
    headers['X-API-Key'] = API_KEY;
  }

  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers
  });

  const data = await response.json();

  if (!response.ok && !data.success) {
    throw new Error(data.error || `HTTP ${response.status}`);
  }

  return data;
}

// ============================================================================
// TESTS
// ============================================================================

async function testHealth(): Promise<any> {
  const result = await fetchApi('/health');
  if (!result.success) throw new Error('Health check failed');
  if (!result.data.python_available) throw new Error('Python not available');
  return result.data;
}

async function testDocumentation(): Promise<any> {
  const result = await fetchApi('/');
  if (!result.name) throw new Error('Documentation missing name');
  if (!result.endpoints) throw new Error('Documentation missing endpoints');
  return { name: result.name, version: result.version };
}

async function testCities(): Promise<any> {
  const result = await fetchApi('/api/scrape/info/cities');
  if (!result.success) throw new Error('Cities fetch failed');
  if (!result.data.cities || result.data.cities.length === 0) {
    throw new Error('No cities returned');
  }
  return { count: result.data.count };
}

async function testValidation(): Promise<any> {
  // Test avec paramètres manquants
  try {
    await fetchApi('/api/scrape', {
      method: 'POST',
      body: JSON.stringify({})
    });
    throw new Error('Should have failed with empty params');
  } catch (e) {
    if (e instanceof Error && e.message.includes('Paramètres invalides')) {
      return { validation: 'working' };
    }
    throw e;
  }
}

async function testScrapeSmall(): Promise<any> {
  console.log('  → Lancement scrape (grid 1, peut prendre ~30s)...');

  const result = await fetchApi('/api/scrape', {
    method: 'POST',
    body: JSON.stringify({
      activity: 'boulangerie',
      city: 'nice',
      grid_size: 1
    })
  });

  if (!result.success) throw new Error('Scrape failed');
  if (!result.data.businesses || result.data.businesses.length === 0) {
    throw new Error('No businesses returned');
  }

  return {
    total: result.data.stats.total,
    with_phone: result.data.stats.with_phone,
    duration: result.data.stats.duration_seconds
  };
}

async function testStreamSSE(): Promise<any> {
  console.log('  → Test SSE stream (grid 1)...');

  return new Promise(async (resolve, reject) => {
    const url = new URL('/api/scrape/stream', API_BASE);
    url.searchParams.set('activity', 'coiffeur');
    url.searchParams.set('city', 'nice');
    url.searchParams.set('grid_size', '1');
    if (API_KEY) {
      url.searchParams.set('api_key', API_KEY);
    }

    try {
      const response = await fetch(url.toString());

      if (!response.ok) {
        const text = await response.text();
        throw new Error(`SSE failed: ${text}`);
      }

      const reader = response.body?.getReader();
      if (!reader) throw new Error('No reader available');

      const decoder = new TextDecoder();
      let eventCount = 0;
      let hasComplete = false;
      let totalBusinesses = 0;

      const timeout = setTimeout(() => {
        reader.cancel();
        reject(new Error('SSE timeout after 120s'));
      }, 120000);

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        const text = decoder.decode(value);
        const lines = text.split('\n');

        for (const line of lines) {
          if (line.startsWith('data:')) {
            eventCount++;
            try {
              const data = JSON.parse(line.slice(5).trim());
              if (data.type === 'complete') {
                hasComplete = true;
                totalBusinesses = data.stats?.total || 0;
              }
            } catch {}
          }
        }

        if (hasComplete) break;
      }

      clearTimeout(timeout);

      resolve({
        event_count: eventCount,
        has_complete: hasComplete,
        total_businesses: totalBusinesses
      });

    } catch (e) {
      reject(e);
    }
  });
}

// ============================================================================
// MAIN
// ============================================================================

async function main() {
  console.log('\n╔═══════════════════════════════════════════════════════════════╗');
  console.log('║                    GMB API - TESTS                            ║');
  console.log('╚═══════════════════════════════════════════════════════════════╝\n');

  if (!API_KEY) {
    console.log('⚠️  Aucune API_KEY fournie (TEST_API_KEY). Tests en mode dev.\n');
  }

  const tests: Array<{ name: string; fn: () => Promise<any> }> = [
    { name: 'Health Check', fn: testHealth },
    { name: 'Documentation', fn: testDocumentation },
    { name: 'Villes supportées', fn: testCities },
    { name: 'Validation des paramètres', fn: testValidation },
    { name: 'Scrape synchrone (petit)', fn: testScrapeSmall },
    { name: 'Streaming SSE', fn: testStreamSSE },
  ];

  const results: TestResult[] = [];

  for (const test of tests) {
    process.stdout.write(`▶ ${test.name}... `);
    const result = await runTest(test.name, test.fn);
    results.push(result);

    if (result.success) {
      console.log(`✅ OK (${result.duration}ms)`);
      if (result.data) {
        console.log(`  └─ ${JSON.stringify(result.data)}`);
      }
    } else {
      console.log(`❌ FAIL`);
      console.log(`  └─ ${result.error}`);
    }
  }

  // Résumé
  console.log('\n═══════════════════════════════════════════════════════════════');
  const passed = results.filter(r => r.success).length;
  const failed = results.filter(r => !r.success).length;

  if (failed === 0) {
    console.log(`✅ Tous les tests passent (${passed}/${results.length})`);
  } else {
    console.log(`❌ ${failed} test(s) en échec sur ${results.length}`);
    process.exit(1);
  }
}

main().catch(console.error);
