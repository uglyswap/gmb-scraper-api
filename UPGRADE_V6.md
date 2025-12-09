# GMB Scraper v6.0 - Upgrade Guide

## Problem Fixed
The scraper was crashing after ~1 hour when using maximum grid precision (3025 zones = 55x55 grid).

## Root Causes Identified
1. **Memory Accumulation**: DataStore kept all place_ids and businesses in memory without flush
2. **Playwright Memory Leaks**: Browser contexts accumulated handles over thousands of zones
3. **No Garbage Collection**: Python GC was never explicitly called

## Solution Implemented

### 1. Redis DataStore
- External storage for place_ids and businesses
- Automatic expiration after 24 hours
- Falls back to in-memory if Redis unavailable

### 2. Batch Processing
- Large grids (>200 zones) are split into batches of 200
- Each batch: Phase 1 (scan) → Phase 2 (extract) → Flush
- Progress streamed to client after each batch

### 3. Browser Restart
- Browser restarts every 3 batches to clear Playwright memory
- Forced restart if memory exceeds 1500MB

### 4. Garbage Collection
- `gc.collect()` called after each batch
- Memory monitoring with psutil

## New Dependencies
```bash
pip install redis psutil
```

## Environment Variables
```
REDIS_URL=redis://localhost:6379/0  # Redis connection (optional)
```

## Configuration (gmb_scraper_production.py)
```python
BATCH_SIZE = 200              # Zones per batch
BROWSER_RESTART_INTERVAL = 3  # Restart browser every N batches
GC_INTERVAL = 1               # GC every N batches
MEMORY_LIMIT_MB = 1500        # Force restart if exceeded
```

## Docker Setup with Redis
```yaml
# docker-compose.yml
services:
  scraper:
    build: .
    environment:
      - REDIS_URL=redis://redis:6379/0
    depends_on:
      - redis

  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
```

## Without Redis
The scraper works without Redis using in-memory fallback, but for 3025+ zones, Redis is strongly recommended to prevent memory issues.

## Expected Behavior with 3025 zones
- 16 batches of ~200 zones each
- ~7-10 minutes per batch
- Total time: ~2-3 hours (stable, no crash)
- Memory stays under 500MB with Redis
