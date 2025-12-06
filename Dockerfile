# Multi-stage build for GMB Scraper API
# Node.js API + Python Playwright Scraper

FROM node:20-slim AS builder

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install Node.js dependencies
RUN npm ci --only=production

# Copy source code
COPY tsconfig.json ./
COPY src ./src

# Build TypeScript
RUN npm run build || true

# Production image with Python + Playwright
FROM mcr.microsoft.com/playwright:v1.40.0-jammy

# Install Node.js
RUN apt-get update && apt-get install -y \
    curl \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy Node.js dependencies and built files
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/dist ./dist 2>/dev/null || true
COPY --from=builder /app/package*.json ./

# Copy source files (for tsx runtime if dist doesn't exist)
COPY src ./src
COPY tsconfig.json ./

# Copy Python scraper
COPY scraper ./scraper

# Install Python dependencies
RUN pip install playwright && \
    playwright install chromium && \
    playwright install-deps chromium

# Environment variables
ENV NODE_ENV=production
ENV PORT=3000
ENV PYTHON_PATH=python3

# Expose port
EXPOSE 3000

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:3000/health || exit 1

# Start the API
CMD ["npx", "tsx", "src/index.ts"]
