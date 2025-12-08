# Multi-stage build for GMB Scraper API
# Node.js API + Python Playwright Scraper

FROM node:20-slim AS builder

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install Node.js dependencies (including dev for tsx)
RUN npm install

# Copy source code
COPY tsconfig.json ./
COPY src ./src

# Production image with Python + Playwright
FROM mcr.microsoft.com/playwright:v1.40.0-jammy

WORKDIR /app

# Install Node.js 20 AND python3-pip (pip3 not included in base image)
RUN apt-get update && \
    apt-get install -y --no-install-recommends curl python3-pip && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy Node.js dependencies and source files
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/src ./src
COPY --from=builder /app/tsconfig.json ./

# Copy Python scraper
COPY scraper ./scraper

# Copy public folder for web frontend
COPY public ./public

# Install Python dependencies (browsers already in base image)
RUN pip3 install playwright aiohttp --break-system-packages

# Environment variables
ENV NODE_ENV=production
ENV PORT=3000
ENV PYTHON_PATH=python3

# Expose port
EXPOSE 3000

# Start the API with tsx (TypeScript runtime)
CMD ["npx", "tsx", "src/index.ts"]
