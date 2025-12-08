# Stage 1: Build TypeScript
FROM node:20-slim AS builder

WORKDIR /app

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy source files
COPY tsconfig.json ./
COPY src ./src

# Stage 2: Production image with Playwright
FROM mcr.microsoft.com/playwright:v1.49.0-jammy

WORKDIR /app

# Install Node.js 20 and Python pip
RUN apt-get update && apt-get install -y \
    curl \
    python3-pip \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y nodejs \
    && rm -rf /var/lib/apt/lists/*

# Copy built files from builder
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/package*.json ./
COPY --from=builder /app/src ./src
COPY --from=builder /app/tsconfig.json ./

# Copy scraper and public files
COPY scraper ./scraper
COPY public ./public

# Install Python dependencies (playwright + aiohttp for parallel email extraction)
# Note: Playwright browsers are already installed in the base image
RUN pip3 install playwright aiohttp --break-system-packages

# Expose port
ENV PORT=3000
EXPOSE 3000

# Start the application
CMD ["npx", "tsx", "src/index.ts"]
