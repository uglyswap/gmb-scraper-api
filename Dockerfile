# Single stage - Playwright image already has Node.js 18 and browsers
FROM mcr.microsoft.com/playwright:v1.49.0-jammy

WORKDIR /app

# Copy package files and install dependencies
COPY package*.json ./
RUN npm install

# Copy all source files
COPY tsconfig.json ./
COPY src ./src
COPY scraper ./scraper
COPY public ./public

# Install Python dependencies (browsers already in base image)
RUN pip3 install playwright aiohttp --break-system-packages

# Expose port
ENV PORT=3000
EXPOSE 3000

# Start the application
CMD ["npx", "tsx", "src/index.ts"]
