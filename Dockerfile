# Elite Web Scraping Dockerfile
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Install system dependencies for Playwright + Xvfb for headless display
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libc6 \
    libcairo2 \
    libcups2 \
    libdbus-1-3 \
    libexpat1 \
    libfontconfig1 \
    libgbm1 \
    libgcc1 \
    libglib2.0-0 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libpango-1.0-0 \
    libpangocairo-1.0-0 \
    libstdc++6 \
    libx11-6 \
    libx11-xcb1 \
    libxcb1 \
    libxcomposite1 \
    libxcursor1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxi6 \
    libxrandr2 \
    libxrender1 \
    libxss1 \
    libxtst6 \
    lsb-release \
    xdg-utils \
    xvfb \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for better caching
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Set Playwright browsers path before installation
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright

# Install Playwright browsers (must be after setting PLAYWRIGHT_BROWSERS_PATH)
RUN mkdir -p /ms-playwright && \
    playwright install chromium && \
    playwright install-deps chromium

# Copy application code
COPY src/ ./src/
COPY .env* ./

# Create directories for data and cache
RUN mkdir -p /app/data /app/.cache/robots

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DISPLAY=:99
ENV HEADLESS=False

# Default command (can be overridden in docker-compose)
CMD ["python", "-m", "src"]

