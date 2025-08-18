FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    wget \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Playwright and browser
RUN playwright install chromium
RUN playwright install-deps

# Copy application
COPY cinecalidad_scraper.py .

# Create volume for database
VOLUME ["/app/data"]

# Expose port
EXPOSE 9697

# Set environment variables
ENV DB_FILE=/app/data/cinecalidad_cache.sqlite

CMD ["python", "cinecalidad_scraper.py"]

