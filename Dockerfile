FROM python:3.12-slim

WORKDIR /app

# Install system deps for DNS resolution
RUN apt-get update && apt-get install -y --no-install-recommends \
    dnsutils \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Default port for API
EXPOSE 8025

# Environment variables (override at runtime)
ENV KADENVERIFY_API_KEY=""
ENV KADENVERIFY_HELO_DOMAIN="verify.kadenwood.com"
ENV KADENVERIFY_FROM_ADDRESS="verify@kadenwood.com"
ENV KADENVERIFY_CONCURRENCY="5"

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8025/health')" || exit 1

# Run the API server
CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8025"]
