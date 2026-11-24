# Build stage
FROM python:3.12-slim-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libvips-dev \
    && rm -rf /var/lib/apt/lists/*

# Set up virtual environment
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Final stage
FROM python:3.12-slim-bookworm

# Install only runtime dependencies
# Removed the extras from libvips to reduce image
RUN apt-get update && apt-get install -y --no-install-recommends \
    libvips42 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Bound nested concurrency and glibc arena growth. Both values remain
# overridable at deployment time for larger containers.
ENV PROCESS_WORKERS=8 \
    VIPS_CONCURRENCY=2 \
    MALLOC_ARENA_MAX=2


WORKDIR /app
RUN mkdir -p /app/temp/downloads /app/temp/outputs && \
    chmod -R 777 /app/temp
RUN mkdir -p /data && chmod 777 /data
VOLUME [ "/data" ]

# Copy application code
COPY ./app ./app

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
