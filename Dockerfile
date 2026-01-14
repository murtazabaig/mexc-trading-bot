# Multi-stage build for MEXC Futures Signal Bot
FROM python:3.11-slim as builder

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    make \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements and install Python dependencies
COPY requirements.txt .
RUN pip install --user --no-cache-dir -r requirements.txt

# Final stage
FROM python:3.11-slim

# Create non-root user
RUN groupadd -r mexc && useradd -r -g mexc mexc

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy only necessary files from builder
COPY --from=builder /root/.local /home/mexc/.local
COPY --chown=mexc:mexc . .

# Create directories for logs and data
RUN mkdir -p logs data && chown -R mexc:mexc logs data

# Switch to non-root user
USER mexc

# Update PATH
ENV PATH=/home/mexc/.local/bin:$PATH

# Set Python path to include src directory
ENV PYTHONPATH=/app/src:$PYTHONPATH

# Set environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import sys; sys.exit(0)"

# Default command
CMD ["python", "src/main.py"]