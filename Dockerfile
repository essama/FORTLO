FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY app2.py .
COPY helpers.py .
COPY forte4_logo.png .

# Create directories for mounted volumes
RUN mkdir -p logger data logs db

# Volume mounts for:
# - .env (environment variables)
# - data/ (CSV input files)
# - db/ (database output)
# - logs/ (log files)

VOLUME ["/app/.env", "/app/data", "/app/db", "/app/logs"]

# Set .env file location (user should mount it)
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "app2.py"]
