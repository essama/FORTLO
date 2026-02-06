FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY app2.py .
COPY helpers.py .
COPY forte4_logo.png .

# Create logger directory
RUN mkdir -p logger

# Volume mounts for:
# - .env (environment variables)
# - CSV files (input data)
# - outreach_log.sqlite (database output)
# - logger/ (log files)

VOLUME ["/app/.env", "/app/data", "/app/logs"]

# Set .env file location (user should mount it)
ENV PYTHONUNBUFFERED=1

# Run the application
CMD ["python", "app2.py"]
