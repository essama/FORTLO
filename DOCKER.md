# FORTLO Email Outreach Docker Setup

## Quick Start

### 1. Build the Docker image
```bash
docker build -t fortlo-outreach .
```

### 2. Prepare your environment
Create a `.env` file in the project root with your credentials:
```
TENANT_ID=your_tenant_id
CLIENT_ID=your_client_id
SENDER_UPN=your_email@company.com
CLIENT_SECRET=your_client_secret
CSV_PATH=data/mdg_high_intent.csv
DAILY_LIMIT=50
```

### 3. Prepare data directory
Create a `data/` folder and place your CSV file:
```
data/
  mdg_high_intent.csv
```

### 4. Run with Docker Compose
```bash
docker-compose up
```

Or run standalone:
```bash
docker run -it \
  --env-file .env \
  -v $(pwd)/data:/app/data:ro \
  -v $(pwd)/db:/app/db \
  -v $(pwd)/logs:/app/logs \
  fortlo-outreach
```

## Volumes
- `.env` - Read-only environment configuration
- `data/` - CSV input files (read-only)
- `db/` - SQLite database output (persists on host)
- `logs/` - Log files (persists on host)

## Debugging
To run bash inside container:
```bash
docker run -it --entrypoint bash fortlo-outreach
```

Or with docker-compose:
```bash
docker-compose run fortlo-email bash
```

## Notes
- Change `CSV_PATH` in `.env` to match your mounted data directory structure
- Database (`outreach_log.sqlite`) will be created in `/app/db` (mapped to `./db` on host)
- Logs written to `/app/logs` (mapped to `./logs` on host)
