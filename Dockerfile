FROM python:3.11-slim

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY graph_auth.py outlook_mail.py eligible.py refuel_apollo.py ./
COPY app3.py crm_app.py helpers.py databases.py email_log_helpers.py outreach.py ./
COPY db.py pipeline_crm.py enrich_apollo.py build_accounts.py exclusions.py ./
COPY config.yaml candidates.yaml ./
COPY templates ./templates
COPY leadgen ./leadgen
COPY forte4_logo.png ./
COPY start.sh ./

RUN mkdir -p logger db data logs \
    && chmod +x start.sh \
    && touch msal_token_cache.json outreach_log.sqlite fortlo.db

ENV PYTHONUNBUFFERED=1
ENV TZ=UTC
ENV DATABASE_URL=sqlite:///fortlo.db
ENV DB_PATH=/app/outreach_log.sqlite
ENV TOKEN_CACHE_FILE=/app/msal_token_cache.json

EXPOSE 8000 8010

CMD ["./start.sh"]
