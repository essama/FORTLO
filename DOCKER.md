# FORTLO Docker

One container runs the outreach sender (`app3.py`) and the customer CRM UI (`crm_app.py` on port 8000).

## Run on the EC2 host

```bash
cd /home/ec2-user/FORTLO
touch do_not_email.txt
sudo docker compose up -d --build
```

UI: `http://127.0.0.1:8000/customers`

## What the container does

- CRM UI with filters (ICP, campaign, phase, unsent, sent, email status).
- Persistent email sender. It does not exit when the daily company cap is hit.
- On start, and every Berlin morning (07:30), it emails `SENDER_UPN` with remaining contacts until the next refuel.
- If the sendable list is empty it sends one “refuel required” Outlook mail per day, fetches up to 100 Head of ERP contacts from Apollo in the Bechtle €5–10bn German band, then checks again later if still empty.

Required in `.env`: `TENANT_ID`, `CLIENT_ID`, `SENDER_UPN`, `APOLLO_API_KEY`, `DATABASE_URL=sqlite:///fortlo.db`. Optional: `NOTIFY_EMAIL`, `CLIENT_SECRET`, `DAILY_LIMIT`.
