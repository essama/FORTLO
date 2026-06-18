"""
CRM web application.

What this app does:
- Runs a FastAPI-based UI for browsing customers and contacts.
- Provides searchable and paginated views using data from the main database.
- Exposes endpoints that render HTML templates for internal CRM operations.
"""

import os
import math
import uuid
from typing import Optional

from dotenv import load_dotenv
from fastapi import FastAPI, Request, Query, Body
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from sqlalchemy import create_engine, select, func, or_, String, inspect, text
from sqlalchemy.orm import Session, sessionmaker

from databases import Customer, Contact, EmailLog  # import models for UI

load_dotenv()

# use the same main database that holds customers and email logs
#DATABASE_URL = os.getenv("DB_PATH", "outreach_log.sqlite")
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./fortlo.db")

if not DATABASE_URL:
    raise RuntimeError("Missing DATABASE_URL. Put it in your .env file.")

engine = create_engine(DATABASE_URL, future=True, pool_pre_ping=True)
# ensure our ORM models have corresponding tables
from databases import Base
Base.metadata.create_all(engine)

SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)

CLIENT_PHASE_OPTIONS = (
    "new",
    "contacted",
    "qualified",
    "proposal",
    "negotiation",
    "won",
    "lost",
)


def ensure_customer_schema() -> None:
    inspector = inspect(engine)
    if "customers" not in inspector.get_table_names():
        return

    existing_columns = {column["name"] for column in inspector.get_columns("customers")}
    alter_statements = []

    if "last_client_status" not in existing_columns:
        alter_statements.append("ALTER TABLE customers ADD COLUMN last_client_status TEXT")
    if "client_phase" not in existing_columns:
        alter_statements.append("ALTER TABLE customers ADD COLUMN client_phase VARCHAR(50)")

    if not alter_statements:
        return

    with engine.begin() as connection:
        for statement in alter_statements:
            connection.execute(text(statement))


ensure_customer_schema()

app = FastAPI(title="CRM UI")
templates = Jinja2Templates(directory="templates")


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    # small landing redirect-style page
    return templates.TemplateResponse(
        "customers.html",
        {
            "request": request,
            "customers": [],
            "contact_counts": {},
            "contact_names": {},
            "q": "",
            "tenant_id": "",
            "include_contact_names": False,
            "client_phase_options": CLIENT_PHASE_OPTIONS,
            "page": 1,
            "page_size": 20,
            "total": 0,
            "pages": 0,
        },
    )


@app.get("/customers", response_class=HTMLResponse)
def list_customers(
    request: Request,
    q: str = Query(default="", description="Search text"),
    tenant_id: str = Query(default="", description="Tenant UUID filter"),
    include_contact_names: bool = Query(default=False, description="Search and list contact names"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=5, le=200),
):
    q = (q or "").strip()

    tenant_uuid: Optional[uuid.UUID] = None
    if tenant_id.strip():
        try:
            tenant_uuid = uuid.UUID(tenant_id.strip())
        except ValueError:
            tenant_uuid = None  # ignore invalid tenant_id in UI

    with SessionLocal() as db:
        db: Session
        stmt = select(Customer)

        # tenant filter
        if tenant_uuid:
            stmt = stmt.where(Customer.tenant_id == tenant_uuid)

        # search across customer fields and optionally related contact names
        if q:
            like = f"%{q}%"
            search_filters = [
                Customer.name.ilike(like),
                Customer.industry.ilike(like),
                Customer.country.ilike(like),
                Customer.city.ilike(like),
                Customer.website.ilike(like),
            ]
            if include_contact_names:
                contact_name_filters = [
                    Contact.first_name.ilike(like),
                    Contact.last_name.ilike(like),
                    (func.coalesce(Contact.first_name, "") + " " + func.coalesce(Contact.last_name, "")).cast(String).ilike(like),
                ]
                search_filters.append(Customer.contacts.any(or_(*contact_name_filters)))
            stmt = stmt.where(
                or_(*search_filters)
            )

        # total count for pagination
        count_stmt = select(func.count()).select_from(stmt.subquery())
        total = db.execute(count_stmt).scalar_one()

        pages = max(1, math.ceil(total / page_size)) if total else 0
        page = min(page, pages) if pages else 1
        offset = (page - 1) * page_size

        stmt = stmt.order_by(Customer.created_at.desc()).offset(offset).limit(page_size)

        customers = db.execute(stmt).scalars().all()

        # count contacts per customer for display
        contact_counts = {}
        contact_names = {}
        if customers:
            cust_ids = [c.id for c in customers]
            rows = db.execute(
                select(Contact.customer_id, func.count()).where(Contact.customer_id.in_(cust_ids)).group_by(Contact.customer_id)
            ).all()
            contact_counts = {cid: cnt for cid, cnt in rows}

            if include_contact_names:
                name_rows = db.execute(
                    select(
                        Contact.customer_id,
                        Contact.first_name,
                        Contact.last_name,
                        Contact.job_title,
                    )
                    .where(Contact.customer_id.in_(cust_ids))
                    .order_by(Contact.last_name, Contact.first_name)
                ).all()

                for customer_id_value, first_name, last_name, job_title in name_rows:
                    full_name = " ".join(part for part in [first_name, last_name] if part).strip()
                    if not full_name:
                        continue
                    contact_names.setdefault(customer_id_value, []).append(
                        {
                            "name": full_name,
                            "job_title": job_title or "",
                        }
                    )

    return templates.TemplateResponse(
        "customers.html",
        {
            "request": request,
            "customers": customers,
            "contact_counts": contact_counts,
            "contact_names": contact_names,
            "q": q,
            "tenant_id": tenant_id,
            "include_contact_names": include_contact_names,
            "client_phase_options": CLIENT_PHASE_OPTIONS,
            "page": page,
            "page_size": page_size,
            "total": total,
            "pages": pages,
        },
    )


@app.get("/contacts", response_class=HTMLResponse)
def list_contacts(
    request: Request,
    q: str = Query(default="", description="Search text (name/email/title)"),
    customer_id: str = Query(default="", description="Filter by customer UUID"),
    page: int = Query(default=1, ge=1),
    page_size: int = Query(default=20, ge=5, le=200),
):
    q = (q or "").strip()
    cust_uuid = None
    if customer_id.strip():
        try:
            cust_uuid = uuid.UUID(customer_id.strip())
        except ValueError:
            cust_uuid = None

    with SessionLocal() as db:
        db: Session
        from sqlalchemy.orm import joinedload
        stmt = select(Contact).options(joinedload(Contact.customer))
        if cust_uuid:
            stmt = stmt.where(Contact.customer_id == cust_uuid)
        if q:
            like = f"%{q}%"
            stmt = stmt.where(
                or_(
                    Contact.first_name.ilike(like),
                    Contact.last_name.ilike(like),
                    Contact.email.ilike(like),
                    Contact.job_title.ilike(like),
                )
            )
        count_stmt = select(func.count()).select_from(stmt.subquery())
        total = db.execute(count_stmt).scalar_one()

        pages = max(1, math.ceil(total / page_size)) if total else 0
        page = min(page, pages) if pages else 1
        offset = (page - 1) * page_size

        stmt = stmt.order_by(Contact.last_name, Contact.first_name).offset(offset).limit(page_size)
        contacts = db.execute(stmt).scalars().all()

        # email log counts for these contacts
        sent_counts = {}
        if contacts:
            cids = [c.id for c in contacts]
            rows = db.execute(
                select(EmailLog.contact_id, func.count()).where(EmailLog.contact_id.in_(cids)).group_by(EmailLog.contact_id)
            ).all()
            sent_counts = {cid: cnt for cid, cnt in rows}

    return templates.TemplateResponse(
        "contacts.html",
        {
            "request": request,
            "contacts": contacts,
            "sent_counts": sent_counts,
            "q": q,
            "customer_id": customer_id,
            "page": page,
            "page_size": page_size,
            "total": total,
            "pages": pages,
        },
    )

@app.post("/api/customers/{customer_id}/toggle-campaign")
def toggle_customer_campaign(customer_id: str):
    """Toggle the in_email_campaign flag for a customer"""
    try:
        cust_uuid = uuid.UUID(customer_id)
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid customer ID"})
    
    with SessionLocal() as db:
        db: Session
        customer = db.query(Customer).filter(Customer.id == cust_uuid).first()
        if not customer:
            return JSONResponse(status_code=404, content={"error": "Customer not found"})
        
        # Toggle the flag
        customer.in_email_campaign = not customer.in_email_campaign
        db.commit()
        
        return JSONResponse({
            "id": str(customer.id),
            "name": customer.name,
            "in_email_campaign": customer.in_email_campaign
        })


@app.post("/api/customers/{customer_id}/details")
def update_customer_details(customer_id: str, payload: dict = Body(...)):
    try:
        cust_uuid = uuid.UUID(customer_id)
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "Invalid customer ID"})

    last_client_status = (payload.get("last_client_status") or "").strip()
    client_phase = (payload.get("client_phase") or "").strip().lower()

    if client_phase and client_phase not in CLIENT_PHASE_OPTIONS:
        return JSONResponse(status_code=400, content={"error": "Invalid client phase"})

    with SessionLocal() as db:
        db: Session
        customer = db.query(Customer).filter(Customer.id == cust_uuid).first()
        if not customer:
            return JSONResponse(status_code=404, content={"error": "Customer not found"})

        customer.last_client_status = last_client_status or None
        customer.client_phase = client_phase or None
        db.commit()

        return JSONResponse(
            {
                "id": str(customer.id),
                "name": customer.name,
                "last_client_status": customer.last_client_status or "",
                "client_phase": customer.client_phase or "",
            }
        )