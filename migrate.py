import os
import uuid
import pandas as pd

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from databases import Base, Customer, Contact

# load environment variables if needed
from dotenv import load_dotenv
load_dotenv()

# database URL/config
DB_URL = os.getenv("DATABASE_URL", "sqlite:///fortlo.db")
engine = create_engine(DB_URL, echo=False, future=True)
# ensure tables exist
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)


def split_name(full_name: str):
    if not isinstance(full_name, str):
        return (None, None)
    parts = [p for p in full_name.strip().split() if p]
    if len(parts) < 2:
        return (None, None)
    return parts[0], parts[-1]


def migrate_csv(path: str):
    df = pd.read_csv(path)
    tenant_id = None
    tstr = os.getenv("TENANT_ID")
    if tstr:
        try:
            tenant_id = uuid.UUID(tstr)
        except Exception:
            pass

    session = Session()

    for _, row in df.iterrows():
        company = row.get("Company") or row.get("apollo_company")
        if not company or not isinstance(company, str):
            continue
        company = company.strip()

        # find or create customer
        q = session.query(Customer).filter(Customer.name == company)
        if tenant_id is not None:
            q = q.filter(Customer.tenant_id == tenant_id)
        cust = q.first()
        if not cust:
            cust = Customer(name=company)
            if tenant_id is not None:
                cust.tenant_id = tenant_id
            # capture notes or other customer-level metadata
            notes = row.get("Notes")
            if notes and isinstance(notes, str):
                cust.metadata_json = {"notes": notes}
            session.add(cust)
            session.flush()
        else:
            # update notes if new
            notes = row.get("Notes")
            if notes and isinstance(notes, str):
                existing_meta = cust.metadata_json or {}
                if existing_meta.get("notes") != notes:
                    existing_meta["notes"] = notes
                    cust.metadata_json = existing_meta

        # contact details
        persona = str(row.get("Person", ""))
        first, last = split_name(persona)
        email = (row.get("apollo_email") or row.get("Official Company Email") or "").strip()
        job = row.get("apollo_title") or row.get("Role") or None
        phone = row.get("apollo_phone") or None

        if first and last and email:
            # check for duplicate by email
            q2 = session.query(Contact).filter(Contact.email == email)
            if tenant_id is not None:
                q2 = q2.filter(Contact.tenant_id == tenant_id)
            existing = q2.first()
            if not existing:
                contact = Contact(
                    customer_id=cust.id,
                    first_name=first,
                    last_name=last,
                    email=email,
                    job_title=job,
                    phone=phone,
                )
                if tenant_id is not None:
                    contact.tenant_id = tenant_id
                # metadata: linkedin, match status, person id, apollo values, notes
                meta = {}
                if row.get("LinkedIn URL"):
                    meta["linkedin"] = row.get("LinkedIn URL")
                for key in [
                    "ID", "apollo_match_status", "apollo_person_id", "apollo_phone",
                    "apollo_title", "apollo_linkedin_url", "Notes"
                ]:
                    val = row.get(key)
                    if val or isinstance(val, str):
                        meta[key] = val
                if meta:
                    contact.metadata_json = meta
                session.add(contact)

    session.commit()
    session.close()


if __name__ == "__main__":
    migrate_csv("1_contacts_filter.csv")
    print("Migration complete.")
    