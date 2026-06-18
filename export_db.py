#!/usr/bin/env python3
"""
Export customers and contacts from the database to database_copy.csv
"""

import os
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from databases import Customer, Contact
from dotenv import load_dotenv

load_dotenv()

# database URL
DB_URL = os.getenv("DATABASE_URL", "sqlite:///fortlo.db")
engine = create_engine(DB_URL, echo=False, future=True)
Session = sessionmaker(bind=engine)


def export_to_csv(output_path: str = "database_copy.csv"):
    """
    Read all customers and contacts from DB and export to CSV.
    CSV columns map back to the original fields:
    ID, Company, Person, Role, LinkedIn URL, Official Company Email, Notes,
    apollo_match_status, apollo_person_id, apollo_email, apollo_phone,
    apollo_title, apollo_company, apollo_linkedin_url
    """
    session = Session()

    rows = []

    # query all customers with their contacts
    customers = session.query(Customer).all()

    for cust in customers:
        # get customer metadata
        cust_meta = cust.metadata_json or {}

        # if no contacts, still add the customer row
        if not cust.contacts:
            row = {
                "ID": cust_meta.get("ID", ""),
                "Company": cust.name,
                "Person": "",
                "Role": "",
                "LinkedIn URL": "",
                "Official Company Email": "",
                "Notes": cust_meta.get("notes", ""),
                "apollo_match_status": "",
                "apollo_person_id": "",
                "apollo_email": "",
                "apollo_phone": "",
                "apollo_title": "",
                "apollo_company": cust.name,
                "apollo_linkedin_url": "",
            }
            rows.append(row)
        else:
            # add a row for each contact
            for contact in cust.contacts:
                contact_meta = contact.metadata_json or {}

                row = {
                    "ID": contact_meta.get("ID", ""),
                    "Company": cust.name,
                    "Person": f"{contact.first_name} {contact.last_name}",
                    "Role": contact.job_title or "",
                    "LinkedIn URL": contact_meta.get("linkedin", ""),
                    "Official Company Email": contact.email,
                    "Notes": contact_meta.get("Notes", ""),
                    "apollo_match_status": contact_meta.get("apollo_match_status", ""),
                    "apollo_person_id": contact_meta.get("apollo_person_id", ""),
                    "apollo_email": contact.email,
                    "apollo_phone": contact.phone or contact_meta.get("apollo_phone", ""),
                    "apollo_title": contact.job_title or contact_meta.get("apollo_title", ""),
                    "apollo_company": cust.name,
                    "apollo_linkedin_url": contact_meta.get("apollo_linkedin_url", ""),
                }
                rows.append(row)

    session.close()

    # create dataframe and save to CSV
    df = pd.DataFrame(rows)
    df.to_csv(output_path, index=False)
    print(f"Exported {len(rows)} rows to {output_path}")


if __name__ == "__main__":
    export_to_csv()
