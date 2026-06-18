#!/usr/bin/env python3
"""
Helper functions to log emails sent to contacts
"""

from datetime import datetime
from typing import Optional
import uuid

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from databases import Base, EmailLog, Contact, Customer
import os
from dotenv import load_dotenv

load_dotenv()

DB_URL = os.getenv("DATABASE_URL", "sqlite:///fortlo.db")
engine = create_engine(DB_URL, echo=False, future=True)
Session = sessionmaker(bind=engine)


def log_email_send(
    contact_id: uuid.UUID,
    customer_id: uuid.UUID,
    email_address: str,
    subject: str,
    body: Optional[str] = None,
    campaign_name: Optional[str] = None,
    external_message_id: Optional[str] = None,
    tenant_id: Optional[uuid.UUID] = None,
    metadata: Optional[dict] = None,
) -> EmailLog:
    """
    Log an email send to the database.
    
    Args:
        contact_id: UUID of the contact
        customer_id: UUID of the customer/company
        email_address: Email address the message was sent to
        subject: Email subject line
        body: Email body (optional)
        campaign_name: Name of the campaign (optional)
        external_message_id: ID from external email service (SendGrid, etc.)
        tenant_id: Tenant ID for multi-tenancy
        metadata: Free-form metadata dict
    
    Returns:
        The created EmailLog record
    """
    session = Session()
    
    # if tenant_id not provided, try to get from env
    if not tenant_id:
        tenant_str = os.getenv("TENANT_ID")
        if tenant_str:
            try:
                tenant_id = uuid.UUID(tenant_str)
            except ValueError:
                pass
    
    email_log = EmailLog(
        contact_id=contact_id,
        customer_id=customer_id,
        email_address=email_address,
        subject=subject,
        body=body,
        sent_at=datetime.utcnow(),
        delivery_status="sent",
        campaign_name=campaign_name,
        external_message_id=external_message_id,
        metadata_json=metadata or {},
        tenant_id=tenant_id,
    )
    
    session.add(email_log)
    session.commit()
    result = email_log
    session.close()
    
    return result


def check_email_sent(
    contact_id: uuid.UUID, 
    tenant_id: Optional[uuid.UUID] = None
) -> bool:
    """
    Check if an email has ever been sent to a contact.
    
    Returns:
        True if at least one email was sent, False otherwise
    """
    session = Session()
    
    if not tenant_id:
        tenant_str = os.getenv("TENANT_ID")
        if tenant_str:
            try:
                tenant_id = uuid.UUID(tenant_str)
            except ValueError:
                pass
    
    query = session.query(EmailLog).filter(EmailLog.contact_id == contact_id)
    if tenant_id:
        query = query.filter(EmailLog.tenant_id == tenant_id)
    
    result = query.first() is not None
    session.close()
    return result


def get_email_history(
    contact_id: uuid.UUID,
    tenant_id: Optional[uuid.UUID] = None,
):
    """
    Get all emails sent to a contact.
    
    Returns:
        List of EmailLog records
    """
    session = Session()
    
    if not tenant_id:
        tenant_str = os.getenv("TENANT_ID")
        if tenant_str:
            try:
                tenant_id = uuid.UUID(tenant_str)
            except ValueError:
                pass
    
    query = session.query(EmailLog).filter(EmailLog.contact_id == contact_id)
    if tenant_id:
        query = query.filter(EmailLog.tenant_id == tenant_id)
    
    results = query.order_by(EmailLog.sent_at.desc()).all()
    session.close()
    return results


def update_delivery_status(
    email_log_id: uuid.UUID,
    status: str,
    status_details: Optional[str] = None,
) -> EmailLog:
    """
    Update the delivery status of an email log entry.
    
    Args:
        email_log_id: UUID of the EmailLog
        status: New status (e.g., 'bounced', 'opened', 'clicked', 'failed')
        status_details: Additional details about the status change
    
    Returns:
        Updated EmailLog record
    """
    session = Session()
    
    email_log = session.query(EmailLog).filter(EmailLog.id == email_log_id).first()
    if email_log:
        email_log.delivery_status = status
        if status_details:
            email_log.status_details = status_details
        session.commit()
    
    result = email_log
    session.close()
    return result


if __name__ == "__main__":
    # Example usage
    print("EmailLog helper functions loaded.")
    print("Functions available:")
    print("  - log_email_send()")
    print("  - check_email_sent()")
    print("  - get_email_history()")
    print("  - update_delivery_status()")
