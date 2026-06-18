# This is the EmailLog model addition to be appended to databases.py
# Run: cat emaillog_model.py >> databases.py

from datetime import datetime
from typing import Optional
from sqlalchemy import (
    String,
    DateTime,
    ForeignKey,
    Text,
    JSON,
    Index
)
from sqlalchemy.orm import (
    Mapped,
    mapped_column,
    relationship
)
from sqlalchemy.dialects.postgresql import UUID
import uuid


# =========================================================
# Email Log (Outreach Tracking)
# =========================================================

class EmailLog:
    """
    Tracks all email sends to contacts.
    Used for outreach management and delivery tracking.
    """

    __tablename__ = "email_logs"
    __table_args__ = (
        Index("ix_email_logs_contact", "contact_id"),
        Index("ix_email_logs_customer", "customer_id"),
        Index("ix_email_logs_sent_at", "sent_at"),
        Index("ix_email_logs_status", "delivery_status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    contact_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("contacts.id", ondelete="CASCADE"),
        nullable=False
    )

    customer_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("customers.id", ondelete="CASCADE"),
        nullable=False
    )

    # Email content
    email_address: Mapped[str] = mapped_column(String(255), nullable=False)
    subject: Mapped[str] = mapped_column(String(500), nullable=False)
    body: Mapped[Optional[str]] = mapped_column(Text)

    # Delivery tracking
    sent_at: Mapped[datetime] = mapped_column(DateTime, nullable=False)
    
    # Status options: pending, sent, bounced, opened, clicked, failed, unsubscribed
    delivery_status: Mapped[str] = mapped_column(
        String(50), nullable=False, default="sent"
    )
    
    # Details for failures or status changes
    status_details: Mapped[Optional[str]] = mapped_column(Text)

    # Campaign/campaign tracking
    campaign_name: Mapped[Optional[str]] = mapped_column(String(255))
    
    # External service tracking (e.g., SendGrid, Mailgun IDs)
    external_message_id: Mapped[Optional[str]] = mapped_column(String(255))

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False
    )

    # Free-form metadata
    metadata_json: Mapped[Optional[dict]] = mapped_column(JSON)

    # Relationships
    contact: Mapped["Contact"] = relationship()
    customer: Mapped["Customer"] = relationship()
