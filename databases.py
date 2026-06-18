from datetime import datetime
from typing import Optional, List
from sqlalchemy import (
    String,
    Integer,
    Boolean,
    DateTime,
    ForeignKey,
    Text,
    Float,
    JSON,
    UniqueConstraint,
    Index
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship
)
from sqlalchemy.dialects.postgresql import UUID
import uuid


# =========================================================
# Base
# =========================================================

class Base(DeclarativeBase):
    pass


# =========================================================
# Mixins
# =========================================================

class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=datetime.utcnow, nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        onupdate=datetime.utcnow,
        nullable=False
    )


class TenantMixin:
    # each model using this mixin references the tenants table
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("tenants.id", ondelete="CASCADE"),
        nullable=False,
        index=True
    )


# =========================================================
# Tenant
# =========================================================

class Tenant(Base, TimestampMixin):
    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    name: Mapped[str] = mapped_column(String(255), nullable=False, unique=True)

    customers: Mapped[List["Customer"]] = relationship(
        back_populates="tenant",
        cascade="all, delete-orphan"
    )

    vendors: Mapped[List["Vendor"]] = relationship(
        back_populates="tenant",
        cascade="all, delete-orphan"
    )


# =========================================================
# Customer (Account)
# =========================================================

class Customer(Base, TimestampMixin, TenantMixin):
    """
    Represents a company/account in CRM.
    Used for SAP MDG lead scoring.
    """

    __tablename__ = "customers"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_customer_tenant_name"),
        Index("ix_customer_industry", "industry"),
        Index("ix_customer_employee_count", "employee_count"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    # Core Company Data
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    website: Mapped[Optional[str]] = mapped_column(String(255))
    industry: Mapped[Optional[str]] = mapped_column(String(150))
    country: Mapped[Optional[str]] = mapped_column(String(100))
    city: Mapped[Optional[str]] = mapped_column(String(100))

    # Firmographics
    employee_count: Mapped[Optional[int]] = mapped_column(Integer)
    annual_revenue: Mapped[Optional[float]] = mapped_column(Float)

    # CRM Flags (for SAP MDG scoring)
    is_regulated_industry: Mapped[bool] = mapped_column(
        Boolean, default=False
    )
    has_multiple_erps: Mapped[bool] = mapped_column(
        Boolean, default=False
    )
    
    # Email Campaign Flag
    in_email_campaign: Mapped[bool] = mapped_column(
        Boolean, default=True
    )

    # CRM tracking fields
    last_client_status: Mapped[Optional[str]] = mapped_column(Text)
    client_phase: Mapped[Optional[str]] = mapped_column(String(50))

    # Free-form metadata (extensible)
    metadata_json: Mapped[Optional[dict]] = mapped_column(JSON)

    # Relationships
    tenant: Mapped["Tenant"] = relationship(back_populates="customers")

    contacts: Mapped[List["Contact"]] = relationship(
        back_populates="customer",
        cascade="all, delete-orphan"
    )

    tech_stack: Mapped[Optional["TechStackProfile"]] = relationship(
        back_populates="customer",
        uselist=False,
        cascade="all, delete-orphan"
    )

    signals: Mapped[List["AccountSignal"]] = relationship(
        back_populates="customer",
        cascade="all, delete-orphan"
    )


# =========================================================
# Contact
# =========================================================

class Contact(Base, TimestampMixin, TenantMixin):
    __tablename__ = "contacts"
    __table_args__ = (
        UniqueConstraint("tenant_id", "email", name="uq_contact_tenant_email"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    customer_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("customers.id", ondelete="CASCADE"),
        nullable=False
    )

    first_name: Mapped[str] = mapped_column(String(150), nullable=False)
    last_name: Mapped[str] = mapped_column(String(150), nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    job_title: Mapped[Optional[str]] = mapped_column(String(150))
    phone: Mapped[Optional[str]] = mapped_column(String(50))

    # free-form metadata for enrichments / extra info
    metadata_json: Mapped[Optional[dict]] = mapped_column(JSON)

    customer: Mapped["Customer"] = relationship(back_populates="contacts")


# =========================================================
# Vendor
# =========================================================

class Vendor(Base, TimestampMixin, TenantMixin):
    __tablename__ = "vendors"
    __table_args__ = (
        UniqueConstraint("tenant_id", "name", name="uq_vendor_tenant_name"),
        Index("ix_vendor_industry", "industry"),
        Index("ix_vendor_country", "country"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    website: Mapped[Optional[str]] = mapped_column(String(255))
    industry: Mapped[Optional[str]] = mapped_column(String(150))
    country: Mapped[Optional[str]] = mapped_column(String(100))
    city: Mapped[Optional[str]] = mapped_column(String(100))
    in_email_campaign: Mapped[bool] = mapped_column(Boolean, default=True)
    metadata_json: Mapped[Optional[dict]] = mapped_column(JSON)

    tenant: Mapped["Tenant"] = relationship(back_populates="vendors")

    contacts: Mapped[List["VendorContact"]] = relationship(
        back_populates="vendor",
        cascade="all, delete-orphan"
    )


# =========================================================
# Vendor Contact
# =========================================================

class VendorContact(Base, TimestampMixin, TenantMixin):
    __tablename__ = "vendor_contacts"
    __table_args__ = (
        UniqueConstraint("tenant_id", "email", name="uq_vendor_contact_tenant_email"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    vendor_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("vendors.id", ondelete="CASCADE"),
        nullable=False
    )

    first_name: Mapped[str] = mapped_column(String(150), nullable=False)
    last_name: Mapped[str] = mapped_column(String(150), nullable=False)
    email: Mapped[str] = mapped_column(String(255), nullable=False)
    job_title: Mapped[Optional[str]] = mapped_column(String(150))
    phone: Mapped[Optional[str]] = mapped_column(String(50))
    metadata_json: Mapped[Optional[dict]] = mapped_column(JSON)

    vendor: Mapped["Vendor"] = relationship(back_populates="contacts")


# =========================================================
# Tech Stack Profile (Important for SAP MDG Lead Engine)
# =========================================================

class TechStackProfile(Base, TimestampMixin, TenantMixin):
    """
    Stores ERP / technology footprint.
    Critical for SAP MDG opportunity detection.
    """

    __tablename__ = "tech_stack_profiles"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    customer_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("customers.id", ondelete="CASCADE"),
        nullable=False,
        unique=True
    )

    # ERP footprint
    uses_sap: Mapped[bool] = mapped_column(Boolean, default=False)
    uses_s4hana: Mapped[bool] = mapped_column(Boolean, default=False)
    s4hana_go_live_year: Mapped[Optional[int]] = mapped_column(Integer)

    uses_sap_mdg: Mapped[bool] = mapped_column(Boolean, default=False)
    mdg_maturity_level: Mapped[Optional[str]] = mapped_column(String(100))

    uses_ariba: Mapped[bool] = mapped_column(Boolean, default=False)
    uses_btp: Mapped[bool] = mapped_column(Boolean, default=False)

    other_systems: Mapped[Optional[dict]] = mapped_column(JSON)

    customer: Mapped["Customer"] = relationship(back_populates="tech_stack")


# =========================================================
# Signal Library
# =========================================================

class Signal(Base, TimestampMixin, TenantMixin):
    """
    Defines standardized signals used in solution-based lead generation.
    Example:
    - S4HANA_TRANSFORMATION
    - DATA_QUALITY_INITIATIVE
    - MA_OR_MULTI_ERP
    """

    __tablename__ = "signals"
    __table_args__ = (
        UniqueConstraint("tenant_id", "key", name="uq_signal_tenant_key"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    key: Mapped[str] = mapped_column(String(100), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)


# =========================================================
# Account Signals (Observed Evidence)
# =========================================================

class AccountSignal(Base, TimestampMixin, TenantMixin):
    """
    Stores observed signals per customer.
    Used by SAP MDG Lead Scoring Engine.
    """

    __tablename__ = "account_signals"
    __table_args__ = (
        UniqueConstraint(
            "tenant_id",
            "customer_id",
            "signal_id",
            name="uq_customer_signal"
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4
    )

    customer_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("customers.id", ondelete="CASCADE"),
        nullable=False
    )

    signal_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("signals.id", ondelete="CASCADE"),
        nullable=False
    )

    confidence: Mapped[float] = mapped_column(Float, default=0.5)
    source: Mapped[Optional[str]] = mapped_column(String(150))
    evidence: Mapped[Optional[dict]] = mapped_column(JSON)

    customer: Mapped["Customer"] = relationship(back_populates="signals")
    signal: Mapped["Signal"] = relationship()

# =========================================================
# Email Log (Outreach Tracking)
# =========================================================

class EmailLog(Base, TimestampMixin, TenantMixin):
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

    # Free-form metadata
    metadata_json: Mapped[Optional[dict]] = mapped_column(JSON)

    # Relationships
    contact: Mapped["Contact"] = relationship()
    customer: Mapped["Customer"] = relationship()
