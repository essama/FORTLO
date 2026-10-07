"""CRM ingest. Analysis depends on these records, not on a vendor SDK.

Swap `CrmAdapter` to point at another export. Keep the same keys.
This module only reads. It has no insert or update method.
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Protocol


class CrmAdapter(Protocol):
    def recruiting_companies(self) -> list[dict]:
        """Payers. In this CRM they are staffing and recruiting firms."""

    def invoices(self) -> list[dict]:
        """Issued invoices. Used as closed-won evidence for the payer."""

    def sourced_accounts(self) -> list[dict]:
        """Accounts to score. Prospects, not the system of record for wins."""


def _client_row(row: dict) -> dict:
    return {
        "source_id": str(row.get("id") or row.get("source_id") or ""),
        "company_name": (row.get("company_name") or "").strip(),
        "contact_name": (row.get("contact_name") or row.get("name") or "").strip(),
        "city": (row.get("city") or "").strip(),
        "country": (row.get("country") or "").strip(),
        "email": (row.get("email") or "").strip(),
        "source": row.get("source") or "crm",
    }


def _invoice_row(row: dict) -> dict:
    return {
        "id": str(row.get("id") or ""),
        "inv_nr": row.get("inv_nr") or "",
        "date": (row.get("date") or "")[:10],
        "net_price": float(row.get("net_price") or 0),
        "client_id": str(row.get("client_id") or ""),
        "item": row.get("item") or "",
        "item_count": float(row.get("item_count") or 0),
        "price": float(row.get("price") or 0),
    }


class SnapshotCrmAdapter:
    """JSON export of the invoice CRM and the FORTLO account list."""

    def __init__(self, crm_path: Path, accounts_path: Path | None = None):
        self.crm_path = Path(crm_path)
        self.accounts_path = Path(accounts_path) if accounts_path else None
        self._crm = json.loads(self.crm_path.read_text(encoding="utf-8"))

    def recruiting_companies(self) -> list[dict]:
        return [_client_row(row) for row in self._crm.get("clients") or []]

    def invoices(self) -> list[dict]:
        return [_invoice_row(row) for row in self._crm.get("invoices") or []]

    def sourced_accounts(self) -> list[dict]:
        if self.accounts_path is None or not self.accounts_path.exists():
            return []
        data = json.loads(self.accounts_path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            data = data.get("accounts") or []
        return list(data)


class SqliteCrmAdapter:
    """Read-only SQLite. Opens with mode=ro so the source file cannot be written."""

    def __init__(self, invoice_db: Path, account_db: Path | None = None):
        self.invoice_db = Path(invoice_db)
        self.account_db = Path(account_db) if account_db else None

    def _connect(self, path: Path) -> sqlite3.Connection:
        uri = "file:" + path.resolve().as_posix() + "?mode=ro"
        con = sqlite3.connect(uri, uri=True)
        con.row_factory = sqlite3.Row
        return con

    def _rows(self, path: Path, sql: str) -> list[sqlite3.Row]:
        con = self._connect(path)
        try:
            return con.execute(sql).fetchall()
        finally:
            con.close()

    def recruiting_companies(self) -> list[dict]:
        rows = self._rows(
            self.invoice_db,
            "SELECT id, company_name, name, city, country, email FROM clients",
        )
        out = []
        for row in rows:
            out.append(
                _client_row(
                    {
                        "id": row["id"],
                        "company_name": row["company_name"],
                        "name": row["name"],
                        "city": row["city"],
                        "country": row["country"],
                        "email": row["email"],
                        "source": "invoice_sqlite",
                    }
                )
            )
        return out

    def invoices(self) -> list[dict]:
        rows = self._rows(
            self.invoice_db,
            """
            SELECT id, inv_nr, date, net_price, client_id, item, item_count, price
            FROM outgoing_invoices
            """,
        )
        return [_invoice_row(dict(row)) for row in rows]

    def sourced_accounts(self) -> list[dict]:
        if self.account_db is None or not self.account_db.exists():
            return []
        rows = [dict(row) for row in self._rows(
            self.account_db,
            """
            SELECT id, name, domain, industry, country, city, employee_count,
                   annual_revenue, size_band, icp_status, exclusion_reason,
                   score_notes, suggested_titles_json, in_email_campaign
            FROM customers
            """,
        )]
        try:
            stacks = self._rows(
                self.account_db,
                "SELECT customer_id, uses_sap, uses_s4hana, uses_sap_mdg FROM tech_stack_profiles",
            )
        except sqlite3.OperationalError:
            stacks = []
        by_id = {}
        for stack in stacks:
            words = [label for flag, label in (("uses_sap", "SAP"), ("uses_s4hana", "S/4HANA"), ("uses_sap_mdg", "MDG")) if stack[flag]]
            if words:
                by_id[stack["customer_id"]] = " ".join(words)
        for row in rows:
            row["tech_stack"] = by_id.get(row["id"], "")
        return rows
