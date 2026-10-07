"""Write leadgen scores onto FORTLO customers. Only the leadgen_* columns are touched.

Campaign flags, icp_status and contacts are left alone here; the refuel gate
decides campaign membership for new accounts only.
"""

from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

LEADGEN_COLUMNS = {
    "leadgen_score": "INTEGER",
    "leadgen_decision": "VARCHAR(20)",
    "leadgen_reason": "TEXT",
    "leadgen_scored_at": "VARCHAR(30)",
}


def ensure_columns(con: sqlite3.Connection) -> None:
    have = {row[1] for row in con.execute("PRAGMA table_info(customers)")}
    for col, ddl in LEADGEN_COLUMNS.items():
        if col not in have:
            con.execute(f"ALTER TABLE customers ADD COLUMN {col} {ddl}")
    con.commit()


def write_scores(db_path: Path, ranked: list[dict], accounts: list[dict], backup: bool = True) -> dict:
    """ranked and accounts are parallel by account name + domain. Returns counts."""
    db_path = Path(db_path)
    if backup:
        shutil.copy2(db_path, f"{db_path}.bak-leadgen-{datetime.now():%Y%m%d-%H%M%S}")
    by_key = {(a.get("name") or "", a.get("domain") or ""): a.get("id") for a in accounts}
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    con = sqlite3.connect(db_path)
    try:
        ensure_columns(con)
        rows = []
        for row in ranked:
            cid = by_key.get((row["account"], row["domain"]))
            if cid:
                rows.append((row["score"], row["decision"], row["reason"][:1500], stamp, cid))
        with con:
            con.executemany(
                "UPDATE customers SET leadgen_score=?, leadgen_decision=?, leadgen_reason=?, leadgen_scored_at=? WHERE id=?",
                rows,
            )
    finally:
        con.close()
    return {"updated": len(rows), "pursue": sum(1 for r in rows if r[1] == "pursue")}
