from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from .parser import IndexItem


DECISION_COLUMNS = (
    "item_key",
    "title_raw",
    "case_name_raw",
    "parties_raw",
    "parties_json",
    "party_names_all",
    "party_names_normalised",
    "primary_adverse_caption",
    "adverse_pair_key",
    "division",
    "panel",
    "case_number",
    "registry_number",
    "order_or_decision_number",
    "decision_date",
    "document_type",
    "type_of_action",
    "language",
    "headnote_raw",
    "headnote_text",
    "keywords_raw",
    "keywords_list",
    "pdf_url_official",
    "pdf_url_mirror",
    "node_url",
    "pdf_sha256",
    "first_seen_at",
    "last_seen_at",
    "ingested_at",
    "alerted_at",
    "last_error",
)


JSON_COLUMNS = {"parties_json", "party_names_all", "party_names_normalised", "keywords_list"}


class Database:
    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def init(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS seen_items (
                    item_key TEXT PRIMARY KEY,
                    node_url TEXT,
                    first_seen_at TEXT NOT NULL,
                    last_seen_at TEXT NOT NULL,
                    source_index_snapshot TEXT NOT NULL DEFAULT '{}',
                    bootstrapped_at TEXT
                );

                CREATE TABLE IF NOT EXISTS decisions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_key TEXT NOT NULL UNIQUE,
                    title_raw TEXT,
                    case_name_raw TEXT,
                    parties_raw TEXT,
                    parties_json TEXT,
                    party_names_all TEXT,
                    party_names_normalised TEXT,
                    primary_adverse_caption TEXT,
                    adverse_pair_key TEXT,
                    division TEXT,
                    panel TEXT,
                    case_number TEXT,
                    registry_number TEXT,
                    order_or_decision_number TEXT,
                    decision_date TEXT,
                    document_type TEXT,
                    type_of_action TEXT,
                    language TEXT,
                    headnote_raw TEXT,
                    headnote_text TEXT,
                    keywords_raw TEXT,
                    keywords_list TEXT,
                    pdf_url_official TEXT,
                    pdf_url_mirror TEXT,
                    node_url TEXT,
                    pdf_sha256 TEXT,
                    first_seen_at TEXT,
                    last_seen_at TEXT,
                    ingested_at TEXT,
                    alerted_at TEXT,
                    last_error TEXT
                );

                CREATE TABLE IF NOT EXISTS decision_documents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    decision_id INTEGER NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
                    language TEXT,
                    pdf_url_official TEXT NOT NULL,
                    pdf_url_mirror TEXT,
                    pdf_sha256 TEXT,
                    file_path TEXT,
                    is_primary INTEGER NOT NULL DEFAULT 0,
                    downloaded_at TEXT,
                    UNIQUE(decision_id, pdf_url_official)
                );

                CREATE TABLE IF NOT EXISTS ingestion_runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    discovered_count INTEGER NOT NULL DEFAULT 0,
                    new_count INTEGER NOT NULL DEFAULT 0,
                    failure_summary TEXT,
                    debug_dir TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_decisions_date ON decisions(decision_date DESC);
                CREATE INDEX IF NOT EXISTS idx_decisions_seen ON decisions(first_seen_at DESC);
                CREATE INDEX IF NOT EXISTS idx_seen_node ON seen_items(node_url);
                """
            )
            self._migrate_decision_documents_unique_constraint(conn)

    def _migrate_decision_documents_unique_constraint(self, conn: sqlite3.Connection) -> None:
        row = conn.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'decision_documents'"
        ).fetchone()
        table_sql = str(row["sql"] if row else "")
        if "pdf_url_official TEXT NOT NULL UNIQUE" not in table_sql:
            return

        conn.executescript(
            """
            ALTER TABLE decision_documents RENAME TO decision_documents_old;

            CREATE TABLE decision_documents (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                decision_id INTEGER NOT NULL REFERENCES decisions(id) ON DELETE CASCADE,
                language TEXT,
                pdf_url_official TEXT NOT NULL,
                pdf_url_mirror TEXT,
                pdf_sha256 TEXT,
                file_path TEXT,
                is_primary INTEGER NOT NULL DEFAULT 0,
                downloaded_at TEXT,
                UNIQUE(decision_id, pdf_url_official)
            );

            INSERT OR IGNORE INTO decision_documents (
                id, decision_id, language, pdf_url_official, pdf_url_mirror,
                pdf_sha256, file_path, is_primary, downloaded_at
            )
            SELECT id, decision_id, language, pdf_url_official, pdf_url_mirror,
                   pdf_sha256, file_path, is_primary, downloaded_at
            FROM decision_documents_old;

            DROP TABLE decision_documents_old;
            """
        )

    def start_run(self, started_at: str, debug_dir: str) -> int:
        with self.connect() as conn:
            cursor = conn.execute(
                """
                INSERT INTO ingestion_runs (started_at, status, debug_dir)
                VALUES (?, 'running', ?)
                """,
                (started_at, debug_dir),
            )
            return int(cursor.lastrowid)

    def finish_run(
        self,
        run_id: int,
        finished_at: str,
        status: str,
        discovered_count: int,
        new_count: int,
        failure_summary: str = "",
    ) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE ingestion_runs
                SET finished_at = ?, status = ?, discovered_count = ?, new_count = ?, failure_summary = ?
                WHERE id = ?
                """,
                (finished_at, status, discovered_count, new_count, failure_summary, run_id),
            )

    def has_seen(self, item_key: str) -> bool:
        with self.connect() as conn:
            row = conn.execute("SELECT 1 FROM seen_items WHERE item_key = ?", (item_key,)).fetchone()
            return row is not None

    def needs_enrichment(self, item_key: str) -> bool:
        """Return True for seen items that still lack a mirrored PDF/document row or had errors.

        A detail page can be temporarily unavailable or challenged. In that case
        the scraper persists index metadata and marks the item seen so it is not
        re-alerted, but later runs should still retry detail/PDF enrichment.
        """
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT d.id, d.pdf_url_mirror, d.pdf_sha256, d.last_error,
                       COUNT(dd.id) AS document_count
                FROM decisions d
                LEFT JOIN decision_documents dd ON dd.decision_id = d.id
                WHERE d.item_key = ?
                GROUP BY d.id
                """,
                (item_key,),
            ).fetchone()
            if row is None:
                return True
            return (
                not row["pdf_url_mirror"]
                or not row["pdf_sha256"]
                or bool(row["last_error"])
                or int(row["document_count"] or 0) == 0
            )

    def mark_seen(self, item: IndexItem, now: str, bootstrapped: bool = False) -> None:
        bootstrapped_at = now if bootstrapped else None
        snapshot = json.dumps(item.source_index_snapshot, ensure_ascii=False, sort_keys=True)
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO seen_items (
                    item_key, node_url, first_seen_at, last_seen_at, source_index_snapshot, bootstrapped_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(item_key) DO UPDATE SET
                    node_url = excluded.node_url,
                    last_seen_at = excluded.last_seen_at,
                    source_index_snapshot = excluded.source_index_snapshot,
                    bootstrapped_at = COALESCE(seen_items.bootstrapped_at, excluded.bootstrapped_at)
                """,
                (item.item_key, item.node_url, now, now, snapshot, bootstrapped_at),
            )

    def touch_seen(self, item: IndexItem, now: str) -> None:
        snapshot = json.dumps(item.source_index_snapshot, ensure_ascii=False, sort_keys=True)
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE seen_items
                SET node_url = ?, last_seen_at = ?, source_index_snapshot = ?
                WHERE item_key = ?
                """,
                (item.node_url, now, snapshot, item.item_key),
            )
            conn.execute(
                "UPDATE decisions SET last_seen_at = ? WHERE item_key = ?",
                (now, item.item_key),
            )

    def upsert_decision(self, values: dict[str, Any]) -> int:
        serialised = dict(values)
        for column in JSON_COLUMNS:
            serialised[column] = json.dumps(serialised.get(column, []), ensure_ascii=False)
        for column in DECISION_COLUMNS:
            serialised.setdefault(column, "")

        placeholders = ", ".join("?" for _ in DECISION_COLUMNS)
        columns = ", ".join(DECISION_COLUMNS)
        update_columns = [column for column in DECISION_COLUMNS if column not in {"item_key", "first_seen_at"}]
        updates = ", ".join(f"{column} = excluded.{column}" for column in update_columns)
        params = [serialised[column] for column in DECISION_COLUMNS]

        with self.connect() as conn:
            conn.execute(
                f"""
                INSERT INTO decisions ({columns})
                VALUES ({placeholders})
                ON CONFLICT(item_key) DO UPDATE SET {updates}
                """,
                params,
            )
            row = conn.execute("SELECT id FROM decisions WHERE item_key = ?", (serialised["item_key"],)).fetchone()
            if row is None:
                raise RuntimeError("decision upsert did not return an id")
            return int(row["id"])

    def replace_documents(self, decision_id: int, documents: list[dict[str, Any]]) -> None:
        with self.connect() as conn:
            for doc in documents:
                conn.execute(
                    """
                    INSERT INTO decision_documents (
                        decision_id, language, pdf_url_official, pdf_url_mirror,
                        pdf_sha256, file_path, is_primary, downloaded_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(decision_id, pdf_url_official) DO UPDATE SET
                        language = excluded.language,
                        pdf_url_mirror = excluded.pdf_url_mirror,
                        pdf_sha256 = excluded.pdf_sha256,
                        file_path = excluded.file_path,
                        is_primary = excluded.is_primary,
                        downloaded_at = excluded.downloaded_at
                    """,
                    (
                        decision_id,
                        doc.get("language", ""),
                        doc["pdf_url_official"],
                        doc.get("pdf_url_mirror", ""),
                        doc.get("pdf_sha256", ""),
                        doc.get("file_path", ""),
                        1 if doc.get("is_primary") else 0,
                        doc.get("downloaded_at", ""),
                    ),
                )

    def get_decisions(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM decisions
                ORDER BY COALESCE(decision_date, '') DESC, COALESCE(first_seen_at, '') DESC, id DESC
                """
            ).fetchall()
            decisions: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                for column in JSON_COLUMNS:
                    try:
                        item[column] = json.loads(item.get(column) or "[]")
                    except json.JSONDecodeError:
                        item[column] = []
                docs = conn.execute(
                    """
                    SELECT language, pdf_url_official, pdf_url_mirror, pdf_sha256, file_path,
                           is_primary, downloaded_at
                    FROM decision_documents
                    WHERE decision_id = ?
                    ORDER BY is_primary DESC, id ASC
                    """,
                    (item["id"],),
                ).fetchall()
                item["documents"] = [
                    {
                        **dict(doc),
                        "is_primary": bool(doc["is_primary"]),
                    }
                    for doc in docs
                ]
                decisions.append(item)
            return decisions
