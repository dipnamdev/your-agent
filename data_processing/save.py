# route: Project_YA/data_processing/save.py
# Purpose: Database initialization and CRUD helper functions for processed_pages.db
# Tables: cleaned_pages, chunks
# Uses: utils.logger.setup_logger for logging
# Note: embeddings and images are stored as JSON text for portability.

import sqlite3
import json
import os
from typing import List, Optional, Dict, Any
from datetime import datetime

from utils.logger import setup_logger

logger = setup_logger("data_processing.save")

# DB path - keep it relative to project root 'data' folder
# project_root = dirname(dirname(__file__))
DEFAULT_DB_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "processed_pages.db")
DEFAULT_DB_PATH = os.path.normpath(DEFAULT_DB_PATH)


def get_conn(db_path: str = DEFAULT_DB_PATH) -> sqlite3.Connection:
    """Create (if needed) and return a sqlite3 connection with foreign keys enabled."""
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30)
    conn.row_factory = sqlite3.Row
    # ensure FK checks on
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    """Create cleaned_pages and chunks tables if they do not exist."""
    logger.info("Initializing processed DB schema if missing.")
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS cleaned_pages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT,
            title TEXT,
            meta_desc TEXT,
            content TEXT,
            page_type TEXT,
            scraped_at TEXT,
            email TEXT,
            phone TEXT,
            images TEXT,
            created_at TEXT DEFAULT (DATETIME('now')),
            updated_at TEXT DEFAULT (DATETIME('now')),
            UNIQUE(url, page_type)
        );
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS chunks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            page_id INTEGER,
            section TEXT,
            chunk_index INTEGER,
            content TEXT,
            embedding TEXT, -- stored as JSON array text
            created_at TEXT DEFAULT (DATETIME('now')),
            FOREIGN KEY (page_id) REFERENCES cleaned_pages (id) ON DELETE CASCADE
        );
        """
    )
    conn.commit()
    logger.info("DB initialized (tables ensured).")


def insert_or_update_cleaned_page(
    conn: sqlite3.Connection,
    url: str,
    title: str,
    meta_desc: str,
    content: str,
    page_type: str,
    scraped_at: Optional[str] = None,
    email: Optional[str] = None,
    phone: Optional[str] = None,
    images: Optional[List[str]] = None,
) -> int:
    """
    Insert a cleaned page or update it if (url, page_type) already exists.
    Returns the cleaned_pages.id.
    """
    if scraped_at is None:
        scraped_at = datetime.utcnow().isoformat()

    images_json = json.dumps(images or [])

    sql = """
    INSERT INTO cleaned_pages (url, title, meta_desc, content, page_type, scraped_at, email, phone, images, updated_at)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, DATETIME('now'))
    ON CONFLICT(url, page_type) DO UPDATE SET
        title = excluded.title,
        meta_desc = excluded.meta_desc,
        content = excluded.content,
        scraped_at = excluded.scraped_at,
        email = excluded.email,
        phone = excluded.phone,
        images = excluded.images,
        updated_at = DATETIME('now')
    ;
    """
    cur = conn.cursor()
    try:
        cur.execute(
            sql,
            (url, title, meta_desc, content, page_type, scraped_at, email, phone, images_json),
        )
        conn.commit()
    except Exception as e:
        logger.exception("Failed to insert/update cleaned_pages for url=%s : %s", url, str(e))
        raise

    # fetch id
    cur.execute("SELECT id FROM cleaned_pages WHERE url = ? AND page_type = ?", (url, page_type))
    row = cur.fetchone()
    page_id = row["id"]
    logger.info("Upserted cleaned_page id=%s url=%s", page_id, url)
    return page_id


def insert_chunks(
    conn: sqlite3.Connection,
    page_id: int,
    chunks: List[Dict[str, Any]],
    replace_existing: bool = False,
) -> List[int]:
    """
    Insert multiple chunks for a page.
    chunks: list of dicts -> { "section": str, "chunk_index": int, "content": str, "embedding": Optional[List[float]] }
    If replace_existing is True, delete existing chunks for the page before inserting.
    Returns list of inserted chunk ids.
    """

    if replace_existing:
        logger.info("Replacing existing chunks for page_id=%s", page_id)
        conn.execute("DELETE FROM chunks WHERE page_id = ?", (page_id,))
        conn.commit()

    sql = "INSERT INTO chunks (page_id, section, chunk_index, content, embedding) VALUES (?, ?, ?, ?, ?);"
    cur = conn.cursor()
    inserted_ids = []
    try:
        for ch in chunks:
            section = ch.get("section")
            chunk_index = int(ch.get("chunk_index", 0))
            content = ch.get("content", "")
            embedding = ch.get("embedding")
            embedding_json = json.dumps(embedding) if embedding is not None else None

            cur.execute(sql, (page_id, section, chunk_index, content, embedding_json))
            inserted_ids.append(cur.lastrowid)

        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.exception("Failed to insert chunks for page_id=%s : %s", page_id, str(e))
        raise

    logger.info("Inserted %d chunks for page_id=%s", len(inserted_ids), page_id)
    return inserted_ids


def update_chunk_embedding(
    conn: sqlite3.Connection,
    chunk_id: int,
    embedding: List[float],
) -> None:
    """Update embedding for a single chunk (embedding stored as JSON text)."""
    embedding_json = json.dumps(embedding)
    try:
        conn.execute("UPDATE chunks SET embedding = ? WHERE id = ?", (embedding_json, chunk_id))
        conn.commit()
        logger.debug("Updated embedding for chunk_id=%s", chunk_id)
    except Exception as e:
        conn.rollback()
        logger.exception("Failed to update embedding for chunk_id=%s : %s", chunk_id, str(e))
        raise


def get_cleaned_page_by_url(conn: sqlite3.Connection, url: str, page_type: Optional[str] = None) -> Optional[Dict]:
    """Return cleaned_page row as dict, or None."""
    cur = conn.cursor()
    if page_type:
        cur.execute("SELECT * FROM cleaned_pages WHERE url = ? AND page_type = ?", (url, page_type))
    else:
        cur.execute("SELECT * FROM cleaned_pages WHERE url = ? ORDER BY updated_at DESC LIMIT 1", (url,))
    row = cur.fetchone()
    if not row:
        return None
    data = dict(row)
    # deserialize images
    try:
        data["images"] = json.loads(data.get("images", "[]") or "[]")
    except Exception:
        data["images"] = []
    return data


def get_chunks_by_page_id(conn: sqlite3.Connection, page_id: int) -> List[Dict]:
    """Return list of chunks (with embedding parsed if present)."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM chunks WHERE page_id = ? ORDER BY chunk_index ASC", (page_id,))
    rows = cur.fetchall()
    result = []
    for r in rows:
        d = dict(r)
        emb_text = d.get("embedding")
        try:
            d["embedding"] = json.loads(emb_text) if emb_text else None
        except Exception:
            d["embedding"] = None
        result.append(d)
    return result


def sample_cleaned_pages(conn: sqlite3.Connection, limit: int = 5) -> List[Dict]:
    """Return a random sample (or most recent if random not desired) of cleaned pages for QA."""
    cur = conn.cursor()
    cur.execute("SELECT * FROM cleaned_pages ORDER BY updated_at DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    res = []
    for r in rows:
        d = dict(r)
        try:
            d["images"] = json.loads(d.get("images", "[]") or "[]")
        except Exception:
            d["images"] = []
        res.append(d)
    return res


def close_conn(conn: sqlite3.Connection) -> None:
    try:
        conn.close()
    except Exception:
        logger.exception("Error on closing DB connection.")


# Example CLI/test usage
# if __name__ == "__main__":
    # quick sanity check to initialize DB and insert a dummy page + chunks
def save_outputs():    
    conn = get_conn()
    init_db(conn)

    # Insert or update a dummy page
    page_id = insert_or_update_cleaned_page(
        conn,
        url="https://example.com/test-page",
        title="Test Page",
        meta_desc="A test page meta description",
        content="This is cleaned content for testing.",
        page_type="test",
        scraped_at=datetime.utcnow().isoformat(),
        email="info@example.com",
        phone="+1234567890",
        images=["https://example.com/img1.png"],
    )

    # Insert chunks
    sample_chunks = [
        {"section": "about", "chunk_index": 0, "content": "Chunk 1 content small.", "embedding": None},
        {"section": "about", "chunk_index": 1, "content": "Chunk 2 content small.", "embedding": None},
    ]
    inserted = insert_chunks(conn, page_id, sample_chunks, replace_existing=True)

    # Fetch and log sample cleaned pages
    samples = sample_cleaned_pages(conn, limit=3)
    logger.info("Sample cleaned pages: %s", json.dumps(samples, indent=2, ensure_ascii=False))

    # Fetch chunks for the inserted page
    chunks = get_chunks_by_page_id(conn, page_id)
    logger.info("Inserted chunks: %s", json.dumps(chunks, indent=2, ensure_ascii=False))

    close_conn(conn)
