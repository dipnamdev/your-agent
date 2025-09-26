# storage/db_manager.py
import aiosqlite
from utils.logger import setup_logger
from .models import PageData
import json
from typing import Optional

logger = setup_logger()

from config import DB_PATH as DEFAULT_DB_PATH

# Mutable DB path that can be overridden per-site by the runner
_DB_PATH: str = DEFAULT_DB_PATH

def set_db_path(path: str) -> None:
    global _DB_PATH
    _DB_PATH = path

def get_db_path() -> str:
    return _DB_PATH

CREATE_TABLE_QUERY = """
CREATE TABLE IF NOT EXISTS pages (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE,
    title TEXT,
    meta_desc TEXT,
    content TEXT,
    links TEXT,
    images TEXT,
    page_type TEXT,
    scraped_at TEXT
);
"""

async def init_db():
    """Initialize SQLite database and tables"""
    async with aiosqlite.connect(_DB_PATH) as db:
        await db.execute(CREATE_TABLE_QUERY)
        await db.commit()
    logger.info("Database initialized ✅")

async def save_page(data: PageData):
    """Insert a page into DB (ignore duplicates)"""
    async with aiosqlite.connect(_DB_PATH) as db:
        try:
            await db.execute(
                """
                INSERT OR REPLACE INTO pages 
                (url, title, meta_desc, content, links, images, page_type, scraped_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    data.url,
                    data.title,
                    data.meta_desc,
                    data.content,
                    json.dumps(data.links),
                    json.dumps(data.images),
                    data.page_type,
                    data.scraped_at,
                )
            )
            await db.commit()
            logger.info(f"✅ Saved page: {data.url}")
        except Exception as e:
            logger.error(f"❌ Error saving {data.url}: {e}")

async def get_all_pages():
    """Fetch all pages from DB"""
    async with aiosqlite.connect(_DB_PATH) as db:
        cursor = await db.execute("SELECT url, title, meta_desc, content, page_type FROM pages")
        rows = await cursor.fetchall()
        return rows

async def get_page_by_url(url: str):
    """Fetch a single page by URL"""
    async with aiosqlite.connect(_DB_PATH) as db:
        cursor = await db.execute("SELECT * FROM pages WHERE url = ?", (url,))
        row = await cursor.fetchone()
        return row
