# config.py

"""
Configuration file for Web Scraper AI Agent
Modify these settings to control scraping behavior.
"""

# 🌐 Starting point for crawler
BASE_URL = "https://thirdessential.com/"


# 👷 Number of concurrent scraper workers
WORKER_COUNT = 10
MAX_DEPTH = 3   # 🔎 Maximum depth for crawler (0 = only seed page, 1 = follow links once, etc.)
DELAY_RANGE = (0.2, 0.8)   # ⏱️ Randomized polite delay between scrapes (in seconds)
MAX_CHUNK_TOKENS = 1000
CHUNK_OVERLAP_TOKENS = 50

# 💾 Storage settings
DB_PATH = "data/scraper.db"   # SQLite database file path
PROCESSED_DB_PATH ="data/processed_pages.db"
SAVE_HTML = False    # Save raw HTML to /data folder as backup
EMBEDDING_MODEL = "all-MiniLM-L6-v2"
VECTOR_INDEX_PATH = "data/vector.index"  # FAISS index persistence path

# 🚀 Performance flags
BLOCK_ASSETS = True  # Block images/media/fonts/stylesheet
NAV_TIMEOUT_MS = 15000  # Navigation timeout per page
WAIT_UNTIL = "domcontentloaded"  # Faster than 'load'

DEEPSEEK_API_KEY="sk-01572351a73247ac88d648f083bd6d51"