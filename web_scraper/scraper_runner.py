# uv.py

import os
import sys
import asyncio
import subprocess
import shutil

# ===== FIX: Add project root to sys.path =====
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
# =============================================

# ✅ Absolute imports from the project
from config import WORKER_COUNT, BASE_URL
from web_scraper.crawler.crawler import Crawler
from web_scraper.crawler.queue_manager import URLQueue, DiscoveryQueue
from web_scraper.scraper.scraper_worker import ScraperWorker
from web_scraper.scraper.playwright_utils import close_browser
from web_scraper.storage.db_manager import init_db, set_db_path, get_db_path
from utils.logger import setup_logger

# ✅ Setup logger
logger = setup_logger()


async def install_playwright():
    """
    Installs Playwright Chromium browser if not already installed.
    """
    try:
        logger.info("Installing Playwright Chromium browser (if not installed)...")

        python_exe = sys.executable or shutil.which("python") or "python"
        logger.info(f"Using Python executable: {python_exe}")

        result = subprocess.run(
            [python_exe, "-m", "playwright", "install", "chromium"],
            check=True,
            capture_output=True,
            text=True
        )

        logger.info("Playwright installation complete.")
        logger.debug(f"Installation output: {result.stdout}")

    except subprocess.CalledProcessError as e:
        logger.error("Primary installation method failed.")
        logger.debug(f"stdout: {e.stdout}")
        logger.debug(f"stderr: {e.stderr}")

        logger.info("Attempting fallback Playwright installation method...")
        try:
            from playwright.sync_api import sync_playwright

            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                browser.close()

            logger.info("Fallback Playwright installation successful!")

        except Exception as alt_e:
            logger.critical(f"Fallback installation also failed: {alt_e}")
            logger.critical("Please manually run:\n  pip install playwright\n  playwright install chromium")
            raise e


def _db_path_for_site(seed_url: str) -> str:
    # Create a per-site DB file under data/, e.g., data/thirdessential.com.scraper.db
    from urllib.parse import urlparse
    parsed = urlparse(seed_url)
    host = (parsed.netloc or "site").replace(":", "_")
    return os.path.join(project_root, "data", f"{host}.scraper.db")


async def main(seed_url: str | None = None):
    # Step 1: Ensure Playwright is installed
    try:
        await install_playwright()
    except Exception as e:
        logger.warning(f"Playwright installation failed or skipped: {e}")
        logger.info("Continuing assuming Playwright is already installed...")

    # Step 2: Init database (per-site path)
    seed = seed_url or BASE_URL
    db_path = _db_path_for_site(seed)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    set_db_path(db_path)
    await init_db()

    # Step 3: Setup queues
    discovery_queue = DiscoveryQueue()
    scrape_queue = URLQueue()

    # Step 4: Seed discovery queue
    discovery_queue.add_url(seed, depth=0)

    # Step 5: Start crawler
    crawler = Crawler(discovery_queue, scrape_queue, seed)
    crawler_task = asyncio.create_task(crawler.start_crawling())

    # Step 6: Start scraper workers
    scraper_tasks = []
    for i in range(WORKER_COUNT):
        # Pass site host for per-site output isolation
        from urllib.parse import urlparse
        site_host = (urlparse(seed).netloc or "site").replace(":", "_")
        scraper = ScraperWorker(scrape_queue, i, site_host)
        task = asyncio.create_task(scraper.run())
        scraper_tasks.append(task)

    # Step 7: Wait for crawler to finish
    await crawler_task

    # Step 8: Signal scrapers to shut down
    for _ in range(WORKER_COUNT):
        await scrape_queue.add_sentinel_async()

    # Step 9: Wait for all scrapers to finish
    await asyncio.gather(*scraper_tasks)

    # Step 10: Close browser
    await close_browser()

    logger.info("✅ Scraping completed successfully!")
    return db_path


if __name__ == "__main__":
    asyncio.run(main())
