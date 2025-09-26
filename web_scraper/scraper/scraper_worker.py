# scraper/scraper_worker.py

# Pulling a URL from the queue
# Using Playwright (via playwright_utils) to load the page
# Extracting text + metadata (via content_extractor)
# Storing structured result in DB (via storage/db_manager)
import aiofiles
import asyncio
from pathlib import Path
from web_scraper.crawler.queue_manager import URLQueue
from web_scraper.scraper.playwright_utils import new_page
from web_scraper.scraper.content_extractor import extract_content
from web_scraper.storage.db_manager import save_page
from web_scraper.storage.models import PageData
from utils.logger import setup_logger
from utils.retry import retry_async
from utils.helpers import random_delay
from config import NAV_TIMEOUT_MS, WAIT_UNTIL, DELAY_RANGE

logger = setup_logger()

class ScraperWorker:
    def __init__(self, url_queue: URLQueue, worker_id: int, site_host: str):
        self.url_queue = url_queue
        self.worker_id = worker_id
        self.site_host = site_host

    async def run(self):
        """Run scraper worker loop until shutdown sentinel received"""
        page = await new_page()

        while True:
            url = await self.url_queue.get_url()
            if url is None:  # Sentinel for shutdown
                self.url_queue.task_done()
                break
            try:
                logger.info(f"[Worker {self.worker_id}] Scraping: {url}")

                # Retry visiting page with exponential backoff
                await retry_async(
                    lambda: page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until=WAIT_UNTIL),
                    retries=2,
                    delay=1.5
                )

                # Extract structured content
                data = await extract_content(page, url)

                # Save in DB
                await save_page(PageData(
                    url=data["url"],
                    title=data.get("title", ""),
                    meta_desc=data.get("meta_desc", ""),
                    content=data.get("content", ""),
                    links=data.get("links", []),
                    images=data.get("images", []),
                    page_type=data.get("page_type", "generic"),
                    scraped_at=data.get("scraped_at")
                ))

                await save_page_to_file(data, self.worker_id, self.site_host)


                # Add a polite randomized delay between scrapes
                await random_delay(DELAY_RANGE[0], DELAY_RANGE[1])

            except Exception as e:
                logger.error(f"[Worker {self.worker_id}] Error scraping {url}: {e}")
            finally:
                self.url_queue.task_done()

        await page.close()
        logger.info(f"[Worker {self.worker_id}] Finished scraping")



async def save_page_to_file(data, worker_id, site_host: str):
    out_dir = (Path(__file__).resolve().parents[1] / "output" / site_host)
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = out_dir / f"scraped_worker_{worker_id}.txt"
    async with aiofiles.open(str(filename), "a", encoding="utf-8") as f:
        await f.write(data["url"] + "\n")
        await f.write(data["content"] + "\n\n" + "="*50 + "\n\n")
