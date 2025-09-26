# crawler/crawler.py

# Start from the base URL
# Visit each page
# Extract internal links
# Add new URLs to the central queue (URLQueue)


import asyncio
from web_scraper.crawler.queue_manager import URLQueue, DiscoveryQueue
from web_scraper.crawler.url_utils import is_internal_url, normalize_url, is_probably_html_url
from web_scraper.scraper.playwright_utils import new_page
from utils.logger import setup_logger
from config import MAX_DEPTH, NAV_TIMEOUT_MS, WAIT_UNTIL

logger = setup_logger()

class Crawler:
    def __init__(self, discovery_queue: DiscoveryQueue, scrape_queue: URLQueue, base_url: str):
        self.discovery_queue = discovery_queue
        self.scrape_queue = scrape_queue
        self.base_url = base_url

    async def start_crawling(self):
        """Start the crawling loop using a shared browser. Produce URLs for scraping."""
        page = await new_page()

        while self.discovery_queue.has_pending():
            url, depth = await self.discovery_queue.get_url()
            try:
                await page.goto(url, timeout=NAV_TIMEOUT_MS, wait_until=WAIT_UNTIL)
                logger.info(f"Crawling (depth {depth}): {url}")

                # Enqueue this visited URL for scraping
                await self.scrape_queue.add_url_async(url)

                # Extract all links from the page
                links = await page.eval_on_selector_all(
                    "a[href]",
                    "elements => elements.map(el => el.href)"
                )

                # Add only internal, normalized URLs to the discovery queue with depth control
                if depth < MAX_DEPTH:
                    for link in links:
                        link = normalize_url(link)
                        if is_internal_url(link, self.base_url) and is_probably_html_url(link):
                            await self.discovery_queue.add_url_async(link, depth + 1)

            except Exception as e:
                logger.error(f"Error crawling {url}: {e}")
            finally:
                self.discovery_queue.task_done()

        await page.close()
        logger.info("Crawler finished!")
