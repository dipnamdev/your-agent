# crawler/queue_manager.py

# URLs discovered by the crawler
# URLs to be scraped by scraper workers
# Avoiding duplicates
# Tracking visited URLs

import asyncio
from urllib.parse import urlparse
from web_scraper.crawler.url_utils import normalize_url

class URLQueue:
    def __init__(self):
        self.queue = asyncio.Queue()
        self.visited = set()
        self.lock = asyncio.Lock()  # For thread-safe access to visited set

    async def add_url_async(self, url: str):
        norm = normalize_url(url)
        async with self.lock:
            if norm not in self.visited:
                await self.queue.put(norm)
                self.visited.add(norm)

    def add_url(self, url: str):
        """Synchronous version for adding initial URLs"""
        norm = normalize_url(url)
        if norm not in self.visited:
            self.queue.put_nowait(norm)
            self.visited.add(norm)

    async def add_sentinel_async(self):
        """Enqueue a sentinel (None) without affecting visited set."""
        await self.queue.put(None)

    def add_sentinel(self):
        """Synchronous version to enqueue a sentinel (None)."""
        self.queue.put_nowait(None)

    async def get_url(self):
        """Get a URL from the queue (awaitable)"""
        url = await self.queue.get()
        return url

    def task_done(self):
        self.queue.task_done()

    def has_pending(self):
        return not self.queue.empty()

    def get_visited(self):
        """Returns set of visited URLs"""
        return self.visited


# self.queue → stores URLs to be scraped (async queue for concurrency).
# self.visited → tracks URLs already discovered or scraped to avoid duplicates.
# add_url / add_url_async → add URL safely; async version is for crawler running in async tasks.
# get_url → scrapers call this to get the next URL.
# task_done → marks a task completed (optional if you want to track queue completion)


class DiscoveryQueue:
    """
    Queue dedicated to crawling/discovery phase. Tracks depth per URL so we can
    limit crawl depth without affecting scraping consumers.
    """

    def __init__(self):
        self.queue = asyncio.Queue()
        self.visited = set()
        self.lock = asyncio.Lock()

    async def add_url_async(self, url: str, depth: int):
        norm = normalize_url(url)
        async with self.lock:
            if norm not in self.visited:
                await self.queue.put((norm, depth))
                self.visited.add(norm)

    def add_url(self, url: str, depth: int):
        norm = normalize_url(url)
        if norm not in self.visited:
            self.queue.put_nowait((norm, depth))
            self.visited.add(norm)

    async def get_url(self):
        return await self.queue.get()

    def task_done(self):
        self.queue.task_done()

    def has_pending(self):
        return not self.queue.empty()

    def get_visited(self):
        return self.visited