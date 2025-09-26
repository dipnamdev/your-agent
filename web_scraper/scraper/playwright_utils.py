# scraper/playwright_utils.py

"""
The purpose of this module is:
Create and manage a single browser instance
Reuse it across multiple scraper workers
Provide helper methods for page creation
"""

from playwright.async_api import async_playwright
from config import BLOCK_ASSETS

_browser = None
_playwright = None

async def get_browser(headless: bool = True):
    """
    Launch a shared browser instance if not already running.
    Reuse the same instance across workers.
    """
    global _browser, _playwright
    if _browser is None:
        _playwright = await async_playwright().start()
        _browser = await _playwright.chromium.launch(headless=headless)
    return _browser

async def new_page():
    """
    Create a new page (tab) from the shared browser.
    """
    browser = await get_browser()
    page = await browser.new_page()
    # Block non-essential resource types to speed up navigation
    if BLOCK_ASSETS:
        try:
            await page.route("**/*", lambda route: route.abort() if route.request.resource_type in {"image","media","font","stylesheet"} else route.continue_())
        except Exception:
            pass
    return page

async def close_browser():
    """
    Close the shared browser instance when all work is done.
    """
    global _browser, _playwright
    if _browser:
        await _browser.close()
        _browser = None
    if _playwright:
        await _playwright.stop()
        _playwright = None
