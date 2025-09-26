# utils/retry.py

# Retry failed HTTP/Playwright requests with backoff.

import asyncio
import random

async def retry_async(coro_func, retries=3, delay=2, backoff=2):
    """
    Retry coroutine with exponential backoff.
    """
    for attempt in range(1, retries + 1):
        try:
            return await coro_func()
        except Exception as e:
            if attempt == retries:
                raise
            wait = delay * (backoff ** (attempt - 1)) + random.uniform(0, 1)
            print(f"⚠️ Attempt {attempt} failed: {e}. Retrying in {wait:.1f}s...")
            await asyncio.sleep(wait)
