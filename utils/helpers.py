# utils/helpers.py
# Some helper utilities for randomization, delays, etc.

import asyncio
import random

async def random_delay(min_delay=1, max_delay=3):
    """
    Sleep for a random delay between min_delay and max_delay.
    """
    delay = random.uniform(min_delay, max_delay)
    await asyncio.sleep(delay)
    return delay
