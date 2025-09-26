# storage/models.py
from dataclasses import dataclass, field
from typing import List, Optional
import datetime

@dataclass
class PageData:
    url: str
    title: str
    meta_desc: str
    content: str
    links: List[str] = field(default_factory=list)
    images: List[str] = field(default_factory=list)
    page_type: str = "generic"
    scraped_at: str = field(default_factory=lambda: datetime.datetime.utcnow().isoformat())
