# scraper/content_extractor.py

# This module will make scraped pages more structured for your AI agent:
# Title
# Meta description
# Main visible text (cleaned)
# Links (internal & external)
# Images (optional)
# Page type guess (blog, product, homepage, etc.)

from bs4 import BeautifulSoup
from urllib.parse import urlparse
import datetime

def extract_page_type(url: str, soup: BeautifulSoup) -> str:
    """
    Basic heuristic to guess page type based on URL or DOM.
    """
    path = urlparse(url).path.lower()

    if any(x in path for x in ["blog", "article", "post"]):
        return "blog"
    elif any(x in path for x in ["product", "item", "shop"]):
        return "product"
    elif path in ["", "/", "/home"]:
        return "homepage"
    else:
        return "generic"

async def extract_content(page, url: str) -> dict:
    """
    Extract title, meta description, main text, links, and page type.
    Returns structured dict ready for DB.
    """
    html = await page.content()
    soup = BeautifulSoup(html, "html.parser")

    # Title
    title = soup.title.string.strip() if soup.title else ""

    # Meta description
    meta_desc = ""
    desc_tag = soup.find("meta", attrs={"name": "description"})
    if desc_tag and desc_tag.get("content"):
        meta_desc = desc_tag["content"].strip()

    # Visible text (excluding nav/footer/script/style)
    for tag in soup(["script", "style", "nav", "footer"]):
        tag.decompose()

    text_content = " ".join(soup.stripped_strings)

    # Links
    links = [a["href"] for a in soup.find_all("a", href=True)]

    # Images
    images = [img["src"] for img in soup.find_all("img", src=True)]

    # Page type
    page_type = extract_page_type(url, soup)

    return {
        "url": url,
        "title": title,
        "meta_desc": meta_desc,
        "content": text_content,
        "links": links,
        "images": images,
        "page_type": page_type,
        "scraped_at": datetime.datetime.utcnow().isoformat(),
    }
