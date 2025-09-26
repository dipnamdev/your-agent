# crawler/url_utils.py

# Only crawl internal links (ignore external ads, CDNs, etc).
# Normalize URLs so we don’t crawl the same page multiple times (/about, /about/, /about?ref=123).

from urllib.parse import urlparse, urlunparse, urljoin, parse_qsl

DISALLOWED_EXTENSIONS = {
    ".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico",
    ".mp4", ".webm", ".mp3", ".wav", ".ogg",
    ".pdf", ".zip", ".rar", ".7z", ".gz", ".tar",
    ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".exe", ".dmg", ".apk",
    ".css", ".js", ".mjs", ".map",
}


TRACKING_PARAMS_PREFIXES = (
    "utm_",
)

TRACKING_PARAMS_EXACT = {
    "gclid", "fbclid", "ref", "referrer", "_hsenc", "_hsmi"
}


def _strip_tracking_params(query: str) -> str:
    if not query:
        return ""
    filtered = []
    for k, v in parse_qsl(query, keep_blank_values=True):
        if k in TRACKING_PARAMS_EXACT:
            continue
        if any(k.startswith(p) for p in TRACKING_PARAMS_PREFIXES):
            continue
        filtered.append((k, v))
    if not filtered:
        return ""
    # Re-encode simply as k=v joined by & (we don't need exact quoting here for dedup)
    return "&".join(f"{k}={v}" for k, v in filtered)


def normalize_url(url: str) -> str:
    """
    Normalize URL by:
    - lowercasing scheme and host
    - removing fragments (#...)
    - removing common tracking query parameters
    - stripping trailing slash
    - collapsing duplicate slashes in path
    """
    parsed = urlparse(url)
    scheme = (parsed.scheme or "http").lower()
    netloc = (parsed.netloc or "").lower()
    path = parsed.path or "/"
    # collapse duplicate slashes in path
    while "//" in path:
        path = path.replace("//", "/")
    # strip tracking params
    query = _strip_tracking_params(parsed.query)
    # remove fragment
    fragment = ""
    # strip trailing slash (but keep root "/")
    if len(path) > 1 and path.endswith("/"):
        path = path[:-1]
    normalized = urlunparse((scheme, netloc, path, "", query, fragment))
    return normalized

def is_internal_url(url: str, base_url: str) -> bool:
    """
    Check if a URL belongs to the same domain as the base_url.
    """
    parsed_base = urlparse(base_url)
    parsed_url = urlparse(url)
    base_host = (parsed_base.netloc or "").lower()
    url_host = (parsed_url.netloc or "").lower()
    # Treat www and non-www as same host
    def strip_www(h: str) -> str:
        return h[4:] if h.startswith("www.") else h
    return strip_www(url_host) == strip_www(base_host) and parsed_url.scheme in ["http", "https"]

def absolute_url(link: str, base_url: str) -> str:
    """
    Convert relative URL to absolute.
    Example: '/about' -> 'https://example.com/about'
    """
    return urljoin(base_url, link)

def is_probably_html_url(url: str) -> bool:
    """
    Heuristic filter: returns False for common binary/media/static assets by extension
    and for typical WordPress uploads paths.
    """
    parsed = urlparse(url)
    path = parsed.path.lower()
    # WordPress media uploads folder
    if "/wp-content/uploads/" in path:
        return False
    # Extension filter
    for ext in DISALLOWED_EXTENSIONS:
        if path.endswith(ext):
            return False
    return True
