# route: Project_YA/data_processing/clean.py
# Purpose: Remove boilerplate / UI noise, extract semantic sections from scraped page text.
# Provides:
#   - clean_text(raw) -> str
#   - extract_sections(cleaned_text) -> List[Dict[str, str]]  (each dict: {"section": ..., "content": ...})
#   - remove_boilerplate_lines(text) -> str
# Uses utils.logger.setup_logger for logging.

import re
import html
from typing import List, Dict, Tuple, Optional
from utils.logger import setup_logger

logger = setup_logger("data_processing.clean")

# Common boilerplate phrases and CTAs to remove
BOILERPLATE_PATTERNS = [
    r"get in touch",
    r"scroll down",
    r"learn more",
    r"view more",
    r"read more",
    r"contact us",
    r"all rights reserved",
    r"©\s*\d{4}",
    r"follow us on",
    r"facebook[- ]?f",
    r"instagram",
    r"linkedin",
    r"behance",
    r"twitter",
    r"subscribe",
    r"enter your email",
    r"enter your message",
    r"submit",
    r"privacy policy",
    r"terms of service",
    r"cookie policy",
]

# Common section heading keywords (ordered by priority)
SECTION_KEYWORDS = [
    "about", "services", "our services", "what we do",
    "portfolio", "our work", "case studies",
    "blog", "news", "articles",
    "testimonials", "client", "clients", "reviews",
    "contact", "get in touch", "contact us",
    "process", "our process", "how we work",
    "team", "careers",
    "features", "pricing"
]

# Compile regexes once
_BOILERPLATE_RE = re.compile("|".join([r"\b" + p + r"\b" for p in BOILERPLATE_PATTERNS]), flags=re.I)
_MULTIPLE_NEWLINE_RE = re.compile(r"\n{2,}")
_LEADING_TRAILING_WS_RE = re.compile(r"^\s+|\s+$")
_HTML_TAG_RE = re.compile(r"<(?:script|style).*?>.*?</(?:script|style)>", flags=re.I | re.S)
_GENERAL_TAG_RE = re.compile(r"<[^>]+>")
_NON_PRINTABLE_RE = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]+")


def clean_text(raw: str) -> str:
    """
    High-level cleaning:
    - Unescape HTML entities
    - Remove script/style blocks and HTML tags (best-effort)
    - Replace non-printable chars and NBSPs
    - Remove common boilerplate phrases (CTAs, social links, copyright)
    - Normalize whitespace
    Returns cleaned text (string).
    """
    if not raw:
        return ""

    text = raw

    # 1) Unescape HTML entities
    try:
        text = html.unescape(text)
    except Exception:
        logger.debug("html.unescape failed; continuing with original text")

    # 2) Remove script/style blocks
    text = _HTML_TAG_RE.sub(" ", text)

    # 3) Remove other HTML tags (best-effort)
    text = _GENERAL_TAG_RE.sub(" ", text)

    # 4) Replace NBSP and other weird unicode spaces with normal space
    text = text.replace("\xa0", " ").replace("\u200b", " ").replace("\u202f", " ")

    # 5) Remove non-printable characters
    text = _NON_PRINTABLE_RE.sub(" ", text)

    # 6) Lowercase for boilerplate detection (but keep original later — here for deletion)
    # We'll remove boilerplate via regex on a per-line basis
    text_lines = text.splitlines()
    cleaned_lines = []
    for line in text_lines:
        stripped = line.strip()
        if not stripped:
            continue
        # if line matches boilerplate phrase entirely or mostly, skip it
        if _BOILERPLATE_RE.search(stripped):
            # skip short boilerplate lines, but keep lines that contain real content
            # if the line length is small and matches boilerplate -> drop
            if len(stripped) < 120:
                logger.debug("Dropping boilerplate line: %s", stripped[:60])
                continue
            # otherwise try to remove only matched phrase
            stripped = _BOILERPLATE_RE.sub(" ", stripped)

        cleaned_lines.append(stripped)

    text = "\n".join(cleaned_lines)

    # 7) Collapse multiple newlines and trim whitespace
    text = _MULTIPLE_NEWLINE_RE.sub("\n\n", text)
    text = _LEADING_TRAILING_WS_RE.sub("", text)

    return text


def split_by_headings(text: str) -> List[Tuple[str, str]]:
    """
    Attempt to split text by headings. We look for lines that are short and look like headings,
    or that match SECTION_KEYWORDS. Returns list of (heading, section_text).
    If no headings found, returns a single item with heading 'main' and the full text.
    """
    if not text:
        return []

    lines = text.splitlines()
    indices = []
    headings = []

    # Heuristic: a line is a heading if
    #  - it matches a known section keyword
    #  - OR line is short (<100 chars) and contains >0 uppercase initial words (like "About Our Company")
    for i, ln in enumerate(lines):
        s = ln.strip()
        if not s:
            continue
        low = s.lower()
        # match exact keyword presence
        for kw in SECTION_KEYWORDS:
            if kw in low:
                indices.append(i)
                headings.append(s)
                break
        else:
            # heuristic heading detection
            if len(s) <= 100 and 0 < sum(1 for w in s.split() if w and w[0].isupper()) <= 6:
                # avoid numeric lines
                if not re.match(r"^\d+(\.|:)?\s*$", s):
                    indices.append(i)
                    headings.append(s)

    # if no headings detected -> one block
    if not indices:
        return [("main", text.strip())]

    # Build sections by joining lines between heading indices
    sections = []
    for idx_pos, idx in enumerate(indices):
        start = idx + 1  # content starts after heading line
        end = indices[idx_pos + 1] if idx_pos + 1 < len(indices) else len(lines)
        section_lines = lines[start:end]
        section_text = "\n".join([l.strip() for l in section_lines if l.strip()])
        heading = headings[idx_pos].strip() if headings[idx_pos] else "section"
        sections.append((heading, section_text or ""))

    # If first lines before the first detected heading contain content, add them as 'intro'
    first_heading_idx = indices[0]
    if first_heading_idx > 0:
        intro_text = "\n".join([l.strip() for l in lines[:first_heading_idx] if l.strip()])
        if intro_text:
            sections.insert(0, ("intro", intro_text))

    return sections


def normalize_section_name(raw_heading: str) -> str:
    """
    Normalize heading text to one of our known section names or a slug.
    """
    if not raw_heading:
        return "section"
    low = raw_heading.lower()
    for kw in SECTION_KEYWORDS:
        if kw in low:
            # return the keyword as canonical section name
            return kw.replace(" ", "_")
    # else slugify simple
    slug = re.sub(r"[^a-z0-9]+", "_", low.strip())
    slug = re.sub(r"_{2,}", "_", slug).strip("_")
    return slug or "section"


def extract_sections(text: str) -> List[Dict[str, str]]:
    """
    From cleaned text, extract semantic sections. Output is a list of dicts:
    [{ "section": "about", "content": "..." }, ...]
    If a section has empty content, it's kept with empty string (pipeline can drop it later).
    """
    sections = []
    heading_content_pairs = split_by_headings(text)
    for heading, content in heading_content_pairs:
        section_name = normalize_section_name(heading)
        sections.append({"section": section_name, "content": content.strip()})
    return sections


def remove_boilerplate_lines(text: str) -> str:
    """
    Extra pass to remove short repetitive lines that are likely boilerplate (menu items etc).
    We remove lines shorter than 60 chars that contain only a few words and match common patterns.
    """
    if not text:
        return ""

    out_lines = []
    for line in text.splitlines():
        s = line.strip()
        if not s:
            continue
        # drop lines that are short and mostly non-alphanumeric or contain only a single word that's a known boilerplate
        if len(s) < 60:
            # if it contains only a single word and that word matches boilerplate keywords -> drop
            words = re.findall(r"[A-Za-z0-9']+", s)
            if len(words) <= 3 and any(_BOILERPLATE_RE.search(w) for w in words):
                logger.debug("Dropping tiny boilerplate line: %s", s)
                continue
        out_lines.append(s)
    text = "\n".join(out_lines)
    text = _MULTIPLE_NEWLINE_RE.sub("\n\n", text)
    return text


# small convenience wrapper that runs full cleaning pipeline for a single raw text
def clean_pages(raw_text: str) -> Dict[str, object]:
    """
    Runs cleaning and section extraction and returns:
    {
      "cleaned_text": "...",
      "sections": [{"section": "about", "content": "..."}, ...]
    }
    """
    cleaned = clean_text(raw_text)
    cleaned = remove_boilerplate_lines(cleaned)
    sections = extract_sections(cleaned)
    return {"cleaned_text": cleaned, "sections": sections}


# Example quick test
if __name__ == "__main__":
    sample = """
    Best IT Company in Indore for Web Development Services Get In Touch Leading IT Company in Indore for Web & Software Solutions ThirdEssential is a globally recognised IT company in Indore, offering innovative digital solutions that help businesses grow, scale, and lead in their industries. Contact Us Scroll Down About Our Company Discover Who We Are We are a team of experienced professionals with comprehensive expertise in web marketing, web development and web designing. At ThirdEssential, we improve a brand’s success digitally via our extraordinary IT solutions such as : web development mobile app development Software development UI/UX development full-stack web development Learn More Our Journey Explore Our History 99 % Client Retention 7 Years of Service 30 + Team of Professionals 221 + Client Retention
    """
    out = clean_pages(sample)
    logger.info("CLEANED TEXT:\n%s\n", out["cleaned_text"])
    logger.info("SECTIONS:\n%s", out["sections"])
