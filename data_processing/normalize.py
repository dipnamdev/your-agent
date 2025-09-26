# route: Project_YA/data_processing/normalize.py
# Purpose: Encoding fixes, canonicalization of emails and phone numbers, small utilities for normalization.
# Provides:
#   - fix_encoding(text) -> str
#   - extract_emails(text) -> List[str]
#   - canonicalize_email(email) -> str
#   - extract_and_canonicalize_phone(text, default_region: Optional[str]=None) -> Optional[str]
# Uses utils.logger.setup_logger for logging.
# Note: This module will attempt to use the `phonenumbers` library if available; if not, it falls back to a simple digit-based normalization.

import re
import unicodedata
import html
from typing import List, Optional
from utils.logger import setup_logger

logger = setup_logger("data_processing.normalize")

# Email regex (robust but not perfect)
_EMAIL_RE = re.compile(
    r"([a-zA-Z0-9_.+\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z0-9\-.]+)",
    flags=re.I
)

# Common obfuscated email patterns, e.g. '[email\xa0protected]', 'email [at] example [dot] com'
_OBFUSCATED_PATTERNS = [
    (re.compile(r"\[email(?:\\xa0)?protected\]", flags=re.I), "email@example.com"),
    (re.compile(r'\[email\s*protected\]', flags=re.I), "email@example.com"),
    (re.compile(r"\s*\[at\]\s*", flags=re.I), "@"),
    (re.compile(r"\s*\(at\)\s*", flags=re.I), "@"),
    (re.compile(r"\s*\[dot\]\s*", flags=re.I), "."),
    (re.compile(r"\s*\(dot\)\s*", flags=re.I), "."),
]

# Phone regex - capture digits with optional + and separators
_PHONE_RE = re.compile(r"(\+?\d[\d\-\s().]{6,}\d)")

# Try to import phonenumbers (optional)
try:
    import phonenumbers  # type: ignore
    from phonenumbers import NumberParseException  # type: ignore

    _HAS_PHONENUMBERS = True
except Exception:
    _HAS_PHONENUMBERS = False
    NumberParseException = Exception  # type: ignore


def fix_encoding(text: str) -> str:
    """
    Normalize unicode, unescape HTML entities, replace NBSPs, remove control characters.
    """
    if not text:
        return ""

    # unescape HTML entities
    try:
        text = html.unescape(text)
    except Exception:
        logger.debug("html.unescape failed during fix_encoding")

    # replace common non-breaking spaces with normal spaces
    text = text.replace("\xa0", " ").replace("\u200b", " ").replace("\u202f", " ")

    # normalize unicode (NFKC)
    try:
        text = unicodedata.normalize("NFKC", text)
    except Exception:
        logger.debug("unicodedata.normalize failed")

    # remove control chars
    text = re.sub(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]", " ", text)

    # collapse multiple spaces to single, but keep newlines
    text = re.sub(r"[ \t]{2,}", " ", text)
    text = re.sub(r" +\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text.strip()


def extract_emails(text: str) -> List[str]:
    """
    Extract emails from text. Handles some obfuscated patterns by first normalizing them.
    Returns unique, lowercased email addresses.
    """
    if not text:
        return []

    t = text

    # handle simple obfuscations
    for patt, repl in _OBFUSCATED_PATTERNS:
        t = patt.sub(repl, t)

    # common disguised forms like "email at domain dot com"
    t = re.sub(r"\bemail\s+at\s+", "email@", t, flags=re.I)
    t = re.sub(r"\bdot\b", ".", t, flags=re.I)

    found = _EMAIL_RE.findall(t)
    # normalize & unique
    emails = []
    for e in found:
        ce = canonicalize_email(e)
        if ce not in emails:
            emails.append(ce)
    return emails


def canonicalize_email(email: str) -> str:
    """
    Basic canonicalization for emails:
    - strip surrounding characters
    - lowercase
    - replace multiple dots in domain sensibly
    """
    if not email:
        return ""
    e = email.strip().lower()
    # remove stray surrounding characters
    e = re.sub(r"^[\"'<>]+|[\"'<>]+$", "", e)
    # collapse multiple dots in the domain part
    if "@" in e:
        local, domain = e.split("@", 1)
        domain = re.sub(r"\.{2,}", ".", domain)
        e = f"{local}@{domain}"
    return e


def extract_and_canonicalize_phone(text: str, default_region: Optional[str] = None) -> Optional[str]:
    """
    Try to find and canonicalize a phone number from text.
    - If `phonenumbers` is available and default_region provided (like 'IN' for India), returns E.164 string if parseable.
    - If phonenumbers not available, fall back to extracting the longest continuous digit sequence (with '+' if present).
    Returns None if nothing found.
    """
    if not text:
        return None

    # First try phonenumbers if available
    if _HAS_PHONENUMBERS:
        # find any potential phone-like tokens
        candidates = _PHONE_RE.findall(text)
        for cand in candidates:
            cand = cand.strip()
            try:
                if cand.startswith("+"):
                    pn = phonenumbers.parse(cand, None)
                else:
                    pn = phonenumbers.parse(cand, default_region)
                if phonenumbers.is_possible_number(pn) or phonenumbers.is_valid_number(pn):
                    formatted = phonenumbers.format_number(pn, phonenumbers.PhoneNumberFormat.E164)
                    return formatted
            except NumberParseException:
                continue
        # not found with phonenumbers
        return None

    # Fallback: extract digit groups and pick the longest plausible one
    candidates = re.findall(r"(\+?\d[\d\-\s().]{6,}\d)", text)
    if not candidates:
        return None

    # Clean candidates to digits (keep leading +)
    cleaned = []
    for c in candidates:
        c = c.strip()
        keep_plus = c.startswith("+")
        digits = re.sub(r"[^\d]", "", c)
        if not digits:
            continue
        if keep_plus:
            cleaned.append("+" + digits)
        else:
            cleaned.append(digits)

    # choose the candidate with maximum length (heuristic)
    if not cleaned:
        return None
    best = max(cleaned, key=lambda s: len(re.sub(r"\D", "", s)))
    # if it's a raw local number (no leading +) and length between 8-10, we leave it as is
    if best.startswith("+"):
        return best
    if 8 <= len(best) <= 15:
        # try to return with + if it's long (assume international not known)
        if len(best) > 10:
            return "+" + best
        return best
    return None


# Example usage when run directly
if __name__ == "__main__":
    sample = "Contact: [email\xa0protected] or email [at] example [dot] com. Phone: +91 98266 03803 or (731) 493-8802"
    fixed = fix_encoding(sample)
    logger.info("Fixed encoding: %s", fixed)
    emails = extract_emails(fixed)
    phones = extract_and_canonicalize_phone(fixed, default_region="IN")
    logger.info("Extracted emails: %s", emails)
    logger.info("Extracted phone: %s", phones)

