# Project_YA/data_processing/pipeline.py
import numpy as np
import os
import sys
import sqlite3
import json
from sentence_transformers import SentenceTransformer

model = SentenceTransformer("all-MiniLM-L6-v2")

# ===== FIX: Add project root to sys.path =====
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)


import argparse
from utils.logger import setup_logger

# Import processing stages
from data_processing.chunk import chunk_pages
from data_processing.embed import embed_chunks
from data_processing.clean import clean_text, remove_boilerplate_lines
from data_processing.normalize import fix_encoding, extract_emails, extract_and_canonicalize_phone
from data_processing.save import get_conn, init_db, insert_or_update_cleaned_page
from data_processing.vectorstore import build_vector_index, search_index, save_index, load_index
from config import DB_PATH, PROCESSED_DB_PATH, VECTOR_INDEX_PATH


logger = setup_logger(__name__)


# -----------------------------
# MMR re-ranking (Maximal Marginal Relevance)
# -----------------------------
def _l2_normalize(vecs: np.ndarray) -> np.ndarray:
    norms = np.linalg.norm(vecs, axis=1, keepdims=True) + 1e-12
    return vecs / norms


def mmr_rerank(query_vec: np.ndarray, doc_vecs: np.ndarray, top_n: int = 8, diversity: float = 0.7) -> list:
    """
    Selects a subset of documents that are both relevant and diverse.
    Returns a list of selected indices into doc_vecs, ordered by selection.
    """
    if doc_vecs.size == 0:
        return []

    q = query_vec.reshape(1, -1)
    q = _l2_normalize(q)
    d = _l2_normalize(doc_vecs)

    # cosine similarities
    sim_to_query = (d @ q.T).reshape(-1)  # (num_docs,)

    selected = []
    candidates = list(range(d.shape[0]))

    # pick the most relevant first
    first = int(np.argmax(sim_to_query))
    selected.append(first)
    candidates.remove(first)

    while len(selected) < min(top_n, d.shape[0]) and candidates:
        # compute max similarity to any selected doc for each candidate (diversity term)
        selected_mat = d[selected, :]
        cand_mat = d[candidates, :]
        sim_to_selected = cand_mat @ selected_mat.T  # (num_cand, num_sel)
        max_sim_to_selected = sim_to_selected.max(axis=1)

        # MMR score
        mmr_scores = diversity * sim_to_query[candidates] - (1.0 - diversity) * max_sim_to_selected
        pick_idx = int(np.argmax(mmr_scores))
        pick = candidates[pick_idx]
        selected.append(pick)
        candidates.pop(pick_idx)

    return selected


def _processed_db_path_for_site(seed_url: str) -> str:
    from urllib.parse import urlparse
    host = (urlparse(seed_url).netloc or "site").replace(":", "_")
    return os.path.join(project_root, "data", f"{host}.processed_pages.db")


def _vector_index_path_for_site(seed_url: str) -> str:
    from urllib.parse import urlparse
    host = (urlparse(seed_url).netloc or "site").replace(":", "_")
    return os.path.join(project_root, "data", f"{host}.vector.index")


def run_pipeline(seed_url: str | None = None, scraper_db_path: str | None = None) -> str:
    """
    Orchestrates the full data pipeline:
    1. Clean raw scraped pages
    2. Normalize content
    3. Chunk into retrievable units
    4. Generate embeddings
    5. Save/export final dataset
    """

    logger.info("🚀 Starting Data Processing Pipeline")

    # Determine target processed DB path up front (per-site if seed_url given)
    processed_db_path = _processed_db_path_for_site(seed_url) if seed_url else PROCESSED_DB_PATH

    # Step 0: Ingest scraped pages → cleaned_pages (write into target processed DB)
    ingest_scraped_pages(scraper_db_path, processed_db_path)

    # Step 1: Chunk (process all pages that haven't been chunked yet)
    logger.info("Step 1: Chunking cleaned pages")
    chunk_pages(processed_db_path)

    # Step 2: Generate embeddings for any chunks missing them
    logger.info("Step 2: Generating embeddings")
    embed_chunks(db_path=processed_db_path)

    logger.info("Step 3: storing embedding to vector db") 
    # Try to load existing index; if not present, build from DB and save
    index_path = _vector_index_path_for_site(seed_url) if seed_url else VECTOR_INDEX_PATH
    vd_faiss = load_index(index_path)
    if vd_faiss is None:
        vd_faiss = build_vector_index(db_path=processed_db_path)
        if vd_faiss is not None:
            save_index(vd_faiss, index_path)

    # Return the vector DB path for this site
    return index_path

def find_content(user_query, seed_url: str | None = None):

    index_path = _vector_index_path_for_site(seed_url) if seed_url else VECTOR_INDEX_PATH
    vd_faiss = load_index(index_path)
    if vd_faiss is None:
        processed_db_path = _processed_db_path_for_site(seed_url) if seed_url else PROCESSED_DB_PATH
        vd_faiss = build_vector_index(db_path=processed_db_path)
        if vd_faiss is not None:
            save_index(vd_faiss, index_path)

    processed_db_path = _processed_db_path_for_site(seed_url) if seed_url else PROCESSED_DB_PATH

    # Optional domain-aware pre-filter: restrict to chunks whose page url/title contains a product slug
    slug_candidates = _extract_slug_candidates(user_query)
    allowed_chunk_ids = set()
    if slug_candidates:
        for slug in slug_candidates:
            ids = _get_chunk_ids_for_slug(processed_db_path, slug)
            if ids:
                logger.info("Domain-aware filter: slug='%s' matched %d chunks", slug, len(ids))
            allowed_chunk_ids.update(ids)

    # Initial recall from FAISS
    query_embedding = model.encode(user_query).astype(np.float32)
    initial_k = 25
    indices, distances = search_index(vd_faiss, query_embedding, top_k=initial_k, use_cosine=True)
    print("🔎 Found indices:", indices)
    print("📏 Distances:", distances)

    # Fetch candidate records
    records = fetch_chunk_records(indices, processed_db_path)
    id_to_record = {r["id"]: r for r in records}

    # Build candidate texts in the same order as FAISS indices
    candidate_records = []
    for idx in indices:
        rec = id_to_record.get(int(idx))
        if not rec:
            continue
        # Apply domain-aware pre-filter if present
        if allowed_chunk_ids and int(rec.get("id")) not in allowed_chunk_ids:
            continue
        if rec:
            candidate_records.append(rec)

    # If slug filtering was active but nothing matched, enforce the filter (return empty)
    if slug_candidates and not candidate_records:
        logger.info("Domain-aware filter active and no candidates matched slugs %s", slug_candidates)
        return []

    if not candidate_records:
        logger.info("No relevant content found after recall")
        return []

    # Encode candidate texts and apply MMR re-ranking
    candidate_texts = [r.get("content") or "" for r in candidate_records]
    doc_vecs = model.encode(candidate_texts).astype(np.float32)
    selected_order = mmr_rerank(query_embedding, doc_vecs, top_n=3, diversity=0.7)

    # Prepare results according to MMR order
    results = []
    # compute similarity to report as score
    doc_vecs_norm = (doc_vecs / (np.linalg.norm(doc_vecs, axis=1, keepdims=True) + 1e-12))
    q_norm = query_embedding / (np.linalg.norm(query_embedding) + 1e-12)
    sims = (doc_vecs_norm @ q_norm.reshape(-1, 1)).reshape(-1)

    for rank, pos in enumerate(selected_order, start=1):
        rec = candidate_records[pos]
        results.append({
            "title": rec.get("title"),
            "url": rec.get("url"),
            "page_type": rec.get("page_type"),
            "text": rec.get("content"),
            "score": float(sims[pos]),
        })

    logger.info("✅ Retrieval complete with MMR re-ranking")
    return results


# -----------------------------
# Domain-aware helpers
# -----------------------------
def _slugify(text: str) -> str:
    import re
    s = text.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def _extract_slug_candidates(query: str) -> list[str]:
    """
    Build a list of slug candidates from the query, ignoring price-related stopwords,
    and including sub-slugs from trailing n-grams.
    """
    if not query:
        return []
    stopwords = {
        "price", "prices", "cost", "costs", "rate", "rates", "how", "much",
        "buy", "purchase", "order", "shop", "discount", "offer", "offers",
    }
    tokens = [t for t in query.strip().split() if t]
    content_tokens = [t for t in tokens if t.lower() not in stopwords]
    candidates: list[str] = []

    # full
    if content_tokens:
        full = _slugify(" ".join(content_tokens))
        if len(full) >= 6:
            candidates.append(full)

    # trailing n-grams (4->2)
    for n in (4, 3, 2):
        if len(content_tokens) >= n:
            cand = _slugify(" ".join(content_tokens[-n:]))
            if len(cand) >= 6 and cand not in candidates:
                candidates.append(cand)

    # dedupe while preserving order
    seen = set()
    ordered = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            ordered.append(c)
    return ordered


def _get_chunk_ids_for_slug(db_path: str, slug: str) -> set[int]:
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    like = f"%{slug}%"
    cur.execute(
        """
        SELECT c.id
        FROM chunks c
        JOIN cleaned_pages p ON p.id = c.page_id
        WHERE LOWER(p.url) LIKE LOWER(?) OR LOWER(p.title) LIKE LOWER(?)
        """,
        (like, like),
    )
    rows = cur.fetchall()
    conn.close()
    return {int(r[0]) for r in rows}

    # for i, idx in enumerate(indices):
    #    print(f"\nResult {i+1} (id={int(idx)}, score={distances[i]:.4f}):")
    #    print(results.get(int(idx), "[No text found]"))



def fetch_chunk_texts(ids, db_path: str | None = None):
    # Always return a dict for consistent callers
    if len(ids) == 0:
        return {}

    # Coerce FAISS IDs (often numpy.int64) into native Python ints
    id_list = [int(i) for i in ids]

    conn = sqlite3.connect(db_path or PROCESSED_DB_PATH)
    cursor = conn.cursor()

    q_marks = ",".join("?" * len(id_list))
    cursor.execute(f"SELECT id, content FROM chunks WHERE id IN ({q_marks})", id_list)
    results = cursor.fetchall()
    conn.close()

    # Convert to dict for easy lookup
    return {int(row[0]): row[1] for row in results}


def fetch_chunk_records(ids, db_path: str | None = None):
    # Return list of records (id, content, title, url, page_type) for given ids
    if len(ids) == 0:
        return []

    id_list = [int(i) for i in ids]

    conn = sqlite3.connect(db_path or PROCESSED_DB_PATH)
    cursor = conn.cursor()

    q_marks = ",".join("?" * len(id_list))
    cursor.execute(
        f"""
        SELECT c.id, c.content, p.title, p.url, p.page_type
        FROM chunks c
        JOIN cleaned_pages p ON p.id = c.page_id
        WHERE c.id IN ({q_marks})
        """,
        id_list,
    )
    rows = cursor.fetchall()
    conn.close()

    return [
        {
            "id": int(r[0]),
            "content": r[1],
            "title": r[2],
            "url": r[3],
            "page_type": r[4],
        }
        for r in rows
    ]




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data Processing Pipeline")
    parser.parse_args()
    run_pipeline()
   


def ingest_scraped_pages(scraper_db_path: str | None = None, processed_db_path: str | None = None):
    """
    Read rows from the scraper DB (pages table), clean/normalize content,
    and upsert into processed cleaned_pages.
    """
    source_db = scraper_db_path or DB_PATH
    if not os.path.exists(source_db):
        logger.info("Scraper DB not found at %s; skipping ingest.", source_db)
        return

    try:
        src_conn = sqlite3.connect(source_db)
        src_cur = src_conn.cursor()
        # Fetch full records including images JSON and scraped_at
        src_cur.execute(
            """
            SELECT url, title, meta_desc, content, page_type, scraped_at, images
            FROM pages
            """
        )
        rows = src_cur.fetchall()
    except Exception as e:
        logger.exception("Failed to read from scraper DB: %s", str(e))
        return

    if not rows:
        logger.info("No scraped pages found to ingest.")
        src_conn.close()
        return

    # Open processed DB connection once (write into correct target DB)
    dst_conn = get_conn(processed_db_path) if processed_db_path else get_conn()
    init_db(dst_conn)

    inserted = 0
    for url, title, meta_desc, content, page_type, scraped_at, images_text in rows:
        try:
            # Normalize encoding, then clean and de-boilerplate
            fixed = fix_encoding(content or "")
            cleaned = clean_text(fixed)
            cleaned = remove_boilerplate_lines(cleaned)

            # Extract contact signals
            emails = extract_emails(fixed)
            email = emails[0] if emails else None
            phone = extract_and_canonicalize_phone(fixed, default_region=None)

            # Parse images JSON if present
            try:
                images = json.loads(images_text) if images_text else []
                if not isinstance(images, list):
                    images = []
            except Exception:
                images = []

            insert_or_update_cleaned_page(
                dst_conn,
                url=url,
                title=title or "",
                meta_desc=meta_desc or "",
                content=cleaned,
                page_type=page_type or "generic",
                scraped_at=scraped_at,
                email=email,
                phone=phone,
                images=images,
            )
            inserted += 1
        except Exception:
            # already logged inside insert on failure; continue
            continue

    logger.info("Ingested/upserted %d scraped pages into cleaned_pages", inserted)
    try:
        dst_conn.close()
    except Exception:
        pass
    try:
        src_conn.close()
    except Exception:
        pass
