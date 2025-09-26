import sqlite3
import numpy as np
import faiss
import os

from utils.logger import setup_logger
from config import PROCESSED_DB_PATH, VECTOR_INDEX_PATH


logger = setup_logger(__name__)


def load_embeddings_from_db(db_path: str | None = None):
    conn = sqlite3.connect(db_path or PROCESSED_DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, embedding FROM chunks WHERE embedding IS NOT NULL")
    rows = cursor.fetchall()
    conn.close()

    if not rows:
        logger.warning("⚠️ No embeddings found in database")
        return np.array([], dtype=np.int64), np.array([], dtype=np.float32)

    ids = []
    embeddings = []
    for row in rows:
        ids.append(row[0])
        emb_data = row[1]

        if isinstance(emb_data, str):
            # Old rows stored as string repr (e.g., "[0.1, 0.2, ...]")
            try:
                emb = np.array(eval(emb_data), dtype=np.float32)
            except Exception as e:
                logger.error(f"Failed to parse embedding for id {row[0]}: {e}")
                continue
        else:
            # New rows stored as proper BLOB
            emb = np.frombuffer(emb_data, dtype=np.float32)

        embeddings.append(emb)

    return np.array(ids, dtype=np.int64), np.array(embeddings, dtype=np.float32)



def build_faiss_index(embeddings, ids, use_cosine=True):
    if embeddings.size == 0:
        return None

    dim = embeddings.shape[1]
    if use_cosine:
        faiss.normalize_L2(embeddings)
        index = faiss.IndexIDMap(faiss.IndexFlatIP(dim))  # cosine via inner product
    else:
        index = faiss.IndexIDMap(faiss.IndexFlatL2(dim))  # Euclidean

    index.add_with_ids(embeddings, ids)
    return index


def search_index(index, query_embedding, top_k=3, use_cosine=True):
    if index is None:
        logger.error("❌ Index not built yet")
        return [], []

    query_vector = np.array([query_embedding], dtype=np.float32)
    if use_cosine:
        faiss.normalize_L2(query_vector)

    distances, indices = index.search(query_vector, top_k)
    return indices[0], distances[0]


# -----------------------------
# Persistence helpers
# -----------------------------
def save_index(index, path: str = VECTOR_INDEX_PATH):
    if index is None:
        logger.warning("No index to save")
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    faiss.write_index(index, path)
    logger.info(f"💾 Saved FAISS index to {path}")


def load_index(path: str = VECTOR_INDEX_PATH):
    if not os.path.exists(path):
        logger.warning(f"FAISS index not found at {path}")
        return None
    index = faiss.read_index(path)
    logger.info(f"📦 Loaded FAISS index from {path}")
    return index


# -----------------------------
# Main pipeline function
# -----------------------------
def build_vector_index(use_cosine: bool = True, db_path: str | None = None):
    """
    Loads embeddings from DB, builds FAISS index,
    and returns the index object.
    """
    logger.info("📥 Loading embeddings from database")
    ids, embeddings = load_embeddings_from_db(db_path)

    if embeddings.size == 0:
        logger.warning("⚠️ No embeddings found, skipping vector index build")
        return None

    logger.info(f"Building FAISS index with {len(ids)} vectors")
    index = build_faiss_index(embeddings, ids, use_cosine=use_cosine)

    logger.info("✅ Vector index built successfully")
    return index
