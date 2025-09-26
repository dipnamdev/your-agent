# Project_YA/data_processing/embed.py

from typing import List
from utils.logger import setup_logger
from config import EMBEDDING_MODEL
from data_processing.save import get_conn, init_db, update_chunk_embedding

from sentence_transformers import SentenceTransformer

logger = setup_logger(__name__)


def embed_texts(model: SentenceTransformer, texts: List[str]) -> List[List[float]]:
    """
    Generate embeddings for a list of texts.
    """
    return model.encode(texts, convert_to_numpy=True).tolist()


def embed_chunks(batch_size: int = 32, debug: bool = False, db_path: str | None = None):
    """
    Loads chunks without embeddings, generates embeddings,
    and updates the database.
    """
    conn = get_conn(db_path) if db_path else get_conn()
    cursor = conn.cursor()
    init_db(conn)

    # Load chunks missing embeddings
    cursor.execute("SELECT id, content FROM chunks WHERE embedding IS NULL")
    rows = cursor.fetchall()

    if not rows:
        logger.info("No chunks found without embeddings. Skipping.")
        return

    logger.info(f"Found {len(rows)} chunks to embed")

    # Load model
    logger.info(f"Loading embedding model: {EMBEDDING_MODEL}")
    model = SentenceTransformer(EMBEDDING_MODEL)

    # Process in batches
    for i in range(0, len(rows), batch_size):
        batch = rows[i:i + batch_size]
        ids = [row[0] for row in batch]
        texts = [row[1] for row in batch]

        embeddings = embed_texts(model, texts)

        # Update DB via helper (stores JSON text)
        for chunk_id, emb in zip(ids, embeddings):
            update_chunk_embedding(conn, chunk_id, emb)
        logger.info(f"Processed batch {i // batch_size + 1}/{len(rows) // batch_size + 1}")

        if debug:
            logger.debug(f"Sample embedding (chunk {ids[0]}): {embeddings[0][:5]}...")

    conn.close()
    logger.info("✅ Embedding generation complete")
