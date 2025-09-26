# Project_YA/data_processing/chunk.py

from typing import List
from utils.logger import setup_logger
from data_processing.save import get_conn, init_db
from config import CHUNK_OVERLAP_TOKENS
import sqlite3

# LangChain Semantic Chunker
from langchain_experimental.text_splitter import SemanticChunker
from langchain_community.embeddings import HuggingFaceEmbeddings

logger = setup_logger(__name__)

# -----------------------------
# Main function: chunk pages using semantic splitting
# -----------------------------
def chunk_pages(db_path: str | None = None):
    """
    Loads cleaned_pages, splits into semantic chunks using embeddings,
    and stores results into chunks table.
    """
    conn = get_conn(db_path) if db_path else get_conn()
    cursor = conn.cursor()

    # Ensure schema
    init_db(conn)

    # Load cleaned pages that have not been chunked yet
    cursor.execute(
        """
        SELECT id, url, title, meta_desc, content, page_type, scraped_at
        FROM cleaned_pages cp
        WHERE NOT EXISTS (
            SELECT 1 FROM chunks c WHERE c.page_id = cp.id
        )
        """
    )
    pages = cursor.fetchall()
    logger.info(f"Loaded {len(pages)} cleaned pages for semantic chunking")

    if not pages:
        conn.close()
        logger.info("No pages to process for chunking")
        return

    # Load embedding model for semantic splitting (LangChain embeddings interface)
    model_name = "all-MiniLM-L6-v2"
    embedding_model = HuggingFaceEmbeddings(model_name=model_name)
    # LangChain SemanticChunker expects an embeddings object (with embed_documents)
    text_splitter = SemanticChunker(
        embedding_model,
        min_chunk_size=1000,
        breakpoint_threshold_type="percentile",
    )

    for page_id, url, title, meta_desc, content, page_type, scraped_at in pages:
        if not content:
            continue

        # Create semantic chunks (returns list of LangChain Document objects)
        docs = text_splitter.create_documents([content])

        for idx, doc in enumerate(docs):
            prefix = (title or page_type or "section").strip()
            section_name = prefix if idx == 0 else f"{prefix}_part{idx+1}"
            chunk_text = doc.page_content

            cursor.execute(
                """
                INSERT INTO chunks (page_id, section, chunk_index, content, embedding)
                VALUES (?, ?, ?, ?, ?)
                """,
                (page_id, section_name, idx, chunk_text, None)
            )

    conn.commit()
    conn.close()
    logger.info("✅ Semantic chunking complete, data stored in chunks table")