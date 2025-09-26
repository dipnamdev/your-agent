import argparse
import asyncio
import os
from web_scraper.scraper_runner import main as run_scraper
from data_processing.pipeline import run_pipeline, find_content
from web_scraper.scraper_runner import _db_path_for_site
from data_processing.pipeline import _vector_index_path_for_site
from llm.llm_model import ask_llm

site_results_map = {}


def process_site(site_link):
    # Compute expected per-site paths (consider www/non-www variants)
    from urllib.parse import urlparse
    parsed = urlparse(site_link)
    host = (parsed.netloc or "site").lower().replace(":", "_")
    host_nowww = host[4:] if host.startswith("www.") else host

    def path_for_host(h):
        return (
            os.path.join(os.path.dirname(__file__), "data", f"{h}.scraper.db"),
            os.path.join(os.path.dirname(__file__), "data", f"{h}.vector.index"),
        )

    expected_scraped_db, expected_vector_index = path_for_host(host)
    alt_scraped_db, alt_vector_index = path_for_host(host_nowww)

    # If any vector index already exists, assume processed
    if os.path.exists(expected_vector_index) or os.path.exists(alt_vector_index):
        vector_idx = expected_vector_index if os.path.exists(expected_vector_index) else alt_vector_index
        scraped_db = expected_scraped_db if os.path.exists(expected_scraped_db) else (alt_scraped_db if os.path.exists(alt_scraped_db) else None)
        site_results_map[site_link] = {
            "scraped_db_path": scraped_db,
            "vector_index_path": vector_idx,
        }
        return vector_idx

    # If any scraped DB exists, skip scraping and run pipeline only
    existing_scraped = expected_scraped_db if os.path.exists(expected_scraped_db) else (alt_scraped_db if os.path.exists(alt_scraped_db) else None)
    if existing_scraped:
        vector_index_path = run_pipeline(seed_url=site_link, scraper_db_path=existing_scraped)
        site_results_map[site_link] = {
            "scraped_db_path": existing_scraped,
            "vector_index_path": vector_index_path,
        }
        return vector_index_path

    # Otherwise, run scraper then pipeline
    scraped_db_path = asyncio.run(run_scraper(site_link))
    vector_index_path = run_pipeline(seed_url=site_link, scraper_db_path=scraped_db_path)
    site_results_map[site_link] = {
        "scraped_db_path": scraped_db_path,
        "vector_index_path": vector_index_path,
    }
    return vector_index_path
 

def answer_question(user_query: str, site_link: str | None = None, chat_history: list[tuple[str, str]] | None = None):
    # Use the site-specific index if provided; otherwise default
    chunks = find_content(user_query, seed_url=site_link)
    if not chunks:
        return "No relevant content found."
    # Return non-embedding formatted text for the UI
    import re
    price_query = any(kw in user_query.lower() for kw in ["price", "prices", "cost", "$", "usd"])

    # Extract price-focused snippets first if applicable
    price_lines = []
    if price_query:
        price_re = re.compile(r"\$\s?\d[\d.,]*")
        for item in chunks:
            title = item.get("title") or ""
            url = item.get("url") or ""
            text = (item.get("text") or "").splitlines()
            for ln in text:
                if price_re.search(ln):
                    snippet = ln.strip()
                    if len(snippet) > 240:
                        snippet = snippet[:240] + "…"
                    price_lines.append(f"- {title} | {url}\n{snippet}")
        # de-duplicate while preserving order
        seen = set()
        dedup = []
        for pl in price_lines:
            if pl not in seen:
                seen.add(pl)
                dedup.append(pl)
        price_lines = dedup[:6]

    lines = []
    for item in chunks:
        title = item.get("title") or ""
        url = item.get("url") or ""
        text = item.get("text") or ""
        score = item.get("score")
        score_str = f" (score: {score:.4f})" if isinstance(score, float) else ""
        lines.append(f"- {title} | {url}{score_str}\n{text[:500]}")
    context_blocks = []
    if price_lines:
        context_blocks.append("Price highlights:\n" + "\n\n".join(price_lines))
    context_blocks.append("\n\n".join(lines))
    context = "\n\n".join(context_blocks)
    print(f"{context}")
    answer = ask_llm(user_query, context, chat_history=chat_history)

    # Attach source links for frontend clarity
    sources = "\n".join([f"- {item.get('url')}" for item in chunks if item.get("url")])
    return f"**Answer:** {answer}\n\n**Sources:**\n{sources}"



    
def main():
    parser = argparse.ArgumentParser(description="Project_YA CLI")
    sub = parser.add_subparsers(dest="command", required=True)

    # scrape subcommand
    sub.add_parser("scrape", help="Run the web scraper")
    # pipeline subcommand
    sub.add_parser("pipeline", help="Run data processing pipeline")
    # all subcommand: scrape then pipeline
    sub.add_parser("all", help="Run scraper then pipeline")
    args = parser.parse_args()

    if args.command == "scrape":
        # Expect the base URL to be provided via config or future CLI arg
        scraped_db_path = asyncio.run(run_scraper())
        print(scraped_db_path)

    elif args.command == "pipeline":
        # Run pipeline using default configured DB paths
        out = run_pipeline()
        print(out)

    elif args.command == "all":
        scraped_db_path = asyncio.run(run_scraper())
        out = run_pipeline(scraper_db_path=scraped_db_path)
        print(out)


if __name__ == "__main__":
    main()
