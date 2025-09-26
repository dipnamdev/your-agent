import argparse
import asyncio
from web_scraper.storage.db_manager import get_all_pages, get_page_by_url, set_db_path

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True, help="Path to site-specific scraper DB")
    args = parser.parse_args()

    set_db_path(args.db)
    print(f"Reading from DB: {args.db}")

    # Fetch all pages
    pages = await get_all_pages()

    for row in pages[:1]:
        print(row)

if __name__ == "__main__":
    asyncio.run(main())
