import csv
from pathlib import Path
from scrape_yandex_magnum import scrape_magnum_yandex_selenium


OUTPUT = Path("data/reviews/magnum_reviews.csv")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)


def main():
    print("[START] Magnum — Yandex Maps (Selenium)")

    reviews = scrape_magnum_yandex_selenium(
        max_scrolls=60  
    )

    with open(OUTPUT, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "company",
                "source",
                "review_text",
                "rating",
                "review_date",
                "scraped_at"
            ]
        )
        writer.writeheader()
        writer.writerows(reviews)

    print(f"[DONE] Saved {len(reviews)} reviews")


if __name__ == "__main__":
    main()
