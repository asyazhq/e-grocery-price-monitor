import csv
from pathlib import Path

from scrape_yandex_carefood import scrape_carefood_yandex


OUTPUT = Path("data/reviews/carefood_reviews.csv")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)


def main():
    print("[START] Carefood — Yandex Maps (Selenium)")

    reviews = scrape_carefood_yandex(
        max_scrolls=70  
    )

    with open(OUTPUT, "w", encoding="utf-8-sig", newline="") as f:
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

    print(f"[DONE] Saved {len(reviews)} reviews to {OUTPUT}")


if __name__ == "__main__":
    main()
