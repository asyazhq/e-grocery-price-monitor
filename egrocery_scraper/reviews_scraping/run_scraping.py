import csv
from pathlib import Path

from scrape_yandex import scrape_yandex_reviews
from scrape_wikicity import scrape_wikicity_reviews
from scrape_zoon import scrape_zoon_reviews


OUTPUT = Path("../data/reviews/arbuz_reviews.csv")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)


def save_csv(reviews: list[dict]):
    with OUTPUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=reviews[0].keys())
        writer.writeheader()
        writer.writerows(reviews)


def main():
    all_reviews = []

    yandex = scrape_yandex_reviews("https://reviews.yandex.ru/shop/arbuz.kz")
    print(f"[OK] Yandex: collected {len(yandex)} reviews")
    all_reviews += yandex

    wikicity = scrape_wikicity_reviews(
        "https://wikicity.kz/biz/internet-magazin-arbuz-kz-astana"
    )
    print(f"[OK] WikiCity: collected {len(wikicity)} reviews")
    all_reviews += wikicity

    zoon = scrape_zoon_reviews(
    "https://zoon.kz/almaty/shops/sluzhba_dostavki_produktov_arbuzkz/reviews/"
    )
    print(f"[OK] Zoon: collected {len(zoon)} reviews")
    all_reviews += zoon

    save_csv(all_reviews)
    print(f"[DONE] Total collected: {len(all_reviews)} reviews")
    print(f"[FILE] Saved to: {OUTPUT.resolve()}")


if __name__ == "__main__":
    main()
