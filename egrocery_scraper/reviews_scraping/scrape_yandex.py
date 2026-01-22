import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime


def scrape_yandex_reviews(url):
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20
    )

    soup = BeautifulSoup(response.text, "html.parser")

    reviews = []

    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string)
        except Exception:
            continue

        if isinstance(data, dict) and "review" in data:
            for r in data["review"]:
                reviews.append({
                    "company": "Arbuz",
                    "source": "yandex_reviews",
                    "review_text": r.get("reviewBody"),
                    "rating": int(r["reviewRating"]["ratingValue"]),
                    "review_date": None,  
                    "scraped_at": datetime.utcnow().isoformat()
                })

    return reviews
