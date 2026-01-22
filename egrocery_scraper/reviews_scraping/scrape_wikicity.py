import json
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import html
import re


def fix_encoding(text: str) -> str:
    try:
        return text.encode("latin1").decode("utf-8")
    except Exception:
        return text


def clean_text(text: str) -> str:
    text = html.unescape(text)          
    text = re.sub(r"<br\s*/?>", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def scrape_wikicity_reviews(url):
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0"},
        timeout=20
    )
    response.encoding = "utf-8"  

    soup = BeautifulSoup(response.text, "html.parser")

    scripts = soup.find_all(
        "script",
        type="application/ld+json"
    )

    reviews = []

    for script in scripts:
        try:
            data = json.loads(script.string)
        except Exception:
            continue

        if isinstance(data, dict) and "review" in data:
            for r in data["review"]:
                raw_text = r.get("description", "")
                fixed = fix_encoding(raw_text)
                cleaned = clean_text(fixed)

                reviews.append({
                    "company": "Arbuz",
                    "source": "wikicity",
                    "review_text": cleaned,
                    "rating": int(r["reviewRating"]["ratingValue"]),
                    "review_date": r.get("datePublished"),
                    "scraped_at": datetime.utcnow().isoformat()
                })

    return reviews
