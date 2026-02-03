import requests
import json
import csv
import time
from pathlib import Path
from typing import Dict, List, Set

# “Decision support systems for price comparison and consumer choice in e-grocery platforms”

BASE_URL = "https://api.openalex.org/works"

# ШИРОКИЕ НАУЧНЫЕ ЗАПРОСЫ 
SEARCH_QUERIES = [
    "price comparison in e-grocery",
    "pricing",
    "consumer behavior",
    "consumer choice",
    "decision support",
    "decision making",
    "e-commerce",
    "online retail",
    "digital platform"
]

FROM_DATE = "2015-01-01"
TO_DATE = "2025-12-31"

PER_PAGE = 200
TARGET_RECORDS = 20000        # собираем большой корпус
REQUEST_DELAY = 1.0

DATA_DIR = Path("data/openalex")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

RAW_JSON_PATH = RAW_DIR / "openalex_raw.json"
CSV_PATH = PROCESSED_DIR / "openalex_publications.csv"

# SEMANTIC FILTERS

CORE_KEYWORDS = [
    "price",
    "pricing",
    "consumer",
    "decision",
    "choice",
    "recommendation",
    "utility"
]

DIGITAL_CONTEXT_KEYWORDS = [
    "online",
    "digital",
    "platform",
    "retail",
    "commerce",
    "shopping",
    "marketplace"
]

EXCLUDE_KEYWORDS = [
    "medical",
    "health",
    "clinical",
    "energy",
    "power",
    "microgrid",
    "climate",
    "environment",
    "renewable",
    "manufacturing",
    "industrial",
    "chemical",
    "biological"
]

# HELPERS

def normalize(text: str) -> str:
    return text.lower() if text else ""


def is_relevant(work: Dict) -> bool:
    """
    Logical AND implemented in Python:
    (economic/decision core) AND (digital/retail context)
    """
    text = " ".join([
        normalize(work.get("title", "")),
        normalize(work.get("abstract", "")),
    ])

    core_hits = sum(k in text for k in CORE_KEYWORDS)
    context_hits = sum(k in text for k in DIGITAL_CONTEXT_KEYWORDS)

    if core_hits < 1:
        return False

    if context_hits < 1:
        return False

    if any(k in text for k in EXCLUDE_KEYWORDS):
        return False

    return True


def extract_authors(work: Dict) -> str:
    authors = {
        auth.get("author", {}).get("display_name")
        for auth in work.get("authorships", [])
        if auth.get("author", {}).get("display_name")
    }
    return "; ".join(sorted(authors))


def extract_countries(work: Dict) -> str:
    countries = set()
    for auth in work.get("authorships", []):
        for inst in auth.get("institutions", []):
            code = inst.get("country_code")
            if code:
                countries.add(code)
    return "; ".join(sorted(countries)) if countries else "Unknown"


def extract_keywords(work: Dict) -> str:
    keywords = {
        c.get("display_name")
        for c in work.get("concepts", [])
        if c.get("display_name")
    }
    return "; ".join(sorted(keywords))

def extract_venue(work: Dict) -> str:
    primary_location = work.get("primary_location")
    if primary_location:
        source = primary_location.get("source")
        if source and source.get("display_name"):
            return source["display_name"]

    host_venue = work.get("host_venue")
    if host_venue and host_venue.get("display_name"):
        return host_venue["display_name"]

    locations = work.get("locations", [])
    for loc in locations:
        source = loc.get("source")
        if source and source.get("display_name"):
            return source["display_name"]

    return "Unknown"


# OPENALEX SCRAPER

def fetch_openalex() -> List[Dict]:
    print("[INFO] Starting OpenAlex broad collection")

    collected: List[Dict] = []
    seen_ids: Set[str] = set()

    for query in SEARCH_QUERIES:
        cursor = "*"
        print(f"[INFO] Query: {query}")

        while cursor and len(collected) < TARGET_RECORDS:
            params = {
                "search": query,
                "filter": (
                    f"from_publication_date:{FROM_DATE},"
                    f"to_publication_date:{TO_DATE}"
                ),
                "per-page": PER_PAGE,
                "cursor": cursor
            }

            response = requests.get(BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()

            for work in data.get("results", []):
                work_id = work.get("id")
                if work_id and work_id not in seen_ids:
                    seen_ids.add(work_id)
                    collected.append(work)

            cursor = data.get("meta", {}).get("next_cursor")
            print(f"[INFO] Collected so far: {len(collected)}")

            time.sleep(REQUEST_DELAY)

            if not cursor:
                break

        if len(collected) >= TARGET_RECORDS:
            break

    print(f"[INFO] Finished collection. Total works: {len(collected)}")
    return collected

# SAVE DATA

def save_raw_json(data: List[Dict]) -> None:
    with open(RAW_JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[INFO] Raw JSON saved: {RAW_JSON_PATH}")


def build_csv(data: List[Dict]) -> None:
    rows = []
    seen = set()

    relevant = [w for w in data if is_relevant(w)]
    print(f"[INFO] Relevant after semantic filtering: {len(relevant)} / {len(data)}")

    for w in relevant:
        key = (w.get("title"), w.get("publication_year"))
        if key in seen:
            continue
        seen.add(key)

        rows.append({
            "title": w.get("title"),
            "year": w.get("publication_year"),
            "authors": extract_authors(w),
            "journal": extract_venue(w),
            "number_of_citations": w.get("cited_by_count", 0),
            "keywords": extract_keywords(w),
            "country": extract_countries(w)
        })

    with open(CSV_PATH, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    print(f"[INFO] CSV saved with {len(rows)} rows: {CSV_PATH}")


# MAIN

def main():
    data = fetch_openalex()
    save_raw_json(data)
    build_csv(data)


if __name__ == "__main__":
    main()
