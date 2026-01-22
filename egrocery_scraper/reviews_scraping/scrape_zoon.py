from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime
import time


def scrape_zoon_reviews(url: str) -> list[dict]:
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.get(url)
    time.sleep(3)

    last_count = 0

    while True:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        cards = driver.find_elements(By.CSS_SELECTOR, "div.comment-item")

        if len(cards) == last_count:
            break

        last_count = len(cards)

    reviews = []

    for card in cards:
        try:
            text = card.find_element(By.CSS_SELECTOR, ".comment-text").text
            rating = len(card.find_elements(By.CSS_SELECTOR, ".rating-stars__item--active"))
            date = card.find_element(By.CSS_SELECTOR, "time").get_attribute("datetime")[:10]

            reviews.append({
                "company": "Arbuz",
                "source": "zoon",
                "review_text": text.strip(),
                "rating": rating,
                "review_date": date,
                "scraped_at": datetime.utcnow().isoformat()
            })
        except:
            continue

    driver.quit()
    return reviews
