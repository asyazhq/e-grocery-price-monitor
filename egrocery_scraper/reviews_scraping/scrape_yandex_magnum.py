import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager


def scrape_magnum_yandex_selenium(
    max_scrolls=50,
    sleep_sec=2.0
):
    url = "https://yandex.kz/maps/ru/org/magnum/245020898803/reviews/"

    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.get(url)
    time.sleep(5)

    reviews = []

    for i in range(max_scrolls):
        driver.execute_script(
            "document.querySelector('div[role=\"main\"]').scrollTop += 1000"
        )
        time.sleep(sleep_sec)

    blocks = driver.find_elements(By.CSS_SELECTOR, "div[data-review-id]")

    for b in blocks:
        try:
            text = b.find_element(By.CSS_SELECTOR, "span").text
            rating = b.find_element(By.CSS_SELECTOR, "span[aria-label]").get_attribute("aria-label")
            date = b.find_element(By.CSS_SELECTOR, "span[class*='date']").text
        except Exception:
            continue

        reviews.append({
            "company": "Magnum",
            "source": "yandex_maps",
            "review_text": text,
            "rating": rating,
            "review_date": date,
            "scraped_at": datetime.utcnow().isoformat()
        })

    driver.quit()
    return reviews
