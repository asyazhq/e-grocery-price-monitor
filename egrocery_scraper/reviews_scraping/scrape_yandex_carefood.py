import time
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager


CAREFOOD_URL = (
    "https://yandex.kz/maps/ru/org/carefood/55153753696/reviews/"
)


def scrape_carefood_yandex(max_scrolls=60, sleep_sec=2):
    options = Options()
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.get(CAREFOOD_URL)
    time.sleep(5)

    for _ in range(max_scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(sleep_sec)

    reviews = []

    blocks = driver.find_elements(By.CSS_SELECTOR, "div[data-review-id]")

    for block in blocks:
        try:
            text = block.find_element(By.CSS_SELECTOR, "span").text

            rating_el = block.find_element(
                By.CSS_SELECTOR, "span[aria-label*='оцен']"
            )
            rating = int("".join(filter(str.isdigit, rating_el.get_attribute("aria-label"))))

            date_el = block.find_element(By.XPATH, ".//span[contains(text(),'г')]")
            review_date = date_el.text

            reviews.append({
                "company": "Carefood",
                "source": "yandex_maps",
                "review_text": text,
                "rating": rating,
                "review_date": review_date,
                "scraped_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
            })
        except Exception:
            continue

    driver.quit()
    return reviews
