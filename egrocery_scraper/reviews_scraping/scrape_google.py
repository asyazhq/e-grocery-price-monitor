import time
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager


def scrape_google_reviews(place_url, company_name, scrolls=25):
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--window-size=1920,1080")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

    driver.get(place_url)
    time.sleep(6)

    for _ in range(scrolls):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1.5)

    blocks = driver.find_elements(By.CSS_SELECTOR, "div[data-review-id]")

    reviews = []
    for b in blocks:
        try:
            text = b.find_element(By.CSS_SELECTOR, "span[jsname='bN97Pc']").text
            rating = b.find_element(By.CSS_SELECTOR, "span[class*='kvMYJc']").get_attribute("aria-label")
        except Exception:
            continue

        reviews.append({
            "company": company_name,
            "source": "google_maps",
            "review_text": text,
            "rating": rating,
            "review_date": None,
            "scraped_at": datetime.utcnow().isoformat()
        })

    driver.quit()
    return reviews
