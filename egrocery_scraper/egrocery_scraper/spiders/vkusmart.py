import re
from datetime import datetime, timezone
from urllib.parse import urljoin

import scrapy


def to_int_price(value: str | None) -> int | None:
    if not value:
        return None
    digits = re.sub(r"[^\d]", "", value)
    return int(digits) if digits else None


class VkusmartFullSpider(scrapy.Spider):
    name = "vkusmart_full"
    allowed_domains = ["vkusmart.vmv.kz"]
    start_urls = ["https://vkusmart.vmv.kz/catalog/"]

    custom_settings = {
        "DOWNLOAD_DELAY": 1.0,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,
        "DEFAULT_REQUEST_HEADERS": {
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        },
        "FEED_EXPORT_ENCODING": "utf-8-sig",
        "FEED_EXPORT_FIELDS": [
            "store",
            "city",
            "category_name",
            "category_url",
            "product_id",
            "product_name",
            "brand",
            "price_kzt",
            "currency",
            "unit",
            "source",
            "scraped_at",
        ],
    }

    def __init__(self, max_categories: int = 40, *args, **kwargs):
        super().__init__(*args, **kwargs)
        try:
            self.max_categories = max(1, int(max_categories))
        except Exception:
            self.max_categories = 40

    def parse(self, response: scrapy.http.Response):
        hrefs = response.css('a[href^="/catalog/"]::attr(href)').getall()
        urls = []
        seen = set()

        for href in hrefs:
            if not href:
                continue

            if any(x in href for x in ["#", "?", "/search/", "/compare/", "/basket/"]):
                continue

            url = urljoin(response.url, href)

            if not url.rstrip("/").endswith("/catalog") and "/catalog/" in url:
                if url not in seen:
                    seen.add(url)
                    urls.append(url)

        urls = [u for u in urls if u.rstrip("/") != "https://vkusmart.vmv.kz/catalog"]

        urls = urls[: self.max_categories]

        if not urls:
            self.logger.error("Не нашла категории на странице каталога: %s", response.url)
            return

        for url in urls:
            yield response.follow(
                url,
                callback=self.parse_category,
                meta={
                    "store": "vkusmart",
                    "city": "astana",
                    "source": "vkusmart.vmv.kz",
                    "category_url": url,
                },
            )

    def parse_category(self, response: scrapy.http.Response):
        store = response.meta["store"]
        city = response.meta["city"]
        source = response.meta["source"]
        category_url = response.meta["category_url"]

        category_name = response.css("h1::text").get()
        category_name = (category_name or "").strip() or None

        scraped_at = datetime.now(timezone.utc).isoformat()

        for card in response.css('div.item_block[data-id]'):
            product_id = card.attrib.get("data-id")

            product_name = card.css('meta[itemprop="name"]::attr(content)').get()
            if not product_name:
                product_name = card.css(".item-title a span::text").get()
            product_name = (product_name or "").strip() or None

            brand = card.css('meta[itemprop="brand"]::attr(content)').get()
            brand = (brand or "").strip() or None

            price_raw = card.css('meta[itemprop="price"]::attr(content)').get()
            if not price_raw:
                price_raw = card.css(".price::attr(data-value)").get()
            price_kzt = to_int_price(price_raw)

            currency = card.css('meta[itemprop="priceCurrency"]::attr(content)').get()
            if not currency:
                currency = card.css(".price::attr(data-currency)").get()
            currency = (currency or "KZT").strip()

            unit = card.css(".price_measure::text").get()
            unit = (unit or "").strip() or None

            yield {
                "store": store,
                "city": city,
                "category_name": category_name,
                "category_url": response.url, 
                "product_id": int(product_id) if product_id and product_id.isdigit() else product_id,
                "product_name": product_name,
                "brand": brand,
                "price_kzt": price_kzt,
                "currency": currency,
                "unit": unit,
                "source": source,
                "scraped_at": scraped_at,
            }

        next_url = response.css('link[rel="next"]::attr(href)').get()
        if not next_url:
            next_url = response.css(".module-pagination a.flex-next::attr(href)").get()

        if next_url:
            yield response.follow(next_url, callback=self.parse_category, meta=response.meta)
