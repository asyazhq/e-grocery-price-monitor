import re
import scrapy
from scrapy.selector import Selector
from scrapy_playwright.page import PageMethod


class CleverSpider(scrapy.Spider):
    name = "clever"
    allowed_domains = ["clevermarket.kz"]

    CATEGORIES = [
        {
            "category_name": "Молочка",
            "category_id": "1118",
            "url": "https://clevermarket.kz/supermarket/catalog/Molochnie-produkti-yaitso/1118",
        },
        {
            "category_name": "Овощи",
            "category_id": "1089",
            "url": "https://clevermarket.kz/supermarket/catalog/Ovoshchi-zelen-gribi-solenya/1089",
        },
        {
            "category_name": "Хлеб",
            "category_id": "1151",
            "url": "https://clevermarket.kz/supermarket/catalog/Khleb/1151",
        },
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 1.0,
        "AUTOTHROTTLE_ENABLED": True,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,
        "FEED_EXPORT_FIELDS": [
            "store",
            "city",
            "category_name",
            "category_id",
            "product_id",
            "product_name",
            "price_kzt",
            "currency",
            "source",
        ],
    }

    def start_requests(self):
        for c in self.CATEGORIES:
            yield scrapy.Request(
                c["url"],
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_selector", "div.product-card.product-card-item", timeout=30000),
                    ],
                    "category_name": c["category_name"],
                    "category_id": c["category_id"],
                },
                callback=self.parse_category,
            )

    async def parse_category(self, response):
        page = response.meta["playwright_page"]

        prev_count = 0
        stable_rounds = 0

        while stable_rounds < 2:  
            cards = await page.query_selector_all("div.product-card.product-card-item")
            cur_count = len(cards)

            if cur_count == prev_count:
                stable_rounds += 1
            else:
                stable_rounds = 0
                prev_count = cur_count

            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1200)

        html = await page.content()
        await page.close()

        sel = Selector(text=html)

        category_name = response.meta["category_name"]
        category_id = response.meta["category_id"]

        seen = set()

        for card in sel.css("div.product-card.product-card-item"):
            href = card.css('a[href^="/supermarket/product/"]::attr(href)').get()
            if not href:
                continue

            product_id = href.rstrip("/").split("/")[-1]
            if product_id in seen:
                continue
            seen.add(product_id)

            name = card.css("div.product-card-title::text").get()
            name = (name or "").strip() or None

            price_text = "".join(card.css("div.text-sm.font-semibold ::text").getall()).strip()
            price_kzt = self._parse_price_kzt(price_text)

            yield {
                "store": "clever",
                "city": "astana",
                "category_name": category_name,
                "category_id": category_id,
                "product_id": product_id,
                "product_name": name,
                "price_kzt": price_kzt,
                "currency": "KZT",
                "source": "clevermarket.kz",
            }

    @staticmethod
    def _parse_price_kzt(text: str):
        if not text:
            return None
        m = re.search(r"(\d[\d\s.,]*)", text)
        if not m:
            return None
        digits = re.sub(r"[^\d]", "", m.group(1))
        return int(digits) if digits else None
