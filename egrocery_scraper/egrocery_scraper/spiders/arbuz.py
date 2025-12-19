import json
import re
import scrapy

from egrocery_scraper.items import ProductItem


class ArbuzSpider(scrapy.Spider):
    name = "arbuz"
    allowed_domains = ["arbuz.kz"]

    CATEGORY_MAP = [
        {
            "category_name": "Молочка",
            "category_id": 19986,
            "url": "https://arbuz.kz/ru/astana/catalog/cat/19986-moloko_slivki_sgush_nnoe_moloko",
        },
        {
            "category_name": "Овощи",
            "category_id": 225178,
            "url": "https://arbuz.kz/ru/astana/catalog/cat/225178-ovoshi",
        },
        {
            "category_name": "Хлеб",
            "category_id": 20118,
            "url": "https://arbuz.kz/ru/astana/catalog/cat/20118-hleb",
        },
    ]

    custom_settings = {
        "DOWNLOAD_DELAY": 1.0,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 1.0,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "ROBOTSTXT_OBEY": True,
        "USER_AGENT": "egrocery-price-monitor/1.0 (educational; contact: you@example.com)",
       
        "FEED_EXPORT_FIELDS": [
            "store",
            "city",
            "category_name",
            "category_id",
            "product_id",
            "product_name",
            "brand",
            "price_kzt",
            "currency",
        ],
    }

    def start_requests(self):
        for c in self.CATEGORY_MAP:
            yield scrapy.Request(
                url=c["url"],
                callback=self.parse_catalog,
                meta={
                    "category_name": c["category_name"],
                    "category_id": c["category_id"],
                },
            )

    def parse_catalog(self, response):
        category_name = response.meta["category_name"]
        category_id = response.meta["category_id"]

        config = self._extract_platform_configuration(response.text)
        if not config:
            self.logger.error("platformConfiguration not found: %s", response.url)
            return

        page_view = config.get("pageView") or {}
        if page_view.get("action") != "catalog":
            self.logger.warning("Unexpected pageView.action=%r at %s", page_view.get("action"), response.url)

        products = page_view.get("data") or []
        for p in products:
            item = ProductItem()

            item["store"] = "arbuz"      
            item["city"] = "astana"
            item["category_name"] = category_name
            item["category_id"] = int(category_id)

            item["product_id"] = int(p.get("id")) if p.get("id") is not None else None
            item["product_name"] = (p.get("name") or "").strip()

            brand = (p.get("brand") or "").strip()
            item["brand"] = brand if brand else None

            price = p.get("price")
            item["price_kzt"] = int(price) if price is not None else None
            item["currency"] = "KZT"

            yield item

    @staticmethod
    def _extract_platform_configuration(html: str):
        marker = "window.platformConfiguration"
        idx = html.find(marker)
        if idx == -1:
            return None
        
        eq_idx = html.find("=", idx)
        if eq_idx == -1:
            return None

        brace_start = html.find("{", eq_idx)
        if brace_start == -1:
            return None

        depth = 0
        in_string = False
        escape = False

        for i in range(brace_start, len(html)):
            ch = html[i]

            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == '"':
                    in_string = False
                continue
            else:
                if ch == '"':
                    in_string = True
                    continue
                if ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        json_text = html[brace_start : i + 1]
                        try:
                            return json.loads(json_text)
                        except json.JSONDecodeError:
                            return None

        return None