import re
import scrapy


def to_int_price(value: str | None) -> int | None:
    if not value:
        return None
    digits = re.sub(r"[^\d]", "", value)
    return int(digits) if digits else None


class VkusmartSpider(scrapy.Spider):
    name = "vkusmart"
    allowed_domains = ["vkusmart.vmv.kz"]

    CATEGORIES = [
        {"category_name": "Молочка", "category_id": None, "url": "https://vkusmart.vmv.kz/catalog/moloko-slivki/"},
        {"category_name": "Овощи",   "category_id": None, "url": "https://vkusmart.vmv.kz/catalog/ovoshchi_1/"},
        {"category_name": "Хлеб",    "category_id": None, "url": "https://vkusmart.vmv.kz/catalog/khleb-bagety/"},
    ]

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
            "category_id",
            "product_id",
            "product_name",
            "brand",
            "price_kzt",
            "currency",
            "unit",
            "source",
        ],
    }

    def start_requests(self):
        for cat in self.CATEGORIES:
            yield scrapy.Request(
                url=cat["url"],
                callback=self.parse_category,
                meta={
                    "category_name": cat["category_name"],
                    "category_id": cat["category_id"],  
                    "store": "vkusmart",
                    "city": "astana",
                    "source": "vkusmart.vmv.kz",
                },
            )

    def parse_category(self, response: scrapy.http.Response):
        category_name = response.meta["category_name"]
        category_id = response.meta["category_id"]
        city = response.meta["city"]
        store = response.meta["store"]
        source = response.meta["source"]

        # Карточки товаров: div.item_block[data-id]
        for card in response.css('div.item_block[data-id]'):
            product_id = card.attrib.get("data-id")

            # Название
            product_name = card.css('meta[itemprop="name"]::attr(content)').get()
            if not product_name:
                product_name = card.css(".item-title a span::text").get()
            product_name = (product_name or "").strip() or None

            # Бренд (если есть в верстке — часто отсутствует)
            brand = card.css('meta[itemprop="brand"]::attr(content)').get()
            brand = (brand or "").strip() or None

            # Цена
            price_raw = card.css('meta[itemprop="price"]::attr(content)').get()
            if not price_raw:
                price_raw = card.css(".price::attr(data-value)").get()
            price_kzt = to_int_price(price_raw)

            # Валюта
            currency = card.css('meta[itemprop="priceCurrency"]::attr(content)').get()
            if not currency:
                currency = card.css(".price::attr(data-currency)").get()
            currency = (currency or "KZT").strip()

            # Единица измерения (/шт, /кг...)
            unit = card.css(".price_measure::text").get()
            unit = (unit or "").strip() or None

            yield {
                "store": store,                 
                "city": city,                   
                "category_name": category_name, 
                "category_id": category_id,     
                "product_id": int(product_id) if product_id and product_id.isdigit() else product_id,
                "product_name": product_name,
                "brand": brand,
                "price_kzt": price_kzt,
                "currency": currency,
                "unit": unit,
                "source": source,
            }

        next_url = response.css('link[rel="next"]::attr(href)').get()
        if not next_url:
            next_url = response.css(".module-pagination a.flex-next::attr(href)").get()

        if next_url:
            yield response.follow(next_url, callback=self.parse_category, meta=response.meta)
