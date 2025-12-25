import os
import re
import scrapy
from scrapy.selector import Selector
from scrapy_playwright.page import PageMethod


def to_int_price(value: str | None) -> int | None:
    if not value:
        return None
    digits = re.sub(r"[^\d]", "", value)
    return int(digits) if digits else None


def clean_text(s: str | None) -> str | None:
    if not s:
        return None
    s = re.sub(r"\s+", " ", s).strip()
    return s or None


class CleverSpider(scrapy.Spider):
    name = "clever"
    allowed_domains = ["clevermarket.kz"]

    START_URL = "https://clevermarket.kz/supermarket"

    custom_settings = {
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 0.8,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_MAX_DELAY": 10.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,

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

    async def start(self):
        yield scrapy.Request(
            url=self.START_URL,
            callback=self.parse_category_index,
            dont_filter=True,
        )

    def parse_category_index(self, response: scrapy.http.Response):
        links = response.css('a[href^="/supermarket/catalog/"]::attr(href)').getall()
        links = [l.split("#")[0] for l in links if l]

        seen = set()
        categories = []

        for href in links:
            full = response.urljoin(href)
            m = re.search(r"/supermarket/catalog/[^/]+/(\d+)", full)
            if not m:
                continue
            cid = m.group(1)

            if cid in seen:
                continue
            seen.add(cid)

            categories.append({"url": full, "category_id": cid})

        self.logger.info("Clever categories found: %d", len(categories))

        if not categories:
            self.logger.error("No category links found on %s. Page markup might have changed.", response.url)
            return

        MAX_CATEGORIES = 40
        for c in categories[:MAX_CATEGORIES]:
            yield scrapy.Request(
                url=c["url"],
                callback=self.parse_category,
                meta={
                    "playwright": True,
                    "playwright_include_page": True,
                    "playwright_page_methods": [
                        PageMethod("wait_for_load_state", "domcontentloaded"),
                        PageMethod("wait_for_timeout", 1200),
                        PageMethod("wait_for_selector", "div.product-card.product-card-item", timeout=30000),
                    ],
                    "category_id": c["category_id"],
                    "category_url": c["url"],
                },
                dont_filter=True,
            )

    async def parse_category(self, response: scrapy.http.Response):
        page = response.meta["playwright_page"]

        prev_count = 0
        stable_rounds = 0

        max_rounds = 60
        rounds = 0

        while stable_rounds < 2 and rounds < max_rounds:
            rounds += 1

            cards = await page.query_selector_all("div.product-card.product-card-item")
            cur_count = len(cards)

            if cur_count == prev_count:
                stable_rounds += 1
            else:
                stable_rounds = 0
                prev_count = cur_count

            clicked = await page.evaluate(
                """() => {
                    const buttons = Array.from(document.querySelectorAll("button"));
                    const btn = buttons.find(b => {
                        const t = (b.innerText || "").trim();
                        return /показать|загрузить|ещ[её]/i.test(t) && !b.disabled;
                    });
                    if (btn) {
                        btn.scrollIntoView({block: "center"});
                        btn.click();
                        return true;
                    }
                    return false;
                }"""
            )

            if not clicked:
                await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1200)

        html = await page.content()
        await page.close()

        sel = Selector(text=html)

        category_id = str(response.meta["category_id"])
        category_name = self._extract_category_name(sel, category_id)

        seen_products = set()

        for card in sel.css("div.product-card.product-card-item"):
            href = card.css('a[href^="/supermarket/product/"]::attr(href)').get()
            if not href:
                continue

            product_id = href.rstrip("/").split("/")[-1]
            if product_id in seen_products:
                continue
            seen_products.add(product_id)

            name = clean_text(card.css("div.product-card-title::text").get())
            if not name:
                name = clean_text("".join(card.css(".product-card-title ::text").getall()))

            brand = clean_text(card.css(".product-card-brand::text, .product-card__brand::text").get())
            if not brand:
                brand = clean_text(card.css("div.product-card-subtitle::text").get())

            price_text = clean_text("".join(card.css("div.text-sm.font-semibold ::text").getall()))
            if not price_text:
                price_text = clean_text("".join(card.css(".product-card-price ::text, .product-card__price ::text").getall()))
            price_kzt = to_int_price(price_text)

            unit = clean_text(card.css(".price-measure::text, .product-card-measure::text, .product-card__measure::text").get())
            if not unit:
                unit = self._extract_unit_from_price(price_text)

            yield {
                "store": "clever",
                "city": "astana",
                "category_name": category_name,
                "category_id": int(category_id) if category_id.isdigit() else category_id,
                "product_id": int(product_id) if product_id.isdigit() else product_id,
                "product_name": name,
                "brand": brand,
                "price_kzt": price_kzt,
                "currency": "KZT",
                "unit": unit,
                "source": "clevermarket.kz",
            }

    @staticmethod
    def _extract_unit_from_price(price_text: str | None) -> str | None:
        if not price_text:
            return None
        t = price_text.lower()
        m = re.search(r"/\s*([а-яa-z.]+)", t)
        if m:
            return m.group(1).strip()
        m = re.search(r"\b(кг|г|л|мл|шт|уп|пач|пак)\b", t)
        if m:
            return m.group(1).strip()
        return None

    @staticmethod
    def _extract_category_name(sel: Selector, category_id: str) -> str:
        og_image = sel.css('meta[property="og:image"]::attr(content)').get() or ""
        if og_image:
            filename = og_image.split("/")[-1].split("?")[0]
            base = os.path.splitext(filename)[0]

            base = re.sub(r"^S_[0-9.]+_", "", base)
            base = base.replace(",_", ", ")
            base = base.replace("_", " ")
            base = re.sub(r"\s+", " ", base).strip()
            if base:
                return base

        title = (
            sel.css('meta[property="og:title"]::attr(content)').get()
            or sel.css("title::text").get()
            or ""
        ).strip()

        if title:
            title = re.split(r"\s+\|\s+", title, maxsplit=1)[0].strip()
            return title or f"category_{category_id}"

        return f"category_{category_id}"
