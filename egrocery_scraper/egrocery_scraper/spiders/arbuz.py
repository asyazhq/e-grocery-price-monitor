import json
import re
import html as html_std
from collections import defaultdict
from urllib.parse import urlencode

import scrapy
from scrapy.selector import Selector
from scrapy_playwright.page import PageMethod


class ArbuzSpider(scrapy.Spider):
    name = "arbuz"
    allowed_domains = ["arbuz.kz"]

    START_URL = "https://arbuz.kz/ru/astana/catalog/cat/19986-moloko_slivki_sgush_nnoe_moloko"

    API_LIMIT = 40
    MAX_PAGES_PER_CATEGORY = 200

    custom_settings = {
        "ROBOTSTXT_OBEY": False,

        "DOWNLOAD_DELAY": 0.6,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": 0.5,
        "AUTOTHROTTLE_MAX_DELAY": 8.0,
        "CONCURRENT_REQUESTS_PER_DOMAIN": 2,

        "HTTPERROR_ALLOWED_CODES": [401, 403],

        "USER_AGENT": "egrocery-price-monitor/1.0 (educational)",
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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.jwt = None

        self.seen_ids = defaultdict(set)

    async def start(self):
        yield scrapy.Request(
            url=self.START_URL,
            meta={
                "playwright": True,
                "playwright_include_page": True,
                "playwright_page_methods": [
                    PageMethod("wait_for_load_state", "domcontentloaded"),
                    PageMethod("wait_for_timeout", 1200),
                ],
            },
            callback=self.parse_seed_with_playwright,
            dont_filter=True,
        )

    async def parse_seed_with_playwright(self, response: scrapy.http.Response):
        page = response.meta["playwright_page"]

        cookies = await page.context.cookies()
        for c in cookies:
            if c.get("name") == "jwt" and c.get("value"):
                self.jwt = c["value"]
                break

        if self.jwt:
            self.logger.info("JWT cookie found: %s...", self.jwt[:12])
        else:
            self.logger.warning("JWT cookie NOT found. API may return 401; HTML-only fallback will still work.")

        html = await page.content()
        await page.close()

        categories = self._extract_categories_from_html(html)

        if not categories:
            self.logger.warning("No categories extracted from menu, fallback to START_URL only.")
            categories = [{
                "category_id": self._extract_category_id_from_url(response.url),
                "url": response.url,
                "category_name_hint": None,
            }]

        self.logger.info("Categories found: %d", len(categories))

        for cat in categories:
            if not cat.get("category_id") or not cat.get("url"):
                continue
            yield scrapy.Request(
                url=cat["url"].split("#")[0],
                callback=self.parse_category_html,
                meta={
                    "category_id": int(cat["category_id"]),
                    "category_name_hint": cat.get("category_name_hint"),
                },
                dont_filter=True,
            )

    def parse_category_html(self, response: scrapy.http.Response):
        category_id = int(response.meta["category_id"])
        category_name = self._extract_category_name(response, hint=response.meta.get("category_name_hint"))

        config = self._extract_platform_configuration(response.text)
        if not config:
            self.logger.error("platformConfiguration not found in %s", response.url)
            return

        page_view = config.get("pageView") or {}
        products = page_view.get("data") or []

        if isinstance(products, list) and products:
            for p in products:
                item = self._build_item_from_product_dict(
                    p,
                    category_id=category_id,
                    category_name=category_name,
                    source="arbuz.kz",
                )
                if not item:
                    continue
                pid = item["product_id"]
                if pid in self.seen_ids[category_id]:
                    continue
                self.seen_ids[category_id].add(pid)
                yield item

        if not self.jwt:
            return

        api_url = self._build_api_url(category_id=category_id, page=1, limit=self.API_LIMIT, token=self.jwt)
        yield scrapy.Request(
            url=api_url,
            callback=self.parse_category_api,
            meta={
                "category_id": category_id,
                "category_name": category_name,
                "page": 1,
                "limit": self.API_LIMIT,
                "cookies": {"jwt": self.jwt},
                "referer": response.url,
            },
            headers={
                "Accept": "application/json, text/plain, */*",
                "Referer": response.url,
            },
            dont_filter=True,
        )

    def parse_category_api(self, response: scrapy.http.Response):
        category_id = int(response.meta["category_id"])
        category_name = response.meta["category_name"]
        page = int(response.meta["page"])
        limit = int(response.meta["limit"])
        referer = response.meta.get("referer") or "https://arbuz.kz/"

        if response.status in (401, 403):
            self.logger.error("API %s returned %s. JWT/token not accepted.", response.url, response.status)
            return

        try:
            data = json.loads(response.text)
        except Exception:
            self.logger.error("API JSON decode error at %s", response.url)
            return

        products = self._find_products_list(data)

        if not products:
            return

        new_count = 0
        for p in products:
            item = self._build_item_from_product_dict(
                p,
                category_id=category_id,
                category_name=category_name,
                source="arbuz.kz",
            )
            if not item:
                continue
            pid = item["product_id"]
            if pid in self.seen_ids[category_id]:
                continue
            self.seen_ids[category_id].add(pid)
            new_count += 1
            yield item

        if page >= self.MAX_PAGES_PER_CATEGORY:
            return

        if len(products) >= limit and new_count > 0:
            next_page = page + 1
            next_url = self._build_api_url(category_id=category_id, page=next_page, limit=limit, token=self.jwt)
            yield scrapy.Request(
                url=next_url,
                callback=self.parse_category_api,
                meta={
                    "category_id": category_id,
                    "category_name": category_name,
                    "page": next_page,
                    "limit": limit,
                    "cookies": {"jwt": self.jwt},
                    "referer": referer,
                },
                headers={
                    "Accept": "application/json, text/plain, */*",
                    "Referer": referer,
                },
                dont_filter=True,
            )

    @staticmethod
    def _build_api_url(category_id: int, page: int, limit: int, token: str):
        qs = {
            "where[available][e]": "0",
            "limit": str(limit),
            "page": str(page),
            "token": token or "",
        }
        return f"https://arbuz.kz/api/v1/shop/catalog/{category_id}?{urlencode(qs)}"

    @staticmethod
    def _to_int(value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return int(value)
        s = str(value).strip()
        if not s:
            return None
        s = re.sub(r"[^\d]", "", s)
        return int(s) if s else None

    def _build_item_from_product_dict(self, p: dict, category_id: int, category_name: str, source: str):
        if not isinstance(p, dict):
            return None

        pid = p.get("id") or p.get("product_id")
        pid = self._to_int(pid)
        if pid is None:
            return None

        name = (p.get("name") or p.get("title") or "").strip() or None
        if not name:
            return None

        brand = (p.get("brand") or p.get("producer") or "").strip() or None

        price = p.get("price")
        if price is None and isinstance(p.get("prices"), dict):
            price = p["prices"].get("price")
        price_kzt = self._to_int(price)

        unit = p.get("measure") or p.get("unit") or p.get("measure_unit") or None
        unit = (str(unit).strip() if unit else None) or None

        return {
            "store": "arbuz",
            "city": "astana",
            "category_name": category_name,
            "category_id": int(category_id),
            "product_id": pid,
            "product_name": name,
            "brand": brand,
            "price_kzt": price_kzt,
            "currency": "KZT",
            "unit": unit,
            "source": source,
        }

    @staticmethod
    def _extract_category_id_from_url(url: str):
        m = re.search(r"/cat/(\d+)-", url)
        return int(m.group(1)) if m else None

    @staticmethod
    def _extract_categories_from_html(html: str):
        sel = Selector(text=html)
        cats = []

        seen = set()

        for a in sel.css('a[href*="/ru/astana/catalog/cat/"]'):
            href = a.attrib.get("href") or ""

            if href.startswith("/"):
                url = "https://arbuz.kz" + href
            else:
                url = href

            cid = ArbuzSpider._extract_category_id_from_url(url)
            if not cid:
                continue

            txt = "".join(a.css("::text").getall()).strip()
            txt = re.sub(r"\s+", " ", txt).strip()
            txt = txt or None

            key = (cid, url.split("#")[0])
            if key in seen:
                continue
            seen.add(key)

            cats.append({
                "category_id": cid,
                "url": url.split("#")[0],
                "category_name_hint": txt,
            })

        if not cats:
            for m in re.finditer(r'https?://arbuz\.kz/ru/astana/catalog/cat/(\d+)-[a-zA-Z0-9_\-]+', html):
                cid = int(m.group(1))
                url = m.group(0).split("#")[0]
                key = (cid, url)
                if key in seen:
                    continue
                seen.add(key)
                cats.append({"category_id": cid, "url": url, "category_name_hint": None})

        return cats

    @staticmethod
    def _extract_category_name(response: scrapy.http.Response, hint: str | None = None):

        og_title = response.css('meta[property="og:title"]::attr(content)').get()
        if og_title:
            t = html_std.unescape(og_title).strip()
            t = re.sub(r"\s+", " ", t).strip()
            m = re.match(r"^(.*?)\s+с доставкой\b", t, flags=re.IGNORECASE)
            if m:
                return m.group(1).strip()
            if "|" in t:
                return t.split("|", 1)[0].strip()
            return t

        if hint:
            return hint.strip()

        url = response.url
        m = re.search(r"/cat/\d+-([^/?#]+)", url)
        if m:
            return m.group(1).replace("_", " ").replace("-", " ").strip().title()

        return "Unknown"

    @staticmethod
    def _find_products_list(obj):
        if isinstance(obj, list):
            if obj and isinstance(obj[0], dict) and ("id" in obj[0] or "name" in obj[0]):
                return obj
            return []

        if isinstance(obj, dict):
            for k in ("data", "items", "products", "result"):
                v = obj.get(k)
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    return v

            for v in obj.values():
                found = ArbuzSpider._find_products_list(v)
                if found:
                    return found

        return []

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
