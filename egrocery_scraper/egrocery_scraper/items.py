import scrapy


class ProductItem(scrapy.Item):
    store = scrapy.Field()
    city = scrapy.Field()
    category_name = scrapy.Field()
    category_id = scrapy.Field()
    product_id = scrapy.Field()
    product_name = scrapy.Field()
    brand = scrapy.Field()
    price_kzt = scrapy.Field()
    currency = scrapy.Field()
