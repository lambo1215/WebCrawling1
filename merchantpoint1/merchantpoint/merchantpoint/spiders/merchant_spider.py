import scrapy

class MerchantSpider(scrapy.Spider):
    name = 'merchant_spider'
    allowed_domains = ['merchantpoint.ru']
    start_urls = ['https://merchantpoint.ru/brands']

    def parse(self, response):
        # Extract brand links from the table
        brand_links = response.xpath("//table/tbody/tr/td[2]/a/@href").getall()
        for link in brand_links:
            yield response.follow(link, callback=self.parse_brand)
        
        # Follow pagination to get all brands
        next_page = response.xpath("//ul[@class='pagination']/li[@class='page-item']/a[@class='page-link'][contains(text(), 'Вперед')]/@href").get()
        if next_page:
            yield response.follow(next_page, callback=self.parse)

    def parse_brand(self, response):
        # Extract brand information
        org_name = response.xpath("//h1[@class='h2']/text()").get()
        
        # Get description - handling multi-paragraph case
        description_paragraphs = response.xpath("//div[@id='home']//div[@class='form-group mb-2']/p/text()").getall()
        org_description = " ".join([p.strip() for p in description_paragraphs if p.strip()]) if description_paragraphs else None
        
        source_url = response.url
        
        # Check for terminal information
        has_terminal_info = not bool(response.xpath("//div[@id='terminals']//li[contains(text(), 'У нас нет информации')]/text()").get())
        
        # If the brand has terminal information
        if has_terminal_info:
            store_links = response.xpath("//div[@id='terminals']//a[contains(@href, '/store/') or contains(@href, '/point/')]/@href").getall()
            if store_links:
                for link in store_links:
                    yield response.follow(link, callback=self.parse_store, meta={
                        'org_name': org_name,
                        'org_description': org_description,
                        'source_url': source_url
                    })
                return
        
        # If no terminals found, yield brand information as the main item
        yield {
            'merchant_name': org_name,
            'mcc': None,
            'address': None,
            'geo_coordinates': None,
            'org_name': org_name,
            'org_description': org_description,
            'source_url': source_url
        }

    def parse_store(self, response):
        # Extract store information using multiple possible XPath patterns
        merchant_name = response.xpath("//div[contains(@class, 'merchant-name')]/text() | //h1/text() | //table//tr[1]/td[3]/text()").get()
        mcc = response.xpath("//span[contains(@class, 'mcc-code')]/text() | //table//tr[1]/td[2]/text()").get()
        address = response.xpath("//div[contains(@class, 'address')]/text() | //table//tr[1]/td[4]/text()").get()
        geo_coordinates = response.xpath("//div[contains(@class, 'geo-coordinates')]/text() | //table//tr[1]/td[5]/text()").get()

        yield {
            'merchant_name': merchant_name,
            'mcc': mcc,
            'address': address,
            'geo_coordinates': geo_coordinates,
            'org_name': response.meta['org_name'],
            'org_description': response.meta['org_description'],
            'source_url': response.meta['source_url']
        }

