import os
import logging
import scrapy
from scrapy.crawler import CrawlerProcess

class LouvreSpider(scrapy.Spider):
    name = "louvre"
    start_urls = [
        'https://collections.louvre.fr/en/recherche',
    ]

    def parse(self, response):
        # Scraping the elements on the current page
        for i in range(1, 21):
            name = response.xpath(f'//section/div[2]/div[2]/div[1]/ul/li[{i}]/article/div/div[2]/h3/a/text()').get()
            url = response.xpath(f'//section/div[2]/div[2]/div[1]/ul/li[{i}]/article/div/div[1]/a/@href').get()
            
            if name and url:
                yield {
                    'name': name,
                    'url': url,
                }
        
        # Extracting the total number of pages
        total_pages = '500' #response.xpath('/html/body/div[1]/main/section/div[2]/div[2]/div[2]/nav/form/span[2]/text()').get()
        if total_pages:
            total_pages = int(total_pages.split()[-1])  # Extracting the last number from the text
        else:
            total_pages = 1

        # Logging the total number of pages
        self.log(f"Total number of pages: {total_pages}", level=logging.INFO)
        
        # Iterate through each page and scrape data
        for page_number in range(2, total_pages + 1):
            next_page_url = f'https://collections.louvre.fr/en/recherche?page={page_number}'
            yield scrapy.Request(next_page_url, callback=self.parse_page)

    def parse_page(self, response):
        # Scraping the elements on the current page
        for i in range(1, 21):
            name = response.xpath(f'//section/div[2]/div[2]/div[1]/ul/li[{i}]/article/div/div[2]/h3/a/text()').get()
            url = response.xpath(f'//section/div[2]/div[2]/div[1]/ul/li[{i}]/article/div/div[1]/a/@href').get()
            if name and url:
                yield {
                    'name': name,
                    'url': url,
                }

# Name of the file where the results will be saved
filename = "urls_le_louvre_all.json"

# Directory to save the file
save_dir = os.path.join(os.path.dirname(__file__), '..', 'data')

# Absolute path to the file
filepath = os.path.join(save_dir, filename)

# If file already exists, delete it before crawling (because Scrapy will 
# concatenate the last and new results otherwise)
if os.path.exists(filepath):
    os.remove(filepath)

# Declare a new CrawlerProcess with some settings
process = CrawlerProcess(settings={
    'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/97.0.4692.99 Safari/537.36',
    'LOG_LEVEL': logging.INFO,
    "FEEDS": {
        filepath: {"format": "json"},
    }
})

# Start the crawling using the spider you defined above
process.crawl(LouvreSpider)
process.start()
