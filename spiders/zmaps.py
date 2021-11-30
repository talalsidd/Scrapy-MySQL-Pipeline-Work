import math
import time
from ..items import ZmapsItem
import scrapy
import re
from datetime import datetime,timedelta
from datetime import date

allpages = ["https://zmaps.net/cat/nonclassified-establishments?Page=0"]

class zMapSpider(scrapy.Spider):
    name = 'zmaps'
    start_urls =['https://zmaps.net/']

    #items = ScrapinghubtestItem()

    def parse(self, response, **kwargs):
        for page in range(len(allpages)):
            yield scrapy.Request(allpages[page], callback=self.parse_data)
     #   cats = ["https://zmaps.net/cat/hair-salons","https://zmaps.net/cat/painters","https://zmaps.net/cat/real-estate-management","https://zmaps.net/cat/home-renovation"];
     #   #cats = response.css('[class="container"]>[class*="col-md-12"]>[class*="random-tags"]>a::attr(href)').extract()
     #   print(cats)
     #   if cats is not None:
     #       for cat in cats:
     #           yield scrapy.Request(cat, callback=self.parse_pages)
     #   else:
     #       pass

    #def parse_pages(self, response):
            #count1 = response.css('[class*="pagination"] li:nth-last-of-type(1)>a::attr(href)').get()
            #count = re.sub(r".*Page\=", "", count1)
            #print(count)
            #total_count = int(count)
            #k = 0
            #while k < total_count:
            #    next_page_url = response.request.url + '?Page=' + str(k)
            #    allpages.append(next_page_url)
            #    k = k + 1
            #time.sleep(.5)

    def parse_data(self, response):
        prodlink = response.css('[class*="list-box"] li')
        # print(prodlink)
        for prods in prodlink:
            items = ZmapsItem()
            url = 'https://zmaps.net' + prods.css('a[class*="align-items-center"]::attr(href)').get()
            url_id = re.sub(r".*\-", "", url)
            crawl_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            items['url_id'] = url_id
            items['url'] = url
            items['crawl_time'] = crawl_time
            # print(title)
            yield items
