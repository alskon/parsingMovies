import scrapy
from scrapy.crawler import CrawlerProcess
from csv import reader


class ListsSpider(scrapy.Spider):
    name = "lists"
    allowed_domains = ["ru.wikipedia.org"]
    urls = []
    start_urls = ["https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"]

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse, dont_filter=True)

    def parse(self, response):
        for link in response.css('div.mw-category-columns a'):
            yield {
                'url': response.urljoin(link.css('::attr(href)').extract()[0]),
                'title': link.css('::text').extract()[0]
            }

        next_page = response.css('div#mw-pages a::attr(href)')[-1].extract()
        if next_page is not None:
            next_page_link = response.urljoin(next_page)
            yield scrapy.Request(url=next_page_link, callback=self.parse)


class FilmsSpider(scrapy.Spider):
    name = "films"
    allowed_domains = ["ru.wikipedia.org"]
    with open('output1.csv', 'r', encoding='utf-8') as urls_csv:
        urls = reader(urls_csv)
        urls = [row for row in urls]

    def start_requests(self):
        for link in range(1, len(self.urls)):
            yield scrapy.Request(url=self.urls[link][0], callback=self.parse, dont_filter=True)

    def parse(self, response):
        trs = [el.css('tr') for el in response.css('table.infobox tbody')][0]
        content = {'title': response.css('table.infobox').css('.infobox-above::text').extract()[0],}
        for tr in trs:
            th = tr.css("th ::text").extract()
            td = tr.css("td ::text").extract()
            if th and td:
                td = ' '.join(td).strip('\n').strip()
                th = ' '.join(th).strip('\n').strip()
                if th == 'Жанры': th = 'Жанр'
                if th == 'Страны': th = 'Страна'
                if th == 'Режиссёры': th = 'Режиссёр'
                content[th] = td
            if 'Жанр' not in content: content['Жанр'] = ''
            if 'Страна' not in content: content['Страна'] = ''
            if 'Режиссёр' not in content: content['Режиссёр'] = ''
            if 'Год' not in content: content['Год'] = ''
        yield {
            'title': content['title'],
            'genre': content['Жанр'],
            'director': content['Режиссёр'],
            'country': content['Страна'],
            'year': content['Год']
        }

c = CrawlerProcess({
    'USER_AGENT': 'Mozilla/5.0',
    'FEEDS': {'output2.csv': {'format': 'csv'}},
})
c.crawl(FilmsSpider)
c.start()
