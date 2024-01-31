import scrapy
from scrapy.crawler import CrawlerProcess


class FilmSpider(scrapy.Spider):
    name = "only_wiki"
    allowed_domains = ["ru.wikipedia.org"]
    urls = []
    start_urls = ["https://ru.wikipedia.org/wiki/Категория:Фильмы_по_алфавиту"]

    def start_requests(self):
        yield scrapy.Request(url=self.start_urls[0], callback=self.parse_url, dont_filter=True)

    def parse_url(self, response):
        for link in response.css('div.mw-category-columns a'):
            url = response.urljoin(link.css('::attr(href)').extract()[0])
            yield scrapy.Request(url=url, callback=self.parse)

        next_page = response.css('div#mw-pages a::attr(href)')[-1].extract()
        if next_page is not None:
            next_page_link = response.urljoin(next_page)
            yield scrapy.Request(url=next_page_link, callback=self.parse_url)

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
            if 'title' not in content: content['title'] = ''
            if 'Жанр' not in content: content['Жанр'] = ''
            if 'Страна' not in content: content['Страна'] = ''
            if 'Режиссёр' not in content: content['Режиссёр'] = ''
            if 'Год' not in content: content['Год'] = ''

        yield {
            'title': content['title'],
            'genre': content['Жанр'],
            'director': content['Режиссёр'],
            'country': content['Страна'],
            'year': content['Год'],
        }


c = CrawlerProcess({
    'USER_AGENT': 'Mozilla/5.0',
    'FEEDS': {'out_only_wiki.csv': {'format': 'csv'}},
})
c.crawl(FilmSpider)
c.start()