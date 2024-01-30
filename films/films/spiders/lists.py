import scrapy
import requests
from scrapy.http import TextResponse
from scrapy.crawler import CrawlerProcess


class FilmSpider(scrapy.Spider):
    name = "lists"
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
            if 'Жанр' not in content: content['Жанр'] = ''
            if 'Страна' not in content: content['Страна'] = ''
            if 'Режиссёр' not in content: content['Режиссёр'] = ''
            if 'Год' not in content: content['Год'] = ''
            content['Rate IMDb'] = ''
            if 'IMDb' in content:
                imdb = content['IMDb'][-7:]
                url_imdb = f'https://www.imdb.com/title/tt{imdb}/'
                headers = {
                    "Content-Type": "text",
                    "User-Agent": "Mozilla/5.0"}
                response = requests.get(url_imdb, headers=headers)
                content_imdb = TextResponse(body=response.content, url=url_imdb)
                rate = content_imdb.css('.cMEQkK::text').extract()[0]
                content['Rate IMDb'] = rate

        yield {
            'title': content['title'],
            'genre': content['Жанр'],
            'director': content['Режиссёр'],
            'country': content['Страна'],
            'year': content['Год'],
            'Rate IMDb': content['Rate IMDb']
        }


c = CrawlerProcess({
    'USER_AGENT': 'Mozilla/5.0',
    'FEEDS': {'output.csv': {'format': 'csv'}},
})
c.crawl(FilmSpider)
c.start()