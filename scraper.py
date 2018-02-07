import datetime
import re
import json
from collections import defaultdict

import dataset
import lxml.html
import requests


class BGBLScraper(object):
    BASE_URL = 'https://www.bgbl.de/xaver/bgbl/'
    START = 'start.xav?startbk=Bundesanzeiger_BGBl'
    BASE_TOC = 'ajax.xav?q=toclevel&n=0'
    AJAX = 'ajax.xav?q=toclevel&bk=bgbl&n={}'
    TEXT = ('text.xav?SID=&tf=xaver.component.Text_0&tocf='
            '&qmf=&hlf=xaver.component.Hitlist_0'
            '&bk=bgbl&start=%2F%2F*%5B%40node_id%3D%27{docid}%27%5D')

    year_toc = defaultdict(dict)
    year_docs = defaultdict(dict)
    toc = {}

    def __init__(self, part_count=2):
        self.login()
        self.part_count = part_count

    def login(self):
        self.session = requests.Session()
        self.session.get(self.BASE_URL + self.START)

    def get(self, url):
        while True:
            response = self.session.get(url)
            if 'Session veraltet' in response.text:
                self.login()
                continue
            return response

    def scrape(self, low=0, high=10000):
        self.toc_offsets = self.get_base_toc()
        for part in range(1, self.part_count + 1):
            print(part)
            self.get_main_toc(part)
            self.get_all_year_tocs(part, low, high)
            yield from self.get_all_tocs(part, low, high)

    def get_json(self, url):
        response = self.get(url)
        response.encoding = 'utf-8'
        return json.loads(response.text)

    def get_base_toc(self):
        url = self.BASE_URL + self.BASE_TOC
        doc = self.get_json(url)
        items = doc['items'][0]['c']
        toc_offsets = []
        for item in items:
            if 'Bundesgesetzblatt Teil' not in item['l']:
                continue
            toc_offsets.append(item['id'])
        return toc_offsets

    def get_main_toc(self, part=1):
        self.get_main_toc_part(part)

    def get_main_toc_part(self, part):
        offset = self.toc_offsets[part - 1]
        url = self.BASE_URL + self.AJAX.format(offset)
        doc = self.get_json(url)
        items = doc['items'][0]['c']
        for item in items:
            try:
                year = int(item['l'])
            except ValueError:
                continue
            doc_id = item['id']
            if doc_id is not None:
                self.year_toc[part][year] = doc_id

    def get_all_year_tocs(self, part=1, low=0, high=10000):
        for year in self.year_toc[part]:
            if not (low <= year <= high):
                continue
            print("Getting Year TOC %d for %d" % (year, part))
            self.get_year_toc(part, year)

    def get_year_toc(self, part, year):
        year_doc_id = self.year_toc[part][year]
        url = self.BASE_URL + self.AJAX.format(year_doc_id)
        doc = self.get_json(url)
        items = doc['items'][0]['c']
        for item in items:
            match = re.search('Nr\. (\d+) vom (\d{2}\.\d{2}\.\d{4})',
                              item['l'])
            if match is None:
                continue
            print(item['l'])
            number = int(match.group(1))
            date = match.group(2)
            doc_id = item['id']
            self.year_docs[part].setdefault(year, {})
            self.year_docs[part][year][number] = {
                'date': date,
                'doc_id': doc_id
            }

    def get_all_tocs(self, part=1, low=0, high=10000):
        for year in self.year_docs[part]:
            if not (low <= year <= high):
                continue
            for number in self.year_docs[part][year]:
                yield from self.get_toc(part, year, number)

    def get_toc(self, part, year, number):
        year_doc = self.year_docs[part][year][number]
        doc_id = year_doc['doc_id']
        url = self.BASE_URL + self.TEXT.format(docid=doc_id)
        doc = self.get_json(url)
        root = lxml.html.fromstring(doc['innerhtml'])
        order_num = 1
        for tr in root.cssselect('tr'):
            td = tr.cssselect('td')[1]
            divs = td.cssselect('div')
            law_date = None
            if not len(divs):
                continue
            if len(divs) == 2:
                divs = [None] + divs
            else:
                law_date = divs[0].text_content().strip()
            link = divs[1].cssselect('a')[0]
            name = link.text_content().strip()
            href = link.attrib['href']
            text = divs[2].text_content().strip()
            print(text)
            match = re.search('aus +Nr. +(\d+) +vom +(\d{1,2}\.\d{1,2}\.\d{4}),'
                              ' +Seite *(\d*)\w?\.?$', text)
            page = None
            date = match.group(2)
            if match.group(3):
                page = int(match.group(3))
            kind = 'entry'
            if name in ('Komplette Ausgabe', 'Inhaltsverzeichnis'):
                # FIXME: there are sometimes more meta rows
                kind = 'meta'
            yield {
                'row_id': '{}_{}_{}_{}'.format(part, year, number, order_num),
                'part': part, 'order': order_num,
                'year': year, 'toc_doc_id': doc_id,
                'number': number, 'date': date,
                'law_date': law_date, 'kind': kind,
                'name': name, 'href': href, 'page': page
            }
            order_num += 1


if __name__ == '__main__':
    db = dataset.connect('sqlite:///data.sqlite')
    table = db['data']
    bgbl = BGBLScraper()
    for item in bgbl.scrape(1949, datetime.datetime.now().year):
        table.upsert(item, ['row_id'])
