import datetime
import re
import os
import json
from collections import defaultdict
import sys

import dataset
import lxml.html
import requests


class BGBLScraper(object):
    BASE_URL = 'https://www.bgbl.de/xaver/bgbl/'
    START = 'start.xav?startbk=Bundesanzeiger_BGBl'
    BASE_TOC = 'ajax.xav?q=toclevel&n=0'
    AJAX = 'ajax.xav?q=toclevel&bk=bgbl&n={docid}'
    PDF = 'media/{}'
    TEXT = ('text.xav?SID=&tf=xaver.component.Text_0&tocf='
            '&qmf=&hlf=xaver.component.Hitlist_0'
            '&bk=bgbl&start=%2F%2F*%5B%40node_id%3D%27{did}%27%5D'
            '&tocid={docid}')
    PDF_VIEWER = (
        'text.xav?SID=&tf=xaver.component.Text_0&tocf=&qmf=&'
        'hlf=xaver.component.Hitlist_0&bk=bgbl&start='
        '%2F%2F*%5B%40node_id%3D%27{fragment}%27%5D&skin=pdf'
    )
    PDF_META = (
        'text.xav?SID=&start=%2F%2F*[%40node_id%3D%27{fragment}%27]&'
        'skin=&tf=xaver.component.Text_0&hlf=xaver.component.Hitlist_0'
    )

    PDF_REDIRECT = (
        'media.xav/bgbl{part}_{year}_{num}.pdf'
        '?SID=&iid={docid}&_csrf={token}'
    )

    year_toc = defaultdict(dict)
    year_docs = defaultdict(dict)
    toc = {}

    def __init__(self, min_year=1949, max_year=10000,
                 document_path=None, parts=(1, 2)):
        self.document_path = document_path
        self.login()
        self.max_year = max_year
        self.min_year = min_year
        self.parts = parts

    def login(self):
        self.session = requests.Session()
        self.session.get(self.BASE_URL + self.START)

    def get(self, url, **kwargs):
        while True:
            response = self.session.get(url, **kwargs)
            if not kwargs and 'Session veraltet' in response.text:
                print('Session expired...')
                self.login()
                continue
            return response

    def get_download_dir(self, part, year, number):
        path = os.path.join(self.document_path, str(part), str(year))
        os.makedirs(path, exist_ok=True)
        return path

    def get_download_path(self, part, year, number):
        path = self.get_download_dir(part, year, number)
        path = os.path.join(path, '%s.pdf' % number)
        return path

    def document_exists(self, part, year, number):
        path = self.get_download_path(part, year, number)
        return os.path.exists(path)

    def scrape(self):
        self.toc_offsets = self.get_base_toc()
        for part in self.parts:
            print(part)
            yield from self.get_main_toc_part(part)

    def get_json(self, url):
        response = self.get(self.BASE_URL + url)
        response.encoding = 'utf-8'
        return json.loads(response.text)

    def get_base_toc(self):
        doc = self.get_json(self.BASE_TOC)
        items = doc['items'][0]['c']
        toc_offsets = []
        for item in items:
            if 'Bundesgesetzblatt Teil' not in item['l']:
                continue
            toc_offsets.append(item['id'])
        return toc_offsets

    def get_main_toc_part(self, part):
        offset = self.toc_offsets[part - 1]
        url = self.AJAX.format(docid=offset)
        doc = self.get_json(url)
        items = doc['items'][0]['c']
        for item in items:
            try:
                year = int(item['l'])
            except ValueError:
                continue
            if not (self.min_year <= year <= self.max_year):
                continue
            self.login()
            yield from self.get_year_toc(part, year, item)

    def get_year_toc(self, part, year, doc_item):
        print("Getting Year %d for %d" % (year, part))
        year_doc_id = doc_item['id']
        url = self.AJAX.format(docid=year_doc_id)
        doc = self.get_json(url)
        items = doc['items'][0]['c']
        for item in items:
            match = re.search('Nr\. (\d+) vom (\d{2}\.\d{2}\.\d{4})',
                              item['l'])
            if match is None:
                continue
            number = int(match.group(1))
            yield from self.get_toc(part, year, number, item)

    def get_toc(self, part, year, number, item):
        # self.login()
        # import ipdb; ipdb.set_trace()

        url = self.AJAX.format(docid=item['id'])
        doc = self.get_json(url)
        url = self.TEXT.format(did=item['did'], docid=item['id'])
        doc = self.get_json(url)
        doc_url = None
        if not self.document_exists(part, year, number):
            doc_url = self.download_document(part, year, number, doc)

        root = lxml.html.fromstring(doc['innerhtml'])
        order_num = 1
        for tr in root.xpath('//table[1]//tr'):
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
            match = re.search(
                r'aus +Nr. +(\d+) +vom +(\d{1,2}\.\d{1,2}\.\d{4}),'
                r' +Seite *(\d*)\w?\.?$',
                text
            )
            page = None
            date = match.group(2)
            if match.group(3):
                page = int(match.group(3))
            kind = 'entry'
            if (name in ('Komplette Ausgabe', 'Inhaltsverzeichnis') or
                    name.startswith('Hinweis: ')):
                kind = 'meta'
            yield {
                'row_id': '{}_{}_{}_{}'.format(part, year, number, order_num),
                'part': part, 'order': order_num,
                'year': year, 'toc_doc_id': item['id'], 'doc_did': item['did'],
                'number': number, 'date': date,
                'law_date': law_date, 'kind': kind,
                'name': name, 'href': href, 'page': page,
                'doc_url': doc_url
            }
            order_num += 1

    def download_document(self, part, year, number, doc):
        fragment = doc['nextfragment']
        pdf_viewer_url = (
            self.BASE_URL +
            self.PDF_VIEWER.format(fragment=fragment)
        )
        # Set session state to retrieve URL
        response = self.get(pdf_viewer_url)
        match = re.search(r'iid=(\d+)', response.text)
        docid = match.group(1)
        match = re.search(r'_csrf=(\w+)"', response.text)
        token = match.group(1)
        url = self.BASE_URL + self.PDF_REDIRECT.format(
            part=part, year=year, num=number, docid=docid,
            token=token
        )
        response = self.get(url, stream=True)
        if response.status_code == 200:
            if self.document_path:
                print('Download document', part, year, number)
                path = self.get_download_path(part, year, number)
                with open(path, 'wb') as f:
                    for chunk in response:
                        f.write(chunk)
                print(response.url)
            return response.url
        else:
            print('Could not download', response.status_code,
                  part, year, number, doc)


if __name__ == '__main__':
    db = dataset.connect('sqlite:///data.sqlite')
    table = db['data']
    documents = None
    if os.path.exists('documents'):
        documents = 'documents/'
    bgbl = BGBLScraper(
        min_year=int(sys.argv[1]) if len(sys.argv) > 1 else 1949,
        max_year=datetime.datetime.now().year,
        document_path=documents,
    )
    for item in bgbl.scrape():
        table.upsert(item, ['row_id'])
