import argparse
from collections import defaultdict
import datetime
import glob
import json
import os
import re
import shutil
import subprocess

import dataset
import lxml.html
import requests
try:
    import PyPDF2
except ImportError:
    PyPDF2 = None


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
        '%2F%2F*%5B%40node_id%3D%27{node_id}%27%5D&skin=pdf'
    )
    PDF_META = (
        'text.xav?SID=&start=%2F%2F*[%40node_id%3D%27{fragment}%27]&'
        'skin=&tf=xaver.component.Text_0&hlf=xaver.component.Hitlist_0'
    )

    PDF_REDIRECT = (
        'media.xav/bgbl{part}_{year}_{num}.pdf'
        '?SID=&iid={docid}&_csrf={token}'
    )
    PATH_TEMPLATE = 'bgbl{part}/{year}/bgbl{part}_{year}_{number}.pdf'

    year_toc = defaultdict(dict)
    year_docs = defaultdict(dict)
    toc = {}

    def __init__(self, years=None, document_path=None, parts=(1, 2),
                 numbers=None):
        self.document_path = document_path
        if years is None:
            years = range(1949, datetime.datetime.now().year + 1)
        self.years = list(years)
        self.parts = list(parts)
        self.numbers = None
        if numbers is not None:
            self.numbers = list(numbers)
        self.login()

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

    def get_download_filename(self, part, year, number):
        path_part = self.PATH_TEMPLATE.format(
            part=part, year=year, number=number
        )
        path = os.path.join(self.document_path, path_part)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path

    def should_download(self, part, year, number):
        if self.document_path is None:
            return False
        path = self.get_download_filename(part, year, number)
        return not os.path.exists(path)

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
            if year not in self.years:
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
            if self.numbers is not None and number not in self.numbers:
                continue
            yield from self.get_toc(part, year, number, item)

    def get_toc(self, part, year, number, item):
        url = self.AJAX.format(docid=item['id'])
        doc = self.get_json(url)
        full_edition = [x for x in doc['items'][0]['c']
                        if 'Komplette Ausgabe' in x['l']]
        if not full_edition:
            url = self.TEXT.format(did=item['did'])
        else:
            full_edition = full_edition[0]
            url = self.TEXT.format(
                did=full_edition['did'],
                docid=full_edition['id']
            )
        doc = self.get_json(url)
        doc_url = None
        if self.should_download(part, year, number):
            doc_url = self.download_document(
                part, year, number, full_edition['did']
            )

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

    def download_document(self, part, year, number, node_id):
        pdf_viewer_url = (
            self.BASE_URL +
            self.PDF_VIEWER.format(node_id=node_id)
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
                path = self.get_download_filename(part, year, number)
                with open(path, 'wb') as f:
                    for chunk in response:
                        f.write(chunk)
                self.unlock_pdf(path)
                print(response.url)
            return response.url
        else:
            print('Could not download', response.status_code,
                  part, year, number, doc)

    def unlock_pdfs(self):
        for part in self.parts:
            for year in self.years:
                dummy = self.get_download_filename(part, year, 1)
                dirname = os.path.dirname(dummy)
                for path in glob.glob(os.path.join(dirname, '*.pdf')):
                    if path.endswith('_original.pdf'):
                        continue
                    if self.pdf_is_encrypted(path):
                        print('Unlocking', path)
                        result = self.unlock_pdf(path)
                        if not result:
                            print('Could not unlock. Exit.')
                            return

    def pdf_is_encrypted(self, path):
        if PyPDF2 is None:
            return None
        with open(path, 'rb') as f:
            return PyPDF2.PdfFileReader(f).isEncrypted

    def unlock_pdf(self, path):
        original_path = path.replace('.pdf', '_original.pdf')
        shutil.move(path, original_path)
        result = subprocess.run(
            [
                'gs', '-q', '-dNOPAUSE', '-dBATCH',
                '-sDEVICE=pdfwrite',
                '-sOutputFile=%s' % path,
                '-c', '.setpdfwrite',
                '-f', original_path
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        if result.returncode != 0:
            print(result.stdout)
            print(result.stderr)
            return False
        return True


def unlock(year=None, document_path=None):
    bgbl = BGBLScraper(
        min_year=int(year) if year is not None else 1949,
        max_year=datetime.datetime.now().year,
        document_path=document_path,
    )
    bgbl.unlock_pdfs()


def create_range_argument(arg):
    if arg is None:
        return None
    arg = str(arg)
    parts = arg.split(',')
    for part in parts:
        part = part.strip()
        if '-' in part:
            start_stop = part.split('-')
            yield from range(int(start_stop[0]), int(start_stop[1]) + 1)
        else:
            yield int(part)


def main(document_path=None, years=None, parts=None, numbers=None):
    bgbl = BGBLScraper(
        years=create_range_argument(years),
        parts=create_range_argument(parts),
        numbers=create_range_argument(numbers),
        document_path=document_path,
    )
    print('Scraping parts {} and years {}'.format(bgbl.parts, bgbl.years))
    db = dataset.connect('sqlite:///data.sqlite')
    table = db['data']

    for item in bgbl.scrape():
        table.upsert(item, ['row_id'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Scrape BGBl.')
    parser.add_argument('document_path', default=None, nargs='?',
                        help='base path to document directory')
    parser.add_argument('--years', dest='years', action='store',
                        default=str(datetime.datetime.now().year),
                        help='Scrape these years, default latest year. '
                             'Range and comma-separated allowed.')
    parser.add_argument('--numbers', dest='numbers', action='store',
                        default=None,
                        help='Scrape these numbers, default all.')
    parser.add_argument('--parts', dest='parts', action='store',
                        default='1,2',
                        help='Scrape parts, default all parts. '
                             'Range and comma-separated allowed.')
    args = parser.parse_args()
    main(**vars(args))
