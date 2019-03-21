import os
import re
import sqlite3
import string
from datetime import datetime
from typing import Optional, Any, Dict, List

import requests
from lxml import html


class BrisbaneCityCouncilPetitions:
    """
    Gather the details of the petitions on the Brisbane City Council website.
    """

    petition_list = 'https://epetitions.brisbane.qld.gov.au/'
    petition_item = 'https://epetitions.brisbane.qld.gov.au/petition/view/pid/{}'
    petition_sign = 'https://www.epetitions.brisbane.qld.gov.au/petition/sign/pid/{}'
    petition_signatures = 'https://www.epetitions.brisbane.qld.gov.au/petition/signatures/pid/{}'
    sqlite_db_file = 'data.sqlite'
    iso_datetime_format = '%Y-%m-%dT%H:%M:%S+10:00'
    regex_collapse_newline = re.compile(r'(\n|\r)+')
    regex_collapse_whitespace = re.compile(r'\s{2,}')
    regex_signatures = re.compile('signatures.*', re.DOTALL)

    allowed_chars = string.digits + string.ascii_letters + string.punctuation

    cache_chars = string.digits + string.ascii_letters
    local_cache_dir = 'cache'
    use_cache = True

    def run(self):
        current_time = datetime.today()

        db_conn = None
        try:
            db_conn = self.get_sqlite_db()
            self.create_sqlite_database(db_conn)

            print('Reading petition list')
            petition_list_page = self.download_html(self.petition_list)
            petition_items = self.parse_petition_list_page(petition_list_page)

            count_added = 0
            count_skipped = 0

            print('Reading petitions')
            for petition_item in petition_items:
                reference_id = petition_item['reference_id']
                url = self.petition_item.format(reference_id)
                petition_item_page = self.download_html(url)
                petition_detail = self.parse_petition_item_page(reference_id, url, current_time, petition_item_page)

                db_data = self.build_rows(petition_item, petition_detail)

                if not self.sqlite_petition_row_exists(db_conn, db_data['reference_id'], db_data['signatures']):
                    print('Adding {} - "{}"'.format(db_data['reference_id'], db_data['title']))
                    self.sqlite_petition_row_insert(db_conn, db_data)
                    count_added += 1
                else:
                    print('Already exists {} - "{}"'.format(db_data['reference_id'], db_data['title']))
                    count_skipped += 1

                db_conn.commit()

            print('Added {}, skipped {}, total {}'.format(count_added, count_skipped, count_added + count_skipped))
            print('Completed successfully.')

        finally:
            if db_conn:
                db_conn.close()

    def parse_petition_list_page(self, tree) -> List[Dict[str, Any]]:
        result = []

        if tree is None:
            return result

        rows = tree.xpath('//table[@class="petitions"]/tr')[1:]
        for row in rows:
            cells = row.xpath('td')
            reference_id = cells[0][0].get('href').split('/')[-1]
            item = {
                'reference_id': reference_id,
                'title': cells[0][0].text,
                'url': self.petition_item.format(reference_id),
                'principal': cells[1].text,
                'closed_at': datetime.strptime(cells[2].text, '%a, %d %b %Y'),
            }
            result.append(item)

        return result

    def parse_petition_item_page(self, reference_id, url, current_time, tree) -> Dict[str, Any]:
        title = tree.xpath('//div[@id="content"]/h1/text()')[0].strip()
        principal = tree.xpath('((//table[@class="petition-details"]//tr)[1]/td)[2]/text()')[0].strip()
        closed_at = tree.xpath('((//table[@class="petition-details"]//tr)[2]/td)[2]/text()')[0].strip()
        signatures = tree.xpath('((//table[@class="petition-details"]//tr)[3]/td)[2]')[0].text_content()
        body = tree.xpath('//div[@id="petition-details"]')[0].text_content()
        item = {
            'title': title,
            'principal': principal,
            'body': self.regex_collapse_whitespace.sub(' ', self.regex_collapse_newline.sub('\n', body)).strip(),
            'signatures': self.regex_signatures.sub('', signatures).strip(),
            'sign_uri': self.petition_sign.format(reference_id),
            'retrieved_at': current_time,
            'closed_at': datetime.strptime(closed_at, '%a, %d %b %Y'),
            'url': url,
            'reference_id': reference_id,
        }

        return item

    def build_rows(self, petition_item: Dict[str, Any], petition_detail: Dict[str, Any]) -> Dict[str, Any]:
        """Create a row to be inserted into sqlite db."""

        # petitioner_suburb = petition_detail['principal'].replace(petition_item['principal'], '').strip(' ,')

        for k, v in petition_item.items():
            if k in petition_detail and petition_detail[k] != v and k != 'principal':
                # raise 'List page info did not match details page info: {} --- {}'.format(petition_item, petition_detail)
                pass
            elif k == 'principal' and petition_item['principal'] not in petition_detail['principal']:
                # raise 'Principals were too different: {} --- {}'.format(petition_item['principal'], petition_detail['principal'])
                pass

        data = {
            'url': petition_detail['url'],
            'sign_uri': petition_detail['sign_uri'],
            'title': petition_detail['title'],
            'reference_id': petition_detail['reference_id'],
            'principal': petition_detail['principal'],
            'body': petition_detail['body'],
            'signatures': petition_detail['signatures'],
            'retrieved_at': petition_detail['retrieved_at'].strftime(self.iso_datetime_format),
            'closed_at': petition_detail['closed_at'].strftime(self.iso_datetime_format),
        }

        return data

    def normalise_string(self, value):
        if not value:
            return ''

        value = value.replace('’', "'")
        remove_newlines = value.replace('\n', ' ').replace('\r', ' ').strip()
        result = ''.join(c if c in self.allowed_chars else ' ' for c in remove_newlines).strip()
        return result

    # ---------- SQLite Database -------------------------

    def sqlite_petition_row_exists(self, db_conn, reference_id, signatures):
        c = db_conn.execute(
            'SELECT COUNT() FROM data WHERE reference_id = ? AND signatures = ?',
            (reference_id, signatures))

        row = list(c.fetchone())
        match_count = int(row[0])

        return match_count > 0

    def sqlite_petition_row_insert(self, db_conn, row: Dict[str, Any]) -> int:
        c = db_conn.execute(
            'INSERT INTO data '
            '(url, sign_uri, title, reference_id, principal, body, '
            'signatures, retrieved_at, closed_at) '
            'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)',
            (row['url'], row['sign_uri'], row['title'], row['reference_id'], row['principal'], row['body'],
             row['signatures'], row['retrieved_at'], row['closed_at'],))

        row_id = c.lastrowid

        return row_id

    def get_sqlite_db(self):
        conn = sqlite3.connect(self.sqlite_db_file)
        return conn

    def create_sqlite_database(self, db_conn):
        db_conn.execute(
            'CREATE TABLE '
            'IF NOT EXISTS '
            'data'
            '('
            'retrieved_at TEXT,'
            'url TEXT,'
            'sign_uri TEXT,'
            'reference_id TEXT,'
            'title TEXT,'
            'principal TEXT,'
            'closed_at TEXT NULL,'
            'body TEXT,'
            'signatures TEXT,'
            'UNIQUE (reference_id, signatures)'
            ')')

        db_conn.execute(
            'CREATE UNIQUE INDEX IF NOT EXISTS reference_id_signatures '
            'ON data (reference_id, signatures)')

    # ---------- Downloading -----------------------------

    def download_html(self, url: str):
        content = self.load_page(url)

        if not content:
            page = requests.get(url)
            if page.is_redirect or page.is_permanent_redirect or page.status_code != 200:
                content = None
            else:
                content = page.content
                self.save_page(url, content)

        if not content:
            return None

        tree = html.fromstring(content)
        return tree

    # ---------- Local Cache -----------------------------

    def cache_item_id(self, url):
        item_id = ''.join(c if c in self.cache_chars else '' for c in url).strip()
        return item_id

    def save_page(self, url, content) -> None:
        if not self.use_cache:
            return

        os.makedirs(self.local_cache_dir, exist_ok=True)
        item_id = self.cache_item_id(url)
        file_path = os.path.join(self.local_cache_dir, item_id + '.txt')

        with open(file_path, 'wb') as f:
            f.write(content)

    def load_page(self, url) -> Optional[bytes]:
        if not self.use_cache:
            return None

        os.makedirs(self.local_cache_dir, exist_ok=True)
        item_id = self.cache_item_id(url)
        file_path = os.path.join(self.local_cache_dir, item_id + '.txt')

        if not os.path.isfile(file_path):
            return None

        with open(file_path, 'rb') as f:
            return f.read()


petitions = BrisbaneCityCouncilPetitions()
petitions.run()
