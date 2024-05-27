import os
import io
import zipfile
import requests
import logging
from pathlib import Path
from datetime import datetime, timedelta


from typing import List, Union, Optional

import ftfy
import pandas as pd
import validators
import httplib2
import asyncio
import aiohttp
import json
from urllib.parse import urljoin, urlparse
import feedparser
import pytz
import re

from secutils.utils import (
    _to_quarter, ValidateFields,
    _remove_bad_bytes, _check_cache_dir
)

logger = logging.getLogger(__name__)


async def download_docs(output_dir: Path,loop, num_workers, cache_dir: Optional[str]=None) -> None:
    import asyncio
    sec_container = SECContainer()
    jobs = []
    while sec_container.to_visit:
        sec_file = sec_container.to_visit.pop()
        form_dir = build_dir_structure(output_dir, sec_file)
        jobs.append([sec_file, form_dir])

    logger.info(f"Downloading {len(jobs)} files")
    logger.info(f"Number of workers: {num_workers}")
    sem = asyncio.Semaphore(num_workers)
    async with aiohttp.ClientSession(loop=loop) as session:
        await asyncio.gather(*[job[0].download_file(sem, session, job[1], cache_dir) for job in jobs])



class SECContainer(object):

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(SECContainer, cls).__new__(cls)
        return cls.instance

class FileUtils(object):

    base_url = 'https://www.sec.gov/Archives/'

    def get_response(self, download_url: str) -> Union[str, str]:
        http = httplib2.Http()
        headers = {
                'User-Agent': 'sec-utils'
            }
        try:
            status, response = http.request(download_url, headers=headers)
        except Exception:
            raise RuntimeError(f'Unable to parse download url: {download_url}')

        try:
            response = response.decode('utf-8')
        except UnicodeDecodeError:
            response = ftfy.fix_text(response).decode('utf-8')
        return status, response

    def parse_url_to_parts(self, path: str) -> Union[str, str]:
        fname = path.split('/')[-1]
        # construct full url
        file_download_url = urljoin(self.base_url, path)
        assert validators.url(file_download_url), f"Invalid url: {file_download_url}"
        return fname, file_download_url


class File(FileUtils, ValidateFields):

    def __init__(self, form_type: str, company_name: str, cik_number: str,
                date_filed: str, partial_url: str=None) -> None:

        # validate fields
        cik_number, company_name, form_type, date_filed, partial_url = self.validate_index_line(
            cik=cik_number,
            company_name=company_name,
            form_type=form_type,
            date_filed=date_filed,
            partial_url=partial_url
        )
        self.form_type = form_type
        self.company_name = company_name
        self.cik_number = cik_number
        self.date_filed = date_filed
        self.year = self.date_filed.year
        self.quarter = _to_quarter(self.date_filed.month)
        self.file_name, self.file_download_url = self.parse_url_to_parts(partial_url)

    def to_row(self):
        return pd.DataFrame({
            'Form Type': self.form_type,
            'Company Name': self.company_name,
            'CIK': self.cik_number,
            'Date Filed': self.date_filed,
            'Year': self.year,
            'Quarter': self.quarter,
            'File Name': self.file_name,
            'Download URL': self.file_download_url,
            'Download File Path': getattr(self, 'download_file_dir', None)
        }, index=[0])

    async def download_file(self, sem, session, output_dir, cache_dir) -> str:
        rate = 1 # time unit on which the rate limiting is applied
        file_path = os.path.join(output_dir, self.file_name)
        headers = {
            'User-Agent': 'sec-utils'
        }
        logger.info(f"Downloading file: {self.file_name} from {self.file_download_url} to {file_path}")
        async with sem, session.get(self.file_download_url, headers=headers) as response:
            assert response.status == 200
            with open(file_path, "wb") as out:
                async for chunk in response.content.iter_chunked(4096):
                    out.write(chunk)
            if cache_dir is not None:
                await self.write_log_record(cache_dir)
            await asyncio.sleep(rate)

    async def write_log_record(self, cache_dir: str):
        parts = [self.cik_number, self.company_name, self.form_type, self.file_name, self.year, self.quarter,
                 self.date_filed, self.file_download_url]
        parts = list(map(str, parts))
        line = '|'.join(parts)
        with open(os.path.join(cache_dir, 'success.txt'), 'a') as outfile:
            outfile.write(line + '\n')
            outfile.close()

class FormIDX(object):
    """
    FormIDX is a utility class to capture master.idx zip files and construct a parsable data structure.
    Each file user specifies to download is converted into a File class, where file downloads can occur.

    Parameters
    -------
    year: year of master.idx to download and parse
    quarter:  quarter of master.idx to download and parse
    seen_files: list of files already processed
    cache_dir: directory to cache master.idx files
    form_types: list of form types to download

    See Also:
    -------
    FileUtils
    ValidateFields

    Example:
    --------
    >>> from secutils.edgar import FormIDX
    >>> form = FormIDX(year=2017, quarter=1, seen_files=None, cache_dir=None, form_types=['10-K])
    >>> files = form.index_to_files()
    >>> form.master_index.head()
    >>> # CIK	Company Name	Form Type	Date Filed	Filename	fname
    >>> # 1000015	META GROUP INC	10-K	1998-03-31	edgar/data/1000015/0001000015-98-000009.txt	0001000015-98-000009.txt
    >>> # 1000112	CHEVY CHASE MASTER CREDIT CARD TRUST II	10-K	1998-03-27	edgar/data/1000112/0000920628-98-000038.txt	0000920628-98-000038.txt
    >>> # 1000179	PARAMOUNT FINANCIAL CORP	10-K	1998-03-30	edgar/data/1000179/0000950120-98-000108.txt	0000950120-98-000108.txt
    """
    full_index_url = 'https://www.sec.gov/Archives/edgar/full-index/{year}/QTR{quarter}/master.zip'

    def __init__(self, year: int, quarter: int, seen_files: Optional[List[str]] = None,
                cache_dir: Optional[str]=None, form_types: Optional[List[str]]=None,
                ciks: Optional[int]=None):
        self.year = year
        self.quarter = quarter
        self.download_url = self.full_index_url.format(year=year, quarter=quarter)
        self.seen_files = seen_files
        self.cache_dir = _check_cache_dir(cache_dir)
        self.ciks = ciks
        self.form_name = f"formidx-{self.year}-{self.quarter}.csv"
        self.form_types = form_types
        self.master_index = self._get_master_zip_index()

    def _get_master_zip_index(self) -> List[List[str]]:
        """download zip index files from Edgar db"""
        if self.cache_dir:
            cache_file = os.path.join(self.cache_dir, self.form_name)
        if self.cache_dir and os.path.exists(cache_file):
            master_index = pd.read_csv(cache_file)
        else:
            headers = {
                'User-Agent': 'sec-utils'
            }
            response = requests.get(self.download_url, headers=headers)
            status_code = response.status_code
            if status_code == 200:
                edgarzipfile = zipfile.ZipFile(io.BytesIO(response.content))
                edgarfile = edgarzipfile.open('master.idx').read()
                try:
                    edgarfile = edgarfile.decode('utf-8')
                    edgarfile = ftfy.fix_text(edgarfile)
                    lines = edgarfile.split('\n')
                except UnicodeDecodeError:
                    lines = edgarfile.split(b'\n')
                    lines = _remove_bad_bytes(lines)
                master_index = self._parse_index_lines(lines)
                if self.cache_dir:
                    master_index.to_csv(cache_file, index=False)
            else:
                logger.error(f"URL returned error ({status_code}): {self.year} - {self.quarter} - {self.download_url}")
                return None
        og_shape = master_index.shape[0]
        master_index = self._filter_form_type(master_index)
        master_index = self._filter_ciks(master_index)
        master_index = self._filter_seen_files(master_index)
        num_remaining_download = master_index.shape[0]
        msg = f"master index ({self.year}) - ({self.quarter}) - original shape: {og_shape} - remaining download: {num_remaining_download}"
        logger.info(msg)
        return master_index

    def _parse_index_lines(self, lines: List[str]) -> pd.DataFrame:
        split_line = lambda x: x.replace('\n', '').replace('\r', '').replace('\t', '').split('|')
        master_index = pd.DataFrame([split_line(line) for line in lines if line.count('|')==4])
        columns = ['CIK', 'Company Name', 'Form Type', 'Date Filed', 'Filename']
        master_index.columns = columns
        master_index = master_index.iloc[1:]
        master_index['fname'] = master_index['Filename'].apply(lambda x: x.split('/')[-1])
        return master_index

    def _filter_form_type(self, master_index: pd.DataFrame) -> pd.DataFrame:
        """
        Filter FormIDX to specific form types. For example, if FormIDX(form_types=['S-1', 'S-1/A'], year=2018, quarter=4),
        all S-1 and S-1/A forms from 2018, Q4 will be retrieved

        Args:
            master_index: input pd.DataFrame containing all FormIDX
        """
        master_index['Form Type'] = master_index['Form Type'].apply(lambda x: x.strip())
        if self.form_types:
            unique_forms = master_index['Form Type'].unique().tolist()
            form_not_found = [form for form in self.form_types if form not in unique_forms]
            if len(form_not_found) > 0:
                msg = f"specified form type not found in master.idx ({self.year}) - ({self.quarter}) - form not found: {form_not_found}"
                logger.warning(msg)
            master_index = master_index.loc[master_index['Form Type'].isin(self.form_types)]
        return master_index

    def _filter_ciks(self, master_index: pd.DataFrame) -> pd.DataFrame:
        if self.ciks:
            self.ciks = [self.validate_cik(cik) for cik in self.ciks]
            master_index = master_index.loc[master_index['CIK'].isin(self.ciks)]
            msg = f"Found {master_index.shape[0]} files for CIK list"
            logger.info(msg)
        return master_index

    def _filter_seen_files(self, master_index: pd.DataFrame) -> pd.DataFrame:
        og_shape = master_index.shape[0]
        if self.seen_files:
            master_index = master_index.loc[~master_index['fname'].isin(self.seen_files)]
            num_prior_download = og_shape-master_index.shape[0]
            msg = f"master index ({self.year}) - ({self.quarter}) - original shape: {og_shape} - num prior download: {num_prior_download}"
            logger.info(msg)
        return master_index

    def index_to_files(self) -> List[File]:
        files = []
        if isinstance(self.master_index, pd.DataFrame):
            for line in self.master_index.values:
                cik = line[0]
                company_name = line[1]
                form_type = line[2]
                date_filed = line[3]
                partial_url = line[4]

                files.append(File(
                    form_type=form_type,
                    company_name=company_name,
                    cik_number=cik,
                    date_filed=date_filed,
                    partial_url=partial_url,
                ))
        return files

def build_dir_structure(output_dir: str, sec_file: File) -> str:
    # build output dir
    # modify form type due to forms like S-1/A
    form_type = sec_file.form_type.replace('/', '')
    output_dir = os.path.join(output_dir, form_type, str(sec_file.year), sec_file.quarter)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    return output_dir

class FormIDX_search(FormIDX):
    """
    FormIDX_search is a utility class to allow access to the search function on SEC's EDGAR database.
    The search function allows users to search for specific forms by company name, CIK, form type, and date filed.
    """

    base_search_url = 'https://efts.sec.gov/LATEST/search-index'

    def __init__(self, form_type: str=None, start_date : datetime=None, end_date : datetime=None, search_term: str=None) -> None:
        self.form_type = form_type
        self.start_date = start_date
        self.end_date = end_date
        self.search_term = search_term
        self.query_results = self._query_sec()

    def _query_sec(self) -> dict:
        """
        Query the SEC's EDGAR database using the search function and return a json object of the results.
        """

        # this is the max results that the search website returns
        # that is why we have to make our requests smaller if we get that aomutn of results
        max_hits = 10000
        page_size = 100
        num_pages = 0
        start_date = self.start_date
        # this is a trick to jump into the while loop
        end_date = self.end_date
        results = {}
        time_step = timedelta(days=0)
        header = {
                'User-Agent': 'sec-utils'
            }

        params = {
            'q': self.search_term,
            'category' : 'custom',
            'startdt' : start_date.strftime('%Y-%m-%d'),
            'enddt' : end_date.strftime('%Y-%m-%d'),
            'filter_forms': self.form_type
        }

        # find the time range that doesn't overwhelm the search results and delivers results less 10000
        while start_date < end_date:
            response = requests.get(self.base_search_url, headers=header, params=params)
            logger.info(f'inside outer while loop : {response.url}')

            # print response url
            if response.status_code == 200:
                query_result = self._parse_search_results(response.json())
            else:
                # raise error and show status code and request header
                raise RuntimeError(f"Failed to query SEC database. Status code: {response.status_code} - Request header: {header}")

            total_hits = query_result['hits']['total']['value']
            logger.info(f"Found {total_hits} hits.")


            # too many results
            if total_hits >= max_hits:
                # calculate time step from end and start date and floor the days
                logger.info("Reducing time window...")
                time_step = timedelta(days= ((end_date - start_date) / 2).days)
                end_date = start_date + time_step
                logger.info(f"Current time step: {time_step}")
                logger.info(f"Trying out start_date : {start_date.strftime('%Y-%m-%d')}")
                logger.info(f"Trying out end_date   : {end_date.strftime('%Y-%m-%d')}")
                params['startdt'] = start_date.strftime('%Y-%m-%d')
                params['enddt'] = end_date.strftime('%Y-%m-%d')
            # we have a time window that appears to be small enough
            else:
                if 'hits' in results:
                    results['hits'].extend(query_result['hits']['hits'])
                else:
                    results['hits'] = query_result['hits']['hits']
                num_pages = total_hits // page_size + 1
                logger.info(f"Retrieving num_pages : {num_pages}")
                current_page = 2
                while current_page <= num_pages:
                    params['page'] = str(current_page)
                    params['from'] = str((current_page - 1) * page_size)
                    response = requests.get(self.base_search_url, headers=header, params=params)
                    logger.info(f'inside inner while loop : {response.url}' )
                    if response.status_code == 200:
                        query_result = self._parse_search_results(response.json())
                    else:
                        raise RuntimeError(f"Failed to query SEC database. Status code: {response.status_code} - Request header: {header}")
                    current_page += 1
                    # check if key hits exits
                    if 'hits' in results:
                        results['hits'].extend(query_result['hits']['hits'])
                    else:
                        results['hits'] = query_result['hits']['hits']
                current_page = 1
                start_date = end_date + timedelta(days=1)
                end_date = min(start_date + time_step, self.end_date)
                params['startdt'] = start_date.strftime('%Y-%m-%d')
                params['enddt'] = end_date.strftime('%Y-%m-%d')
                params.pop('page', None)
                params.pop('from', None)
                logger.info(f"new start date: {params['startdt']}")
                logger.info(f"new end date: {params['enddt']}")
                # logger.info(results)
        return results

    def _parse_search_results(self, response: dict) -> dict:
        """
        Parse the json response from the SEC search query and create a list of File objects.
        """
        results = {}
        results['hits'] = response['hits']
        return results

    def index_to_files(self) -> List[File]:
        files = []
        hits = self.query_results['hits']

        # write dictionary to json file
        # with open('/Users/angus/git/external_tools/sec-utils/secutils/logfile.json', 'w') as logfile:
        #     json.dump(hits, logfile, indent = 4)

        logger.info(f"Creating index for {len(hits)} files.")
        for element in hits:
            # import pprint
            # from pprint import pprint
            # pprint(element)

            cik = int(element['_source']['ciks'][0])
            company_name = element['_source']['display_names'][0]
            form_type = element['_source']['form']
            date_filed = element['_source']['file_date']
            adsh = element['_source']['adsh']
            partial_url = f"edgar/data/{cik}/{adsh}.txt"

            files.append(File(
                    form_type=form_type,
                    company_name=company_name,
                    cik_number=cik,
                    date_filed=date_filed,
                    partial_url=partial_url,
                ))
        return files

class RSSFormIDX(object):
    """
    RSSFormIDX is a utility class to capture RSS feeds from SEC's EDGAR database and construct a parsable data structure.
    """

    base_rss_url = 'https://www.sec.gov/cgi-bin/browse-edgar?action=getcurrent&CIK={cik}&type={form_type}&company=&dateb=&owner=include&start={start}&count={count}&output=atom'


    def __init__(
            self,
            form_type: Optional[str] = '',
            company: Optional[str] = '',
            cik: Optional[int] = '',
            search_term: Optional[str] = '',
            cache_dir: Optional[str] = None,
            seen_files: Optional[List[str]] = None
    ) -> None:

        self.rss_date_format = '%Y-%m-%dT%H:%M:%S%z'
        self.cache_dir = _check_cache_dir(cache_dir)
        self.seen_files = seen_files
        self.form_type = form_type
        self.company = company
        self.cik = cik
        self.last_modified = None
        self._get_last_modified_state()
        self.search_term = search_term
        self.start = 0
        self.count = 100
        self.already_downloaded_files = []
        self.already_downloaded = False
        self.query_results = self._query_sec()

    def reset_page(self):
        self.start = 0

    def get_next_page(self):
        self.start += self.count
        self.query_results = self._query_sec()

    def _get_already_downloaded(self): # pragma: no cover
        # gets the downloaded files from the success.txt file
        if self.cache_dir:
            cache_file = os.path.join(self.cache_dir, 'success.txt')
        if self.cache_dir and os.path.exists(cache_file):
            with open(cache_file, 'r') as file_id:
                # read | separated lines into list
                temp = file_id.readlines()
            for line in temp:
                elements = line.split('|')
                cik = int(elements[0])
                adsh = elements[3].replace('.txt', '')
                self.already_downloaded_files.append(
                    File(
                        form_type=elements[2],
                        company_name=elements[1],
                        cik_number=cik,
                        date_filed=elements[6],
                        partial_url=f"edgar/data/{cik}/{adsh}"
                    )
                )

    def _convert_to_berlin_tz(self, date: str) -> str:
        berlin_tz = pytz.timezone('Europe/Berlin')
        original_date = datetime.strptime(date, self.rss_date_format)
        berlin_date = original_date.astimezone(berlin_tz)
        logger.info(f"Berlin time: {berlin_date}")
        return berlin_date.strftime(self.rss_date_format)

    def _convert_to_utc_minus_4_tz(self, date: str) -> str:
        utc_minus_4_tz = pytz.timezone('Etc/GMT+4')
        original_date = datetime.strptime(date, self.rss_date_format)
        utc_minus_4_date = original_date.astimezone(utc_minus_4_tz)
        logger.info(f"UTC-4 time: {self.last_modified}")
        return utc_minus_4_date.strftime(self.rss_date_format)

    def _get_last_modified_state(self):
        if os.path.exists('last_modified.txt'):
            with open('last_modified.txt', 'r') as file_id:
                self.last_modified = self._convert_to_utc_minus_4_tz(file_id.read())

        else:
            self.last_modified = None

    def _update_last_modified_state(self, last_modified: str):
        with open('last_modified.txt', 'w') as file:
            file.write(self._convert_to_berlin_tz(last_modified))

    def _query_sec(self) -> dict:
        """
        Query the SEC's EDGAR database using the RSS feed and return a json object of the results.
        """

        current_url = self.base_rss_url.format(cik=self.cik, form_type=self.form_type, start=self.start, count=self.count)
        logger.info(f"Querying SEC database with RSS feed: {current_url}")
        if self.last_modified is not None:
            logger.info(f"Using last modified date: {self.last_modified} , Berlin time. {self._convert_to_berlin_tz(self.last_modified)}")

            response = feedparser.parse(current_url, modified=self.last_modified, agent = "MyApp/1.0 +http://example.com/")
        else:
            logger.info(f"No last modified date found.")
            response = feedparser.parse(current_url, agent = "MyApp/1.0 +http://example.com/")
        # response = feedparser.parse(current_url, agent = "MyApp/1.0 +http://example.com/")

        if response.status == 200:
            if response.bozo == False:
                logger.info(f"Query successful. Found {len(response.entries)} entries.")
            else:
                raise RuntimeError(f"Failed to parse RSS feed. Error: {response.bozo_exception}")

        else:
            raise RuntimeError(f"Failed to query SEC database. Status code: {response.status} - Request header: {current_url}")

        # logger.info(response)

        self._update_last_modified_state(response.feed.modified)
        logger.info(100*"-")

        logger.info(f"search term: {self.search_term}")
        if self.search_term != '':
            logger.info(f"Searching for search term: {self.search_term}")

            filtered_list = [entry for entry in response.entries if self.search_term in entry.summary.lower()]
            for element in filtered_list:
                logger.info(f"Title: {element.title}")
                logger.debug(f"Summary: {element.summary}")
                logger.info(f"Link: {element.link}")
                logger.debug(100*"-")
        else:
            filtered_list = response.entries
        logger.info(100*"-")
        return filtered_list

    def _in_seen_files(self, adsh: str) -> bool:
        #  is the adsh already downloaded
        if self.seen_files:
            potential_file_name = f"{adsh}.txt"
            if potential_file_name in self.seen_files:
                return True
            else:
                return False
        else:
            return False

    def index_to_files(self) -> List[File]:
        files = []
        hits = self.query_results

        logger.info(f"Creating file list for {len(hits)} files.")
        for element in hits:
            # https://www.sec.gov/Archives/edgar/data/1067701/000110465924065194/0001104659-24-065194-index.htm
            path_parts = urlparse(element.link).path.split('/')
            # "8-K - MULLEN AUTOMOTIVE INC. (0001499961) (Filer)"
            pattern = r'^(.*?) - (.*?) \(\d+\) \(Filer\)$'
            matched = re.match(pattern, element.title)

            cik = int(path_parts[4])
            if matched :
                form_type = matched.group(1)
                company_name = matched.group(2)
            else:
                raise RuntimeError(f"Failed to parse title: {element.title}")

            # 2024-05-24T17:28:17-04:00 but we want 2024-05-24 and timezone is ignored
            date_filed = datetime.strptime(element.updated, self.rss_date_format).strftime('%Y-%m-%d')

            adsh = path_parts[6].replace('-index.htm', '')
            partial_url = f"edgar/data/{cik}/{adsh}.txt"

            self.already_downloaded = self._in_seen_files(adsh)
            if not self.already_downloaded:
                files.append(File(
                        form_type=form_type,
                        company_name=company_name,
                        cik_number=cik,
                        date_filed=date_filed,
                        partial_url=partial_url,
                    ))
            else:
                logger.info(f"File already downloaded: {adsh}.txt")
                return files

        return files
