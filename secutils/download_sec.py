import os
import logging
import httplib2
import argparse
from pathlib import Path
from itertools import product
from datetime import datetime
from dateutil import parser as dateutil_parser
from typing import List

from tqdm.auto import tqdm

from secutils.edgar import FormIDX, FormIDX_search, RSSFormIDX, SECContainer, download_docs
from secutils.utils import scan_output_dir, _read_cik_config, yaml_config_to_args
import asyncio


logger = logging.getLogger(__name__)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output_dir', type=Path, default=None, help='default download path')
    parser.add_argument('--form_types', nargs='+', default=None, help='form types to download')
    parser.add_argument('--num_workers', default=-1, type=int, help='Number of download workers')
    parser.add_argument('--start_year', type=str, help='Download start year')
    parser.add_argument('--end_year', type=str, help='Download end year')
    parser.add_argument('--quarters', default=-1, nargs='+', type=int, choices=[-1, 1, 2, 3, 4], 
                        help='Quarters of documents to download - if -1 then all quarters')
    parser.add_argument('--log_level', default='INFO', choices=['INFO', 'ERROR', 'WARN', 'DEBUG'], help='Default logging level')
    parser.add_argument('--cache_dir', type=str, help='form idx cache dir')
    parser.add_argument('--ciks', nargs='+', type=int, help='List of CIKs to download')
    parser.add_argument('--cik_path', type=str, help='Path to CIK text file')
    parser.add_argument('--config_path', type=str, help='Path to yml config file')
    parser.add_argument('--search_term', type=str, help='search term to look for in forms')
    parser.add_argument('--rss_feed', action='store_true', default=False, help='downloads the rss feed and passes the arguments')
    
    args = parser.parse_args()

    look_for_search_term = False
    if args.search_term:
        look_for_search_term = True
        
    if args.config_path:
        args = yaml_config_to_args(args)

    if args.quarters == -1:
        args.quarters = list(range(1, 5))

    if args.cik_path:
        args.ciks = _read_cik_config(args.cik_path)

    # Setup logging
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s',
                        datefmt = '%Y.%m.%d | %H:%M:%S |',
                        level=getattr(logging, args.log_level))

    now_as_datetime_obj = datetime.now()

    if args.num_workers == -1:
        args.num_workers = 10

    # init container
    sec_container = SECContainer()
    sec_container.to_visit = set()
    sec_container.downloaded = set()
    sec_container.download_error = set()
    sec_container.last_url_message = '200'
    # capture seen files to filter out of new files
    seen_files = scan_output_dir(args.output_dir)
    logger.info(f'Scanned output dir - located {len(seen_files)} downloaded files')
    logger.info(f'Files: {seen_files}')
    
    if not args.rss_feed:
        if not look_for_search_term:
            start_year = dateutil_parser.parse(args.start_year).year
            end_year = dateutil_parser.parse(args.end_year).year
            years = list(range(start_year, end_year+1))
            time = list(product(years, args.quarters))
            for (yr, qtr) in tqdm(time, total=len(time)):
                sec_container.year = yr
                sec_container.quarter = qtr
                logger.info(f'Downloading files - Year: {yr} - Quarter: {qtr}')
                files = FormIDX(year=yr, quarter=qtr, seen_files=seen_files, cache_dir=args.cache_dir, 
                       form_types=args.form_types, ciks=args.ciks).index_to_files()
                if len(files) > 0:
                    sec_container.to_visit.update(files)
        else:
            logger.info(f'Searching for search term: {args.search_term}')
            start_date = dateutil_parser.parse(args.start_year)
            end_date = dateutil_parser.parse(args.end_year)
            #TODO fix this to allow for multiple form types
            files = FormIDX_search(form_type=args.form_types[0], start_date=start_date, end_date=end_date, search_term=args.search_term).index_to_files()
            sec_container.to_visit.update(files)
    else:
        if not look_for_search_term:
            logger.info('Downloading RSS feed, no search term defined')
            search_term = None
        else:
            logger.info(f'Searching for search term: {args.search_term}')
            if args.search_term.lower() == 'merger':
                search_term = 'item 1.01'
            else:
                raise ValueError('Search term not recognized')
        
        temp = RSSFormIDX(form_type=args.form_types[0],seen_files=seen_files,search_term=search_term)
        files = []
        while not temp.already_downloaded:
            files.extend(temp.index_to_files())
            logger.info('Getting next page of RSS feed')
            temp.get_next_page()
        sec_container.to_visit.update(files)

    logger.info(f'Files to download: {len(sec_container.to_visit)}')          
    loop = asyncio.get_event_loop()
    loop.run_until_complete(download_docs(args.output_dir,loop,args.num_workers,args.cache_dir))
    later_as_datetime_obj = datetime.now()
    time_difference = later_as_datetime_obj - now_as_datetime_obj
    total_seconds = time_difference.seconds
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    runtime_string = "The script took {}h:{}m:{}s.".format(hours, minutes, seconds)
    logger.info(runtime_string)

if __name__ == '__main__':
    main()
