import logging
from datetime import datetime
import argparse
from pathlib import Path
from typing import List

logger = logging.getLogger(__name__)

# get form file paths from start year to end year and for defined quarters 
def get_form_file_paths(output_dir : str, form_type : str, year : int, quarter) -> List[str]:
    form_type = form_type.replace('/', '')
    form_folder = f"{output_dir}/{form_type}/{year}/Q{quarter}"

    # get all txt files in the form folder
    form_file_paths = []
    form_file_paths.extend(list(Path(form_folder).rglob('*.txt')))
    return form_file_paths


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--log_level', default='INFO', choices=['INFO', 'ERROR', 'WARN'], help='Default logging level')
    args = parser.parse_args()
    # Setup logging
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(name)s -   %(message)s',
                        datefmt = '%Y.%m.%d | %H:%M:%S |',
                        level=getattr(logging, args.log_level))

    now_as_datetime_obj = datetime.now()
    later_as_datetime_obj = datetime.now()
    time_difference = later_as_datetime_obj - now_as_datetime_obj
    total_seconds = time_difference.seconds
    hours, remainder = divmod(total_seconds, 3600)
    minutes, seconds = divmod(remainder, 60)

    runtime_string = "The script took {}h:{}m:{}s.".format(hours,minutes,seconds)
    logger.info(runtime_string)
if __name__ == '__main__':
    main()