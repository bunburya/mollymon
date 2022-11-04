import logging
from datetime import datetime, timezone

import pandas as pd
from dateutil.parser import parse

"""
Some functions designed to parse and return interesting stats from Molly Brown error log files .

"""


def parse_line(line: str) -> tuple[datetime, str]:
    """Parse a single line of a Molly Brown error log file.

    :param line: The line to parse.
    :returns: A tuple containing:
        0. the datetime of the error;
        1. the error message.

    """
    tokens = line.strip().split()
    dt = parse(' '.join(tokens[0:2]))
    err_msg = ' '.join(tokens[2:])
    return (
        dt.astimezone(timezone.utc),
        err_msg
    )


def parse_file(fpath: str, since: datetime = None, until: datetime = None) -> pd.DataFrame:
    """Parse a Molly Brown error log file.

    :param fpath: Path to the file to parse.
    :param since: Only include entries after this date and time.
    :param until: Only include entries before this date and time.
    :returns: A Pandas DataFrame with the data.
    """
    rows = []
    columns = ['date_time', 'err_msg']
    with open(fpath, 'rb') as f:
        for line in f:
            logging.debug(f'Parsing line: {line}')
            data = parse_line(line.decode(errors='replace'))
            if (since is not None) and (data[0] <= since):
                continue
            if (until is not None) and (data[0] >= until):
                continue
            rows.append(data)
    return pd.DataFrame(rows, columns=columns)
