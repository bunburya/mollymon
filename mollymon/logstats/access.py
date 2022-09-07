#!/usr/bin/env python3

from datetime import datetime, timezone
from urllib.parse import urlparse

from dateutil.parser import parse

import pandas as pd

"""
Some functions designed to parse and return interesting stats from Molly Brown access log files .

The parser functions return Pandas DataFrames, and most of the other functions take a DataFrame as their input.

"""


def parse_line(line: str) -> tuple[datetime, str, int, str, str, str, str, str, str]:
    """Parse a single line of a Molly Brown access log file.

    :param line: The line to parse.
    :returns: A tuple containing:
        0. the datetime of the request;
        1. the IP address from which the request came,
        2. the response code;
        3. the network location specified in the request;
        4. the path specified in the request;
        5. the parameters specified in the request;
        6. the query component of the request;
        7. the fragment component of the request; and
        8. the full body of the request (ie, the full URL requested).

    """
    tokens = line.strip().split()
    dt = parse(tokens[0])
    ip_addr = tokens[1]
    resp_code = int(tokens[2])
    try:
        req_body = tokens[3]
    except IndexError:
        req_body = ''
    url = urlparse(req_body)
    return (
        dt.astimezone(timezone.utc),
        ip_addr,
        resp_code,
        url.netloc,
        url.path,
        url.params,
        url.query,
        url.fragment,
        req_body
    )


def parse_file(fpath: str, since: datetime = None, until: datetime = None) -> pd.DataFrame:
    """Parse a Molly Brown access log file.

    :param fpath: Path to the file to parse.
    :param since: Only include entries after this date and time.
    :param until: Only include entries before this date and time.
    :returns: A Pandas DataFrame with the data.
    """
    rows = []
    columns = ['date_time', 'ip_addr', 'resp_code', 'netloc', 'path', 'params', 'query', 'fragment', 'request_body']
    with open(fpath) as f:
        for line in f:
            data = parse_line(line)
            if (since is not None) and (data[0] <= since):
                continue
            if (until is not None) and (data[0] >= until):
                continue
            rows.append(data)
    return pd.DataFrame(rows, columns=columns)


# Convenience functions for filtering the data

def total_count(df: pd.DataFrame) -> int:
    """Return the total number of log entries/requests."""
    return df.shape[0]


def total_by_resp_code(df: pd.DataFrame, resp_code: int) -> int:
    """Return the total number of requests that returned the given response code."""
    return df[df['resp_code'] == resp_code].shape[0]


def total_by_resp_codes(df: pd.DataFrame, resp_codes: list[int]) -> int:
    """Return the total number of requests that returned any of the given response codes."""
    return df[df['resp_code'] in resp_codes].shape[0]


def path_freq(df: pd.DataFrame) -> pd.Series:
    """Return a Pandas Series containing the number of times each path has been requested."""
    return df.groupby('path').size().sort_values(ascending=False)


def ip_addr_freq(df: pd.DataFrame) -> pd.Series:
    """Return a Pandas Series containing the requests that originated from each IP address."""
    return df.groupby('ip_addr').size().sort_values(ascending=False)


def unique_ip_count(df: pd.DataFrame) -> int:
    """Return the total number of unique IP addresses from which requests have originated."""
    return ip_addr_freq(df).size
