from datetime import datetime

from mollymon.logstats import access

RESP_CODE_DESC = {
    10: 'INPUT',
    11: 'SENSITIVE INPUT',
    20: 'SUCCESS',
    30: 'REDIRECT - TEMPORARY',
    31: 'REDIRECT - PERMANENT',
    40: 'TEMPORARY FAILURE',
    41: 'SERVER UNAVAILABLE',
    42: 'CGI ERROR',
    43: 'PROXY ERROR',
    44: 'SLOW DOWN',
    50: 'PERMANENT FAILURE',
    51: 'NOT FOUND',
    52: 'GONE',
    53: 'PROXY REQUEST REFUSED',
    59: 'BAD REQUEST',
    60: 'CLIENT CERTIFICATE REQUIRED',
    61: 'CERTIFICATE NOT AUTHORISED',
    62: 'CERTIFICATE NOT VALID'
}

def generate_report(access_log: str, error_log: str, msg_db: str, capsule_name: str,
                    since: datetime = None, until: datetime = None) -> list[str]:

    report_time = datetime.utcnow()
    lines = [
        f'# Report for {capsule_name} at {report_time.isoformat()}'
    ]
    if (since is not None) and (until is not None):
        lines.append(f'Period from {since.isoformat()} to {until.isoformat()}.')
    elif since is not None:
        lines.append(f'Period from {since.isoformat()} to present.')
    elif until is not None:
        lines.append(f'Period ending {until.isoformat()}.')
    lines.append('')

    access_df = access.parse_file(access_log, since, until)
    access_df = access_df[~access_df['path'].str.startswith('/remini')] # Exclude Remini-related requests
    success_df = access_df[access_df['resp_code'] == 20]

    lines.append('## Capsule traffic')
    lines.append('')

    total = access.total_count(access_df)
    total_uniq_ips = access.unique_ip_count(access_df)
    resp_codes = access.resp_code_freq(access_df)

    lines.append(f'Total requests: {total}')
    lines.append(f'Unique IPs: {total_uniq_ips}')
    lines.append('Response codes:')
    for r in resp_codes.index:
        lines.append(f'* {r} ({RESP_CODE_DESC[r]}): {resp_codes[r]}')

    is_gemlog = success_df['path'].str.startswith('/gemlog')
    gemlog_df = success_df[is_gemlog]
    gemlog_posts_df = gemlog_df[gemlog_df['path'].str.startswith('/gemlog/posts/202')]
    gemlog_uniq_ips = access.unique_ip_count(gemlog_df)
    gemlog_atom_hits = gemlog_df[gemlog_df['path'] == '/gemlog/posts/atom.xml'].size
    others_df = success_df[~is_gemlog]
    gemlog_posts_freq = access.path_freq(gemlog_posts_df)
    others_freq = access.path_freq(others_df)

    lines.append(f'Most popular page (excl. gemlog): {others_freq.index[0]} ({others_freq[0]} hits).')

    lines.append('')
    lines.append('### Gemlog')
    lines.append('')
    lines.append(f'Total visits to gemlog: {gemlog_df.size}')
    lines.append(f'Unique IPs: {gemlog_uniq_ips}')
    lines.append(f'Hits on atom feed: {gemlog_atom_hits}')
    lines.append('')
    lines.append('Most popular posts:')
    for p in gemlog_posts_df.groupby('path').size().sort_values(ascending=False).index[:3]:
        lines.append(f'* {p} ({gemlog_posts_freq[p]} hits)')

    lines.append('')
    lines.append('## Messages')

    return lines





