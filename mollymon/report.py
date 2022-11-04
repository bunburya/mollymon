from datetime import datetime
from typing import Optional

from mollymon.logstats import access, error
from mollymon.contact import DAO

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


def generate_report(access_log: str, error_log: str, capsule_name: str, msg_db: Optional[str] = None,
                    since: Optional[datetime] = None, until: Optional[datetime] = None) -> list[str]:
    report_time = datetime.utcnow()
    lines = [
        f'# Report for {capsule_name} at {report_time.strftime("%A %d %B %Y")}'
    ]
    if (since is not None) and (until is not None):
        lines.append(f'Period from {since.isoformat()} to {until.isoformat()}.')
    elif since is not None:
        lines.append(f'Period from {since.isoformat()} to present.')
    elif until is not None:
        lines.append(f'Period ending {until.isoformat()}.')
    lines.append('')

    access_df = access.parse_file(access_log, since, until)
    access_df = access_df[~access_df['path'].str.startswith('/remini')]  # Exclude Remini-related requests
    success_df = access_df[access_df['resp_code'] == 20]
    error_df = error.parse_file(error_log, since, until)

    lines.append('## Capsule traffic')
    lines.append('')

    total = access.total_count(access_df)
    total_uniq_ips = access.unique_ip_count(access_df)
    resp_codes = access.resp_code_freq(access_df)
    resp_codes_pct = ((resp_codes / total) * 100).round(2)

    lines.append(f'Total requests: {total}')
    lines.append(f'Unique IPs: {total_uniq_ips}')
    lines.append(f'Errors logged: {error_df.size}')
    lines.append('')
    lines.append('Response codes:')
    for r, p in zip(resp_codes.index, resp_codes_pct):
        lines.append(f'* {r} ({RESP_CODE_DESC.get(r, "[NO DESCRIPTION]")}): {resp_codes[r]} ({p}%)')

    is_gemlog = success_df['path'].str.startswith('/gemlog')
    gemlog_df = success_df[is_gemlog]
    gemlog_posts_df = gemlog_df[gemlog_df['path'].str.startswith('/gemlog/posts/202')]
    gemlog_uniq_ips = access.unique_ip_count(gemlog_df)
    gemlog_atom_hits = gemlog_df[gemlog_df['path'] == '/gemlog/posts/atom.xml'].size
    others_df = success_df[~is_gemlog]
    gemlog_posts_freq = access.path_freq(gemlog_posts_df)
    others_freq = access.path_freq(others_df)

    lines.append('')
    lines.append(f'Most popular page (excl. gemlog):')
    for p in others_freq.index[:3]:
        lines.append(f'* {p} ({others_freq[p]} hits)')

    lines.append('')
    lines.append('### Gemlog')
    lines.append('')
    lines.append(f'Total visits to gemlog: {gemlog_df.size}')
    lines.append(f'Unique IPs: {gemlog_uniq_ips}')
    lines.append(f'Hits on atom feed: {gemlog_atom_hits}')
    lines.append('')
    if gemlog_df.size:
        lines.append('Most popular posts:')
        for p in gemlog_posts_df.groupby('path').size().sort_values(ascending=False).index[:3]:
            lines.append(f'* {p} ({gemlog_posts_freq[p]} hits)')
        lines.append('')

    if msg_db:
        lines.append('## Messages')
        lines.append('')

        dao = DAO(msg_db)
        lines.append(f'Messages received: {dao.count_messages()} ({dao.count_messages(read=0)} unread)')

    return lines


def print_report(access_log: str, error_log: str, msg_db: str, capsule_name: str, since: datetime = None,
                 until: datetime = None):
    for line in generate_report(access_log, error_log, msg_db, capsule_name, since, until):
        print(line)
