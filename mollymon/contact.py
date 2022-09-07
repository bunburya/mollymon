#!/usr/bin/env python3

import logging
import os
import sqlite3 as sql
import threading
import argparse

from urllib.parse import unquote, urlparse
from datetime import datetime
from typing import List, Union, Tuple

from platformdirs import user_data_dir, user_log_dir, user_runtime_dir

PKG_NAME = 'mollymon'
APP_NAME = 'contact'

RUN_DIR = user_runtime_dir(PKG_NAME)
LOG_DIR = user_log_dir(PKG_NAME)
DATA_DIR = user_data_dir(PKG_NAME)


class DAO:

    # Generally, all columns should be NOT NULL, and (where appropriate) set to
    # empty strings if not relevant to a particular message. This allows us
    # to filter by entries with empty values (otherwise, there would be
    # ambiguity in the `select_query` method where we specify None as the
    # desired value).
    SCHEMA = f"""CREATE TABLE IF NOT EXISTS \"messages\" (
        script_path TEXT NOT NULL,
        path_info TEXT NOT NULL,
        tls_client_hash TEXT NOT NULL,
        ip_addr TEXT NOT NULL,
        time TEXT NOT NULL,
        message TEXT NOT NULL,
        read INTEGER NOT NULL
    )"""

    ADD_MESSAGE = f"""INSERT INTO \"messages\" (script_path, path_info, tls_client_hash, ip_addr, time, message, read)
        VALUES(?, ?, ?, ?, ?, ?, ?)
    """

    GET_UNREAD = f"""SELECT * FROM \"messages\" WHERE read=0"""

    MARK_READ_SINGLE = f'UPDATE "messages" SET read=1 WHERE rowid=?'

    MARK_READ_MULTI = f'UPDATE "messages" SET read=1 WHERE rowid in '

    def __init__(self, db_fpath: str):
        db_dir = os.path.dirname(db_fpath)
        if not os.path.exists(db_dir):
            os.makedirs(db_dir)
        self.lock = threading.Lock()
        self.conn = sql.connect(db_fpath, check_same_thread=False)
        self.conn.set_trace_callback(logging.debug)
        self.cursor = self.conn.cursor()
        self.sql_execute(self.SCHEMA)
        self.conn.commit()

    def select_query(self, count: bool = False, script_path: str = None, path_info: str = None,
            tls_client_hash: str = None, ip_addr: str = None, since: datetime = None,
            until: datetime = None, read: bool = None, rowid: list[int] = None) -> tuple[str, list[any]]:
        """Build a basic query for searching the database based on the given inputs.

        :param count: Whether to return the count of results only (as opposed to the results themselves).
        :param script_path: Filter by script path (the part of the request path that the server maps to the SCGI app).
        :param path_info: Filter by path info (the part of the path following script_path).
        :param tls_client_hash: Filter by the hash of the TLS client certificate if provided.
        :param ip_addr: Filter by IP address.
        :param since: Return only messages left on or after the given date and time.
        :param until: Return only messages left on or before the given date and time.
        :param read: Filter by read/unread.

        """

        query_tokens = ['SELECT']
        if count:
            query_tokens.append('COUNT(*)')
        else:
            query_tokens.append('rowid, *')
        query_tokens.append('FROM "messages"')
        where: list[str] = []
        params: list[Any] = []
        if since is not None:
            since = since.strftime('%Y-%m-%d %H:%M:%S')
        if until is not None:
            until = until.strftime('%Y-%m-%d %H:%M:%S')
        if since and until:
            where.append('time BETWEEN ? and ?')
            params += [since, until]
        elif since:
            where.append('time > ?')
            params.append(since)
        elif until:
            where.append('time < ?')
            params.append(until)
        if script_path is not None:
            where.append('script_path = ?')
            params.append(script_path)
        if path_info is not None:
            where.append('path_info = ?')
            params.append(path_info)
        if tls_client_hash is not None:
            where.append('tls_client_hash = ?')
            params.append(tls_client_hash)
        if ip_addr is not None:
            where.append('ip_addr = ?')
            params.append(ip_addr)
        if read is not None:
            where.append('read = ?')
            params.append(int(read))
        if rowid:
            where.append(f'rowid IN ({",".join("?" * len(rowid))})')
            params.extend(rowid)
        query = ' '.join(query_tokens)
        if where:
            query += ' WHERE ' + ' AND '.join(where)
        return query, params

    def sql_execute(self, *args, **kwargs):
        with self.lock:
            self.cursor.execute(*args, **kwargs)

    def sql_fetchall(self) -> list[tuple]:
        with self.lock:
            return self.cursor.fetchall()

    def sql_fetchone(self):
        with self.lock:
            return self.cursor.fetchone()

    def add_message(self, script_path: str, path_info: str, tls_client_hash: str, ip_addr: str, time: datetime, msg: str):
        self.sql_execute(self.ADD_MESSAGE, (script_path, path_info, tls_client_hash, ip_addr, time, msg, 0))
        self.conn.commit()

    def get_messages(self, script_path: str = None, path_info: str = None, tls_client_hash: str = None, ip_addr: str = None, since: datetime = None, until: datetime = None, read: int = None) -> list[tuple]:
        self.sql_execute(*self.select_query(script_path=script_path, path_info=path_info, tls_client_hash=tls_client_hash, ip_addr=ip_addr, since=since, until=until, read=read))
        return self.sql_fetchall()

    def count_messages(self, script_path: str = None, path_info: str = None, tls_client_hash: str = None, ip_addr: str = None, since: datetime = None, until: datetime = None, read: int = None) -> int:
        self.sql_execute(*self.select_query(count=True, script_path=script_path, path_info=path_info, tls_client_hash=tls_client_hash, ip_addr=ip_addr, since=since, until=until, read=read))
        return self.sql_fetchone()[0]

    def mark_read(self, rowid: list[int]):
        """Mark the specified entries as read."""
        if len(rowid) > 1:
            self.sql_execute(self.MARK_READ_MULTI + ', '.join('?' * len(rowid)), rowid)
        elif len(rowid) == 1:
            self.sql_execute(self.MARK_READ_SINGLE, (rowid[0],))
        self.conn.commit()

# Functions for sending Gemini responses to clients.

def get_input(prompt: str = '') -> bytes:
    """Send a 10 (input) response.

    :param prompt: Prompt to display to the user.
    :return: Bytes to be sent to the client.

    """
    return f'10 {prompt}\r\n'.encode()

def display_content(content: str, content_type: str = 'text/gemini') -> bytes:
    """Send a 20 (OK) response followed by the given content.

    :param content: Content to display to the client.
    :return: Bytes to be sent to the client.
    
    """
    return f'20 {content_type}\r\n{content}\n'.encode()

def temp_failure(msg: str) -> bytes:
    """Send a 40 (temporary failure) response with the given message.

    :param msg: Error message to send with response.
    :return: Bytes to be sent to the client.

    """
    return f'40 {msg}\r\n'.encode()

# Commands to query the DB from the command line.

def print_messages(dao: DAO, since: datetime = None, unread_only: bool = False, mark_read: bool = True):
    results = dao.get_messages(since=since, read=(0 if unread_only else None))
    for (rowid, script_path, path_info, tls_client_hash, ip_addr, time, message, read) in results:
        print(f'Script path: {script_path}')
        print(f'Path info: {path_info}')
        print(f'TLS client hash: {tls_client_hash}')
        print(f'IP address: {ip_addr}')
        print(f'Time: {time} UTC')
        print(f'Message: {message}')
        print(f'Read: {bool(read)}')
        print()
    if mark_read:
        dao.mark_read(list(r[0] for r in results))

def print_message_count(dao: DAO, since: datetime = None, unread_only: bool = False):
    c = dao.count_messages(since=since, read=(0 if unread_only else None))
    msg = f'{c} messages'
    if since is not None:
        msg += f' since {since.strftime("%Y-%m-%d at %H:%M UTC")}.'
    else:
        msg += '.'
    print(msg)

def test():
    TEST_DB_FILE = '/tmp/gemnote_test.sql'
    if os.path.exists(TEST_DB_FILE):
        os.remove(TEST_DB_FILE)
    t1 = datetime(2022, 6, 19, 15, 33)
    t2 = datetime(2022, 7, 24, 2, 19)
    dao = DAO(TEST_DB_FILE)
    dao.add_message('/test/path/1', 'path_info_1', '', '127.0.0.1', datetime(2022, 4, 20, 12, 6), 'This is test comment 1')
    dao.add_message('/test/path/2', 'path_info_2', '', '127.0.0.2', datetime(2022, 5, 12, 7, 40), 'This is test comment 2')
    dao.add_message('/test/path/2', 'path_info_3', 'test_hash', '127.0.0.3', datetime(2022, 6, 25, 12, 44), 'This is test comment 3')
    dao.add_message('/test/path/3', 'path_info_4', '', '127.0.0.1', datetime(2022, 8, 2, 12, 30), 'This is test comment 4')

    results = dao.get_messages()
    #print(results)
    assert len(results) == 4
    assert dao.count_messages() == 4
    for r in results:
        assert r[-1] == 0

    results = dao.get_messages(ip_addr='127.0.0.1')
    assert len(results) == 2
    for r in results:
        #print(r)
        assert r[4] == '127.0.0.1'

    results = dao.get_messages(since=t1)
    #print(results)
    assert len(results) == 2

    results = dao.get_messages(since=t1, until=t2)
    assert len(results) == 1
    assert results[0][1] == '/test/path/2'
    assert results[0][4] == '127.0.0.3'

    dao.mark_read([r[0] for r in results])
    results = dao.get_messages(read=0)
    assert len(results) == 3
    for r in results:
        assert r[-1] == 0


def serve_scgi(dao: DAO, sock_fpath: str):
    
    import socket
    import scgi.scgi_server

    sock_dir = os.path.dirname(sock_fpath)
    if not os.path.exists(sock_dir):
        os.makedirs(sock_dir)

    if os.path.exists(sock_fpath):
        os.remove(sock_fpath)
    
    class RequestHandler(scgi.scgi_server.SCGIHandler):

        def produce(self, env, bodysize, input, output):
            logging.debug(f'Received request: {env}')
            script_path = env.get('SCRIPT_PATH')
            path_info = env.get('PATH_INFO')
            tls_client_hash = env.get('TLS_CLIENT_HASH', '')
            query = unquote(env.get('QUERY_STRING'))
            ip_addr = env.get('REMOTE_ADDR')
            if query:
                try:
                    dao.add_message(script_path, path_info, tls_client_hash, ip_addr, datetime.now(), query)
                    resp = display_content('Thank you for your message!', 'text/plain')
                except Exception as e:
                    logging.error(e, exc_info=True)
                    resp = temp_failure('Something went wrong. Error has been logged.')
            else:
                resp = get_input('Please enter your message.')

            output.write(resp)
    
    s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    s.bind(sock_fpath)
    server = scgi.scgi_server.SCGIServer(handler_class=RequestHandler)
    server.serve_on_socket(s)

# "Top-level" functions, ie, those that are invoked directly by the argument parser

def run_scgi(args: argparse.Namespace):
    dao = DAO(args.db_file)
    return serve_scgi(dao, args.sock_file)

def run_test(args: argparse.Namespace):
    test()
    print('Tests passed.')

def run_print_msgs(args: argparse.Namespace):
    dao = DAO(args.db_file)
    return print_messages(dao, unread_only=args.unread, mark_read=args.mark_read)

def run_count_msgs(args: argparse.Namespace):
    dao = DAO(args.db_file)
    return print_message_count(dao, unread_only=args.unread)


def get_argparser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Allow leaving messages on a Gemini capsule.')
    parser.add_argument('--debug', action='store_true', help='Debug mode.')
    parser.add_argument('--log-file', help='Specify where to store application logs. Log to stderr by default.')
    parser.add_argument('--db-file', help='The database file to use for storing and retrieving messages.', default=os.path.join(DATA_DIR, f'{APP_NAME}.db'))
    subparsers = parser.add_subparsers()
    run_parser = subparsers.add_parser('run', help='Run the SCGI application.')
    run_parser.add_argument('--sock-file', help='Where to store the socket file.', default=os.path.join(RUN_DIR, f'{APP_NAME}.sock'))
    run_parser.set_defaults(func=run_scgi)
    print_parser = subparsers.add_parser('print', help='Print messages.')
    print_parser.add_argument('--unread', action='store_true', help='Print unread messages only.')
    print_parser.add_argument('--mark-read', action='store_true', help='Mark messages as read once printed.')
    print_parser.set_defaults(func=run_print_msgs)
    count_parser = subparsers.add_parser('count', help='Print count of unread messages.')
    count_parser.add_argument('--unread', action='store_true', help='Print count of unread messages only.')
    count_parser.set_defaults(func=run_count_msgs)
    test_parser = subparsers.add_parser('test', help='Run some basic tests to verify functionality.')
    test_parser.set_defaults(func=run_test)
    return parser

def main(argv: list[str]):
    parser = get_argparser()
    args = parser.parse_args()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    if args.log_file:
        logging.basicConfig(filename=args.log_file)
    if 'func' in args:
        return args.func(args)
    else:
        return parser.print_help()


if __name__ == '__main__':
    import sys
    main(sys.argv)
