from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import sqlite3
import json
import re

from search import preprocess, get_sql, calculate_score, save_search


class Debug:
    def __init__(self, level=0):
        self.min_level = level
        print(f'Debug:\n\t0 - Error\n\t1 - Warning\n\t2 - Info')
        self.set(level)

    def set(self, level):
        if level < self.min_level:
            return
        else:
            self.level = level
            print(f'Actual: {self.level}')

    def e(self, value, title=None):
        if self.level > 0: self._print(value, title)

    def w(self, value, title=None):
        if self.level > 1: self._print(value, title)

    def i(self, value, title=None):
        if self.level > 2: self._print(value, title)

    def _print(self, value, title=None):
        try:
            if isinstance(title, str): print(title + ':')
            print(value)
        except Exception as err:
            print('Debug error: ' + str(err))
            raise


class Database:
    def __init__(self):
        # cache database to memory
        # create in memory database
        self.conn = sqlite3.connect("file::memory:?cache=shared", uri=True)
        self.conn.row_factory = sqlite3.Row

        # pragmas
        self.conn.execute('pragma foreign_keys = off')

        # cursor
        self.cur = self.conn.cursor()

        # load
        self._load()
        # check
        self._check()

    def _load(self):
        # load disk database to in memory database
        def progress(status, remaining, total):
            DEBUG.i(f'Copied {total - remaining} of {total} pages to memory...')

        try:
            local = sqlite3.connect("database.sqlite")
            local.backup(self.conn, progress=progress)
            local.close()
        except AttributeError:
            cur = self.conn.cursor()
            self.cur.execute("ATTACH DATABASE 'database.sqlite' AS local")
            cur.execute("SELECT name FROM local.sqlite_master WHERE type = 'table'")
            rows = cur.fetchall()
            for row in rows:
                cur.execute("CREATE TABLE " + row['name'] + " AS SELECT * FROM local." + row['name'])
            self.cur.execute("DETACH DATABASE local")

        # generate indexes
        self.conn.execute('CREATE UNIQUE INDEX IDX1 ON dictionary (Word)')
        self.conn.execute('CREATE UNIQUE INDEX IDX2 ON word_by_video (VideoPK, WordPK)')
        self.conn.execute('CREATE UNIQUE INDEX IDX3 ON word_by_channel (WordPK, ChannelPK)')
        self.conn.execute('CREATE UNIQUE INDEX IDX4 ON video (VideoPK)')
        self.conn.execute('CREATE UNIQUE INDEX IDX5 ON channel (ChannelPK)')
        self.conn.execute('CREATE INDEX IDX6 ON word_by_video (WordPK, ChannelPK)')
        self.conn.execute('CREATE UNIQUE INDEX IDX7 ON dictionary (Word) WHERE NGram=1')

    def _check(self):
        # check
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(1) AS CNT FROM dictionary")
            row = cur.fetchall()
            DEBUG.i(f"{row[0]['CNT']} words in dictionary...")
        except Exception as err:
            DEBUG.e(err)
            raise
        finally:
            cur.close()

    def new(self):
        conn = sqlite3.connect("file::memory:?cache=shared")
        conn.row_factory = sqlite3.Row
        return conn

    def query(self, sql, binds=None):
        try:
            cur = self.conn.cursor()

            if binds is None:
                self.cur.execute(sql)
            else:
                self.cur.execute(sql, binds)

            # get results
            rows = self.cur.fetchall()

            result = []
            for row in rows:
                result.append(dict(row))

            cur.close()
            return result
        except:
            try:
                cur.close()
            except:
                None
            finally:
                return None

    def close(self):
        try:
            self.conn.close()
        except:
            None

    def __del__(self):
        self.close()


class Cache:
    def __init__(self):
        # cache files to memory
        self._cache = {}

    def _load(self, filename, type='r'):
        with open(filename, type) as f:
            content = f.read()
        self._cache[filename] = content

    def exist(self, filename):
        if filename in self._cache:
            return True
        else:
            return False

    def get(self, filename):
        if self.exist(filename):
            return self._cache[filename]
        else:
            DEBUG.i(filename, 'Not exist!')
            return None

    def html(self, filename):
        self._load(filename)
        self._cache[filename] = self._cache[filename].replace('\t', ' ')
        self._cache[filename] = re.sub(' +', ' ', self._cache[filename])
        self._cache[filename] = self._cache[filename].encode("utf-8")

    def binary(self, filename):
        self._load(filename, 'rb')

    def text(self, filename):
        self._load(filename)
        self._cache[filename] = self._cache[filename].encode("utf-8")


class GetHandler(BaseHTTPRequestHandler):

    # def __init__(self, *args, **kwargs):
    # def __del__(self):

    def _set_headers(self, type='text/html'):
        self.send_response(200)
        self.send_header('Content-Type', type)
        self.end_headers()

    def _send(self, data):
        try:
            if data is None:
                raise
            else:
                self.wfile.write(data)
        except:
            self.wfile.write(''.encode("utf-8"))

    def do_HEAD(self):
        self._set_headers()

    def do_GET(self):
        # url
        url = urlparse(self.path)
        # get all arguments
        argv = parse_qs(url.query)

        # check database
        DATABASE._check()

        if url.path.startswith("/favicon.ico"):
            self._set_headers('image/png')
            self._send(CACHE.get('favicon.ico'))

        if url.path.startswith("/search"):  # search for a result
            self._set_headers()

            # debug??
            try:
                if argv['debug'][0].isdigit():
                    debug = int(argv['debug'][0])
                else:
                    raise
            except:
                debug = 0

            DEBUG.i(debug, 'Get debug string')
            DEBUG.set(debug)

            # get search string
            try:
                search = str(argv['q'][0])
                if search is None or len(search) < 1: raise ValueError('Search len < 1')
            except ValueError as err:
                DEBUG.w(err, 'Get search string')
                search = None
            except Exception as err:
                DEBUG.e(err, 'Get search string')
                search = None

            # nothing to search
            if search == None:
                DEBUG.w(search, 'Search is empty')
                self._send(None)
                return

            # search
            try:
                # cleanup
                tokens = preprocess(search)
                DEBUG.w(tokens, 'Tokens (preprocess)')
                if tokens is None or len(tokens) < 1: raise ValueError('Tokens len < 1')

                # query
                sql = get_sql(tokens)
                # DEBUG.w(sql, 'Query')
                results = DATABASE.query(sql, tokens)
                # DEBUG.w(results, 'Results (get_sql, query)')
                if results is None or len(results) < 1: raise ValueError('Results len < 1')

                from datetime import datetime
                date = datetime.now()
                search_content_id = save_search(DATABASE.conn,search,results,date)
                print(search_content_id)

                # calculate the score
                sorted = calculate_score(results)

                # send results
                self._send(json.dumps(sorted).encode("utf-8"))

            except ValueError as err:
                DEBUG.w(err)
                self._send(json.dumps('').encode("utf-8"))
                return
            except Exception as err:
                DEBUG.e('Exception on search: ' + str(err))
                self._send(json.dumps('').encode("utf-8"))
                return

        else:  # all other get
            self._set_headers()

            # reload cache??
            try:
                if argv['cache'] != '':
                    DEBUG.i('Reload cache...')
                    CACHE.html('index.html')
                    CACHE.html('privacy-policy.html')
                    CACHE.html('terms-conditions.html')
                    CACHE.html('cookies.html')
                    CACHE.binary('favicon.ico')
                    CACHE.text('wordcloud2.min.js')
            except:
                None

            # in cache?
            filename = url.path[1:]  # remove the first char -> "/"
            if CACHE.exist(filename) == False:
                filename = 'index.html'

            # send
            self._send(CACHE.get(filename))


if __name__ == '__main__':
    try:
        DEBUG = Debug(100)

        # cache database to memory
        # create in memory database
        DATABASE = Database()

        # cache files to memory
        CACHE = Cache()
        # index.html
        CACHE.html('index.html')
        #privacy policy
        CACHE.html('privacy-policy.html')
        #terms and conditions
        CACHE.html('terms-conditions.html')
        #cookies
        CACHE.html('cookies.html')
        # favicon.ico
        CACHE.binary('favicon.ico')
        # others
        CACHE.text('wordcloud2.min.js')

        # open http server
        server = HTTPServer(('', 80), GetHandler)
        print('Starting server, use <Ctrl + F2> to stop')
        server.serve_forever()
    except KeyboardInterrupt:
        # close http server
        server.socket.close()
        # close in memory databse
        DATABASE.close()
