from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
import sqlite3
import json
import re

from search import preprocess, get_sql, calculate_score

class Debug:
    def __init__(self, level = 0):
        self.min_level = level
        print(f'Debug:\n\t0 - Error\n\t1 - Warning\n\t2 - Info')
        self.set(level)

    def set(self, level):
        if level < self.min_level: return
        else:
            self.level = level
            print(f'Actual: {self.level}')

    def e(self, value, title = None):
        if self.level > 0: self._print(value, title)
    def w(self, value, title = None):
        if self.level > 1: self._print(value, title)
    def i(self, value, title = None):
        if self.level > 2: self._print(value, title)

    def _print(self, value, title = None):
        try:
            if isinstance(title, str): print(title + ':')
            print(value)
        except Exception as err:
            print('Debug error: ' + str(err))
            raise

class Database:
    def __init__(self):
        #cache database to memory
        #create in memory database
        self.conn = sqlite3.connect("file::memory:?cache=shared", uri=True)
        self.conn.row_factory = sqlite3.Row
        
        #pragmas
        self.conn.execute('pragma foreign_keys = off')

        #cursor
        self.cur = self.conn.cursor()

        #load
        self._load()
        #check
        self._check()

    def _load(self):
        #load disk database to in memory database
        def progress(status, remaining, total):
            DEBUG.i(f'Copied {total-remaining} of {total} pages to memory...')

        try:
            local = sqlite3.connect("database.sqlite")
            local.backup(self.conn, progress = progress)
            local.close()
        except AttributeError:
            cur = self.conn.cursor()
            self.cur.execute("ATTACH DATABASE 'database.sqlite' AS local")
            cur.execute("SELECT name FROM local.sqlite_master WHERE type = 'table'")
            rows = cur.fetchall()
            for row in rows:
                cur.execute("CREATE TABLE " + row['name'] + " AS SELECT * FROM local." + row['name'])
            self.cur.execute("DETACH DATABASE local")
                

    def _check(self):
        #check
        try:
            cur = self.conn.cursor()
            cur.execute("SELECT COUNT(1) AS CNT FROM dictionary")
            row = cur.fetchall()
            DEBUG.i(f"{row[0]['CNT']} words in dictionary...")
        except Exception as err:
            DEBUG.e(err)
            raise
        finally: cur.close()
    
    def new(self):
      conn = sqlite3.connect("file::memory:?cache=shared")
      conn.row_factory = sqlite3.Row
      return conn

    def query(self, sql, binds = None):
        try:
            cur = self.conn.cursor()

            if binds is None: self.cur.execute(sql)
            else: self.cur.execute(sql, binds)

            #get results
            rows = self.cur.fetchall()

            result = []
            for row in rows:
                result.append(dict(row))

            cur.close()
            return result
        except:
            try: cur.close()
            except: None
            finally: return None

    def close(self):
        try: self.conn.close()
        except: None

    def __del__(self):
        self.close()

class Cache:
    def __init__(self):
        #cache files to memory
        self._cache = {}

    def _load(self, filename, type = 'r'):
        with open(filename, type) as f:
            content = f.read()
        self._cache[filename] = content

    def html(self, filename):
        self._load(filename)
        self._cache[filename] = self._cache[filename].replace('\t', ' ')
        self._cache[filename] = re.sub(' +', ' ', self._cache[filename])
        self._cache[filename] = self._cache[filename].encode("utf-8")

    def binary(self, filename):
        self._load(filename, 'rb')

    def text(self, filename):
        self._load(filename, 'rb')
        self._cache[filename] = self._cache[filename].encode("utf-8")

class GetHandler(BaseHTTPRequestHandler):

    #def __init__(self, *args, **kwargs):
    #def __del__(self):

    def _set_headers(self, type = 'text/html'):
        self.send_response(200)
        self.send_header('Content-Type', type)
        self.end_headers()

    def _send(self, data):
        try:
            if data is None: raise
            else: self.wfile.write(data)
        except: self.wfile.write(''.encode("utf-8"))

    def do_HEAD(self):
        self._set_headers()

    def do_GET(self):
        #check database
        DATABASE._check()

        if self.path == "/favicon.ico":
            self._set_headers('image/png')
            self._send(FAVICON)

        if self.path.startswith("/search"):#search for a result
            self._set_headers()

            #get all arguments
            argv = parse_qs(urlparse(self.path).query)

            #debug??
            try:
              if argv['debug']: debug = argv['debug']
              else: raise
            except: debug = 0

            DEBUG.i(debug, 'Get debug string')
            DEBUG.set(debug)

            #get search string
            try:
                search = str(argv['q'][0])
                if search is None or len(search) < 1: raise ValueError('Search len < 1')
            except ValueError as err:
                DEBUG.w(err, 'Get search string')
                search = None
            except Exception as err:
                DEBUG.e(err, 'Get search string')
                search = None

            #nothing to search
            if search == None:
                DEBUG.w(search, 'Search is empty')
                self._send(None)
                return

            #search
            try:
                #cleanup
                tokens = preprocess(search)
                DEBUG.w(tokens, 'Tokens (preprocess)')
                if tokens is None or len(tokens) < 1: raise ValueError('Tokens len < 1')

                #query
                sql = get_sql(tokens)
                #DEBUG.w(sql, 'Query')
                results = DATABASE.query(sql, tokens)
                #DEBUG.w(results, 'Results (get_sql, query)')
                if results is None or len(results) < 1: raise ValueError('Results len < 1')

                #calculate the score
                sorted = calculate_score(results)

                #send results
                self._send(json.dumps(sorted).encode("utf-8"))

            except ValueError as err:
                DEBUG.w(err)
                self._send(json.dumps('').encode("utf-8"))
                return
            except Exception as err:
                DEBUG.e('Exception on search: ' + str(err))
                self._send(json.dumps('').encode("utf-8"))
                return
    
        else: #all other get
            self._set_headers()
            self._send(INDEX)

if __name__ == '__main__':
    try:
        DEBUG = Debug(100)
    
        #cache database to memory
        #create in memory database
        DATABASE = Database()
        
        #cache files to memory
        #index.html
        with open('index.html', 'r') as f:
            content = f.read()
            content = content.replace('\t', ' ')
            content = re.sub(' +', ' ', content)
        INDEX = content.encode("utf-8")#remove spaces
        #favicon.ico
        with open('favicon.ico', 'rb') as f:
            content = f.read()
        FAVICON = content

        #open http server
        server = HTTPServer(('', 80), GetHandler)
        print('Starting server, use <Ctrl + F2> to stop')
        server.serve_forever()
    except KeyboardInterrupt:
        #close http server
        server.socket.close()
        #close in memory databse
        DATABASE.close()