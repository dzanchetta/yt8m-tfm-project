from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

def index_form(q):
    if q != None:
        value = str(q)
    else:
        value = ''
    return """<form>
        <div class="container h-100">
          <div class="d-flex justify-content-center h-100">
            <div class="searchbar">
              <input class="search_input" type="text" name="q" value='""" + value + """' placeholder="Search and press Enter...">
              <a href="#" class="search_icon"><i class="fas fa-search"></i></a>
            </div>
          </div>
        </div>
    </form>"""

index_start = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1, shrink-to-fit=no">
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css" integrity="sha384-JcKb8q3iqJ61gNV9KGb8thSsNjpSL0n8PARn9HuZOnIxN0hoP+VmmDGMN5t9UJ0Z" crossorigin="anonymous">
    <link rel="stylesheet" href="https://use.fontawesome.com/releases/v5.5.0/css/all.css" integrity="sha384-B4dIYHKNBt8Bc12p+WXckhzcICo0wtJAoU8YZTY5qE0Id1GSseTk6S+L3BlXeVIU" crossorigin="anonymous">
    <script src="https://code.jquery.com/jquery-3.5.1.slim.min.js" integrity="sha384-DfXdz2htPH0lsSSs5nCTpuj/zy4C+OGpamoFVy38MVBnE+IbbVYUew+OrCXaRkfj" crossorigin="anonymous"></script>
    <script src="https://cdn.jsdelivr.net/npm/popper.js@1.16.1/dist/umd/popper.min.js" integrity="sha384-9/reFTGAW83EW2RDu2S0VKaIzap3H66lZH81PoYlFhbGU+6BZp6G7niu735Sk7lN" crossorigin="anonymous"></script>
    <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/js/bootstrap.min.js" integrity="sha384-B4gt1jrGC7Jh4AgTPSdUtOBvfO8shuf57BaghqFfPlYxofvL8/KUEfYiJOMMV+rV" crossorigin="anonymous"></script>
    <title>Youtube Search Engine</title>
    <style>
    body,html{
    height: 100%;
    width: 100%;
    margin: 0;
    padding: 0;
    background: white !important;
    }

    .searchbar{
    margin-bottom: auto;
    margin-top: auto;
    height: 60px;
    background-color: #353b48;
    border-radius: 30px;
    padding: 10px;
    }

    .search_input{
    color: white;
    border: 0;
    outline: 0;
    background: none;
    width: 0;
    caret-color:transparent;
    line-height: 40px;
    transition: width 0.4s linear;
    }

    .searchbar > .search_input{
    padding: 0 10px;
    width: 450px;
    caret-color:red;
    transition: width 0.4s linear;
    }

    .searchbar > .search_icon{
    background: white;
    color: #e74c3c;
    }

    .search_icon{
    height: 40px;
    width: 40px;
    float: right;
    display: flex;
    justify-content: center;
    align-items: center;
    border-radius: 50%;
    color:white;
    text-decoration:none;
    }</style>
  </head>
  <body>
  <br><br>
  <div class="container">
    <div class="container h-100">
        <div class="d-flex justify-content-center h-100">
            <h1>Youtube Search Engine</h1>
        </div>
    </div>
"""
index_end = '</div></body></html>'

class GetHandler(BaseHTTPRequestHandler):

    def do_HEAD(self):
        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()

    def do_GET(self):
        def x(html):
            try:
                self.wfile.write(html.encode())
            except:
                self.wfile.write(b'Erro!')

        def d(var):
            print('DEBUG:')
            print(var)
            x('DEBUG:<br>' + str(var) + '<br>')

        self.send_response(200)
        self.send_header("Content-type", "text/html")
        self.end_headers()
        # <--- HTML starts here --->
        x(index_start)

        argv = parse_qs(urlparse(self.path).query)

        try:
          if argv['debug']:
            debug = True
        except: debug = False

        try:
            search = str(argv['q'][0])
            if(len(search) < 1):
                search = None
        except: search = None

        x(index_form(search))

        if search != None:
            #x('<h2>Search for:</h2>' + search + '<br>')
            #try:
            from search import preprocess, do_search, calculate_score
            tokens = preprocess(search)
            if(len(tokens) < 1):
              if debug: d('Tokens len < 1')
              raise
            results = do_search(tokens)
            final = calculate_score(results)
            #except:
                #results = None

            #x('<h2>Results:</h2>')
            x('<div class="container h-100">')
            if final != None:
                for i in final:
                    x("""<div class="row border py-2 px-2 my-2">
                            <div class="w-25 p-3 mw-200">
                                <img width="200" src=""" + str(i['Thumbnail']) + """>
                            </div>
                            <div class="w-75 p-3 my-auto mx-auto">
                                <a target="_blank" href=""" + str(i['URL']) + """>""" + str(i['Title']) + """</a><br>
                                Score: """ + str(i['Score']) + """
                            </div>
                    </div>""")
            else:
                if debug: d('final is None')
                x('<p>Not found!</p>')
            x('</div>')

        #x('<p>Path: ' + self.path + '</p>')
        #x('<p>Headers: ' + '<br>'.join(self.headers) + '</p>')

        if debug: d(tokens)
        if debug: d(final)
        if debug: d(results)

        x(index_end)

if __name__ == '__main__':
    try:
        server = HTTPServer(('', 8080), GetHandler)
        print('Starting server, use <Ctrl + F2> to stop')
        server.serve_forever()
    except KeyboardInterrupt:
        server.socket.close()