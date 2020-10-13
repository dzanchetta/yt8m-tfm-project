import string
import re

#contractions
#import contractions

#emoji
import emoji

#emoticons
from emot.emo_unicode import EMOTICONS

#number to words
from num2words import num2words

#remove accents
from unidecode import unidecode

from dateutil.parser import parse

#import sqlite3

#punctuation
punctuation_table = "[{0}]".format(re.escape(string.punctuation))

SLANGS = {
"$" : " dollar ", "€":"euro", "4ao" : "for adults only", "a.m" : "before midday", "a3" : "anytime anywhere anyplace", "aamof" : "as a matter of fact", "acct" : "account", "adih" : "another day in hell", "afaic" : "as far as i am concerned", "afaict" : "as far as i can tell", "afaik" : "as far as i know", "afair" : "as far as i remember", "afk" : "away from keyboard", "app" : "application", "approx" : "approximately", "apps" : "applications", "asap" : "as soon as possible", "asl" : "age, sex, location", "atk" : "at the keyboard", "ave." : "avenue", "aymm" : "are you my mother", "ayor" : "at your own risk", 
"b&b" : "bed and breakfast", "b+b" : "bed and breakfast", "b.c" : "before christ", "b2b" : "business to business", "b2c" : "business to customer", "b4" : "before", "b4n" : "bye for now", "b@u" : "back at you", "bae" : "before anyone else", "bak" : "back at keyboard", "bbbg" : "bye bye be good", "bbc" : "british broadcasting corporation", "bbias" : "be back in a second", "bbl" : "be back later", "bbs" : "be back soon", "be4" : "before", "bfn" : "bye for now", "blvd" : "boulevard", "bout" : "about", "brb" : "be right back", "bros" : "brothers", "brt" : "be right there", "bsaaw" : "big smile and a wink", "btw" : "by the way", "bwl" : "bursting with laughter", "c/o" : "care of", "cet" : "central european time", "cf" : "compare", "cia" : "central intelligence agency", "csl" : "can not stop laughing", "cu" : "see you", "cul8r" : "see you later", "cv" : "curriculum vitae", "cwot" : "complete waste of time", "cya" : "see you", "cyt" : "see you tomorrow", "dae" : "does anyone else", "dbmib" : "do not bother me i am busy", "diy" : "do it yourself", "dm" : "direct message", "dwh" : "during work hours", "e123" : "easy as one two three", "eet" : "eastern european time", "eg" : "example", "embm" : "early morning business meeting", "encl" : "enclosed", "encl." : "enclosed", "etc" : "and so on", "faq" : "frequently asked questions", "fawc" : "for anyone who cares", "fb" : "facebook", "fc" : "fingers crossed", "fig" : "figure", "fimh" : "forever in my heart", 
"ft." : "feet", "ft" : "featuring", "ftl" : "for the loss", "ftw" : "for the win", "fwiw" : "for what it is worth", "fyi" : "for your information", "g9" : "genius", "gahoy" : "get a hold of yourself", "gal" : "get a life", "gcse" : "general certificate of secondary education", "gfn" : "gone for now", "gg" : "good game", "gl" : "good luck", "glhf" : "good luck have fun", "gmt" : "greenwich mean time", "gmta" : "great minds think alike", "gn" : "good night", "g.o.a.t" : "greatest of all time", "goat" : "greatest of all time", "goi" : "get over it", "gps" : "global positioning system", "gr8" : "great", "gratz" : "congratulations", "gyal" : "girl", "h&c" : "hot and cold", "hp" : "horsepower", "hr" : "hour", "hrh" : "his royal highness", "ht" : "height", "ibrb" : "i will be right back", "ic" : "i see", "icq" : "i seek you", "icymi" : "in case you missed it", "idc" : "i do not care", "idgadf" : "i do not give a damn fuck", "idgaf" : "i do not give a fuck", "idk" : "i do not know", "ie" : "that is", "i.e" : "that is", "ifyp" : "i feel your pain", "IG" : "instagram", "iirc" : "if i remember correctly", "ilu" : "i love you", "ily" : "i love you", "imho" : "in my humble opinion", "imo" : "in my opinion", "imu" : "i miss you", "iow" : "in other words", "irl" : "in real life", "j4f" : "just for fun", "jic" : "just in case", "jk" : "just kidding", "jsyk" : "just so you know", "l8r" : "later", "lb" : "pound", "lbs" : "pounds", "ldr" : "long distance relationship", "lmao" : "laugh my ass off", "lmfao" : "laugh my fucking ass off", "lol" : "laughing out loud", "ltd" : "limited", "ltns" : "long time no see", "m8" : "mate", "mf" : "motherfucker", "mfs" : "motherfuckers", "mfw" : "my face when", "mofo" : "motherfucker", "mph" : "miles per hour", "mr" : "mister", "mrw" : "my reaction when", "ms" : "miss", "mte" : "my thoughts exactly", "nagi" : "not a good idea", "nbc" : "national broadcasting company", "nbd" : "not big deal", "nfs" : "not for sale", "ngl" : "not going to lie", "nhs" : "national health service", "nrn" : "no reply necessary", "nsfl" : "not safe for life", "nsfw" : "not safe for work", "nth" : "nice to have", "nvr" : "never", "nyc" : "new york city", "oc" : "original content", "og" : "original", "ohp" : "overhead projector", "oic" : "oh i see", "omdb" : "over my dead body", "omg" : "oh my god", "omw" : "on my way", "p.a" : "per annum", "p.m" : "after midday", "pm" : "prime minister", "poc" : "people of color", "pov" : "point of view", "pp" : "pages", "ppl" : "people", "prw" : "parents are watching", "ps" : "postscript", "pt" : "point", "ptb" : "please text back", "pto" : "please turn over", "qpsa" : "what happens", #"que pasa", "ratchet" : "rude", "rbtl" : "read between the lines", "rlrt" : "real life retweet", 
"rofl" : "rolling on the floor laughing", "roflol" : "rolling on the floor laughing out loud", "rotflmao" : "rolling on the floor laughing my ass off", "rt" : "retweet", "ruok" : "are you ok", "sfw" : "safe for work", "sk8" : "skate", "smh" : "shake my head", "sq" : "square", "srsly" : "seriously", 
"ssdd" : "same stuff different day", "tbh" : "to be honest", "tbs" : "tablespooful", "tbsp" : "tablespooful", "tfw" : "that feeling when", "thks" : "thank you", "tho" : "though", "thx" : "thank you", "tia" : "thanks in advance", "til" : "today i learned", "tl;dr" : "too long i did not read", "tldr" : "too long i did not read", "tmb" : "tweet me back", "tntl" : "trying not to laugh", "ttyl" : "talk to you later", "u" : "you", "u2" : "you too", "u4e" : "yours for ever", "utc" : "coordinated universal time", "w/" : "with", "w/o" : "without", "w8" : "wait", "wassup" : "what is up", "wb" : "welcome back", "wtf" : "what the fuck", "wtg" : "way to go", "wtpa" : "where the party at", "wuf" : "where are you from", "wuzup" : "what is up", "wywh" : "wish you were here", "yd" : "yard", "ygtr" : "you got that right", "ynk" : "you never know", "zzz" : "sleeping bored and tired"

#more
,"pls":"please"

}

STOP_WORDS = ["i", "me", "my", "myself", "we", "our", "ours", "ourselves", "you", "your", "yours", "yourself", "yourselves", "he", "him", "his", "himself", "she", "her", "hers", "herself", "it", "its", "itself", "they", "them", "their", "theirs", "themselves", "what", "which", "who", "whom", "this", "that", "these", "those", "am", "is", "are", "was", "were", "be", "been", "being", "have", "has", "had", "having", "do", "does", "did", "doing", "a", "an", "the", "and", "but", "if", "or", "because", "as", "until", "while", "of", "at", "by", "for", "with", "about", "against", "between", "into", "through", "during", "before", "after", "above", "below", "to", "from", "up", "down", "in", "out", "on", "off", "over", "under", "again", "further", "then", "once", "here", "there", "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most", "other", "some", "such", "no", "nor", "not", "only", "own", "same", "so", "than", "too", "very", "s", "t", "can", "will", "just", "don", "should", "now", "i'll", "you'll", "he'll", "she'll", "we'll", "they'll", "i'd", "you'd", "he'd", "she'd", "we'd", "they'd", "i'm", "you're", "he's", "she's", "it's", "we're", "they're", "i've", "we've", "you've", "they've", "isn't", "aren't", "wasn't", "weren't", "haven't", "hasn't", "hadn't", "don't", "doesn't", "didn't", "won't", "wouldn't", "shan't", "shouldn't", "mustn't", "can't", "couldn't", "cannot", "could", "here's", "how's", "let's", "ought", "that's", "there's", "what's", "when's", "where's", "who's", "why's", "would"]

def convert_lower_case(text):
  return text.lower()

def remove_punctuation(text):
  return re.sub(punctuation_table, ' ', text)

def remove_multiple_spaces(text):
  return re.sub('\s+', ' ', text)

def remove_start_end_spaces(text):
  return text.strip()

def convert_tokens(text):
  return text.split()

def convert_ngrams(tokens):
  #convert tokens to ngram
  grams = []
  for n in range(1, 6):
    grams.append([' '.join(tokens[i:i + n]) for i in range(len(tokens) - n + 1)])
  return [item for sublist in grams for item in sublist]

def remove_not_alphabetic(tokens):
  return [w for w in tokens if w.isalnum()]

def remove_only_numbers(tokens):
  return [w for w in tokens if not w.isnumeric()]

def remove_only_spaces(tokens):
  return [w for w in tokens if not w.isspace()]

def remove_small_words(tokens):
  return [w for w in tokens if len(w) >= 3]

def remove_accented_chars(text):
  return unidecode(text)

def remove_html(text):
  return re.sub('<[^<]+?>', ' ', text)

def convert_imoji(text):
  return emoji.demojize(text, delimiters=("", ""))

def convert_emoticons(text):
  for emot, desc in EMOTICONS.items():
    text = re.sub(u'(' + emot + ')', desc, text)
  return text

def convert_contractions(text):
  return text#contractions.fix(text)

def convert_numbers_to_text(text):
  return ' '.join([num2words(w) if w.isdigit() else w for w in text.split()])

def convert_date_to_text(text):
  result = []
  for token in text.split():
    if not re.match('[0-9]', token):
      result.append(token)
    else:
      try:
        found = parse(token, fuzzy=False)
        result.append(found.strftime('%d %B %Y'))
      except ValueError:
        result.append(token)

  return ' '.join(result)

def convert_slang(text):
  return ' '.join([SLANGS[w] if w in SLANGS.keys() else w for w in text.split()])

def convert_elongation(text):
  return re.sub('(.)\1+', '\1\1', text)

def remove_stop_words(tokens):
  return [x for x in tokens if x not in STOP_WORDS]

def preprocess(text):
  text = convert_lower_case(text)
  text = remove_html(text)

  #emoji
  text = convert_imoji(text)
  #emoticons
  text = convert_emoticons(text)
  #contractions
  text = convert_contractions(text)
  #slang
  text = convert_slang(text)
  #date_to_text
  text = convert_date_to_text(text)
  #numbers_to_text
  text = convert_numbers_to_text(text)
  #accented_chars
  text = remove_accented_chars(text)

  #elongation
  text = convert_elongation(text)
  #puntuation
  text = remove_punctuation(text)
  #multiple_spaces
  text = remove_multiple_spaces(text)
  #start_end_spaces
  text = remove_start_end_spaces(text)

  #tokenizer
  tokens = convert_tokens(text)
  #stop words
  tokens = remove_stop_words(tokens)
  #small_words
  tokens = remove_small_words(tokens)

  #not_alphabetic
  tokens = remove_not_alphabetic(tokens)
  #only_numbers
  tokens = remove_only_numbers(tokens)
  #only_spaces
  tokens = remove_only_spaces(tokens)

  #convert to ngrams
  tokens = convert_ngrams(tokens)

  return tokens

def get_sql(tokens):
  return """SELECT A.VideoPK, A.ChannelPK, A.ScoreVideo, A.ScoreChannel
    ,V.VideoId
    ,V.DislikeCount AS DislikeCountVideo
    ,V.LikeCount AS LikeCountVideo
    ,V.Date AS DateVideo
    ,V.Rating AS RatingVideo
    ,V.Duration AS DurationVideo
    ,V.ViewCount AS ViewCountVideo
    ,V.Title AS TitleVideo
    ,V.SentimentPositive AS SentimentPositiveVideo
    ,V.SentimentNegative AS SentimentNegativeVideo
    ,C.DislikeCount AS DislikeCountChannel
    ,C.LikeCount AS LikeCountChannel
    ,C.Rating AS RatingChannel
    ,C.Duration AS DurationChannel
    ,C.ViewCount AS ViewCountChannel
    ,(
        SELECT GROUP_CONCAT(X.Word)
        FROM
            (
                SELECT D1.Word
                FROM dictionary D1, word_by_video WV1
                WHERE
                    D1.WordPK = WV1.WordPK
                    AND WV1.VideoPK = A.VideoPK
                    AND D1.NGram = 1
                ORDER BY WV1.Score DESC, D1.WordPK
                LIMIT 20
            ) X
    ) AS WordsVideo
    ,(
        SELECT GROUP_CONCAT(X.Score)
        FROM
            (
                SELECT WV1.Score
                FROM dictionary D1, word_by_video WV1
                WHERE
                    D1.WordPK = WV1.WordPK
                    AND WV1.VideoPK = A.VideoPK
                    AND D1.NGram = 1
                ORDER BY WV1.Score DESC, D1.WordPK
                LIMIT 20
            ) X
    ) AS WordsScoreVideo
    ,(
        SELECT GROUP_CONCAT(X.Word)
        FROM
            (
                SELECT D1.Word
                FROM dictionary D1, word_by_video WV1, amazon_words AW1
                WHERE
                    D1.WordPK = WV1.WordPK
                    AND WV1.VideoPK = A.VideoPK
                    AND D1.NGram = 1
                    AND D1.WordPK = AW1.WordPK
                ORDER BY WV1.Score DESC, D1.WordPK
                LIMIT 20
            ) X
    ) AS WordsAmazonVideo
    ,(
        SELECT GROUP_CONCAT(X.ASIN)
        FROM
            (
                SELECT AW1.ASIN
                FROM dictionary D1, word_by_video WV1, amazon_words AW1
                WHERE
                    D1.WordPK = WV1.WordPK
                    AND WV1.VideoPK = A.VideoPK
                    AND D1.NGram = 1
                    AND D1.WordPK = AW1.WordPK
                ORDER BY WV1.Score DESC, D1.WordPK
                LIMIT 20
            ) X
    ) AS WordsAmazonASINVideo
FROM
    (
        SELECT WV.VideoPK, WV.ChannelPK, SUM(WC.Score) AS ScoreChannel, SUM(WV.Score) AS ScoreVideo
        FROM
            dictionary D
            ,word_by_video WV
            ,word_by_channel WC
        WHERE
            D.Word IN ({seq})
            AND D.WordPK = WV.WordPK
            AND WV.WordPK = WC.WordPK
            AND WV.ChannelPK = WC.ChannelPK
        GROUP BY
            WV.VideoPK, WV.ChannelPK
    ) A
    ,video V
    ,channel C
WHERE
    A.VideoPK = V.VideoPK
    AND A.ChannelPK = C.ChannelPK""".format(seq = ','.join(['?']*len(tokens)))

def save_search(conn,search,result,date):
  sql = '''INSERT INTO search_content(search_sentence,result,search_date) VALUES (?,?,?)'''
  cur = conn.cursor()
  cur.execute(sql,search,result,date)
  conn.commit()
  return cur.lastrowid

def calculate_score(rows):
  result = []

  max_ScoreVideo = max(rows, key='ScoreVideo')
  max_ScoreChannel = max(rows, key='ScoreChannel')

  for row in rows:
    #Words
    try: words = [list(x) for x in zip(row['WordsVideo'].split(','), row['WordsScoreVideo'].split(','))]
    except: words = []

    #Amazon Words
    try: amazon = [list(x) for x in zip(row['WordsAmazonVideo'].split(','), row['WordsAmazonASINVideo'].split(','))]
    except: amazon = []

    video = {'VideoPK': row['VideoPK'], 'Score': 0.0, 'Title': row['TitleVideo'], 'Thumbnail': row['ThumbnailVideo'], 'URL': row['URLVideo'], 'AmazonWords': amazon, 'WordsCloud': words}
    try:
      video['Score'] = (
      (row['ScoreVideo'] / max_ScoreVideo * 0.5 )
      + (row['ScoreChannel'] / max_ScoreChannel * 0.25)
      + (row['SentimentPositiveVideo'] - (row['SentimentNegativeVideo'] * 0.10)))
    except:
      None

    video['Thumbnail'] = 'https://i.ytimg.com/vi/' + row['VideoId'] + '/hqdefault.jpg'
    video['URL'] = 'http://www.youtube.com/watch?v=' + row['VideoId']

    result.append(video)
  return sorted(result, key=lambda k: k['Score'], reverse=True)