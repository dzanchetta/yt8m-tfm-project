# Imports
import subprocess
import sys
#function to install packages
def install(package):
  subprocess.check_call([sys.executable, "-m", "pip", "install", package])

import string
import re
import math
from datetime import date
import json

import pip
from dateutil.parser import parse

#for os
import os

#for files
from pathlib import Path

#for import
import importlib

#language detection
import fasttext

#if Path("lid.176.ftz").is_file() == False:
#  !wget -O lid.176.ftz https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz
import wget
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
wget.download('https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.ftz')

#contractions
#if importlib.util.find_spec("contractions") is None:
#  !pip install contractions --upgrade
import contractions

#emoji
#if importlib.util.find_spec("emoji") is None:
#  !pip install emoji --upgrade
import emoji

#emoticons
#if importlib.util.find_spec("emot") is None:
#  !pip install emot --upgrade
from emot.emo_unicode import EMOTICONS

#number to words
#if importlib.util.find_spec("num2words") is None:
#  !pip install num2words --upgrade
from num2words import num2words

#remove accents
#if importlib.util.find_spec("unidecode") is None:
#  !pip install unidecode --upgrade
from unidecode import unidecode

#spark
from pyspark.sql import SparkSession
#from pyspark.sql.functions import input_file_name, udf, lower, concat, regexp_replace, col, lit, concat_ws, trim, explode, map_from_arrays, to_date, explode_outer, collect_list, flatten, collect_set, sum, count, monotonically_increasing_id
from pyspark.sql.functions import input_file_name, udf, concat, regexp_replace, concat_ws, explode, map_from_arrays, to_date, explode_outer, flatten, monotonically_increasing_id
'''
  In order to use the functions lower, col, lit, trim, collect_list, collect_set, sum and count, the workaround is to call it through the imported functions
'''
from pyspark.sql import functions as f
from pyspark.ml.feature import CountVectorizer, IDF, Tokenizer, StopWordsRemover, NGram, CountVectorizerModel
from pyspark.sql.types import StringType, ArrayType, IntegerType, DoubleType, Row

#Global Variables
#punctuation
punctuation_table = "[{0}]".format(re.escape(string.punctuation))

#fasttext model
#fasttext_model = fasttext.load_model('lid.176.ftz')

#slang dictionary
#https://www.kaggle.com/nmaguette/up-to-date-list-of-slangs-for-text-preprocessing

SLANGS = {
"$" : " dollar ", "€":"euro", "4ao" : "for adults only", "a.m" : "before midday", "a3" : "anytime anywhere anyplace", "aamof" : "as a matter of fact", "acct" : "account", "adih" : "another day in hell", "afaic" : "as far as i am concerned", "afaict" : "as far as i can tell", "afaik" : "as far as i know", "afair" : "as far as i remember", "afk" : "away from keyboard", "app" : "application", "approx" : "approximately", "apps" : "applications", "asap" : "as soon as possible", "asl" : "age, sex, location", "atk" : "at the keyboard", "ave." : "avenue", "aymm" : "are you my mother", "ayor" : "at your own risk",
"b&b" : "bed and breakfast", "b+b" : "bed and breakfast", "b.c" : "before christ", "b2b" : "business to business", "b2c" : "business to customer", "b4" : "before", "b4n" : "bye for now", "b@u" : "back at you", "bae" : "before anyone else", "bak" : "back at keyboard", "bbbg" : "bye bye be good", "bbc" : "british broadcasting corporation", "bbias" : "be back in a second", "bbl" : "be back later", "bbs" : "be back soon", "be4" : "before", "bfn" : "bye for now", "blvd" : "boulevard", "bout" : "about", "brb" : "be right back", "bros" : "brothers", "brt" : "be right there", "bsaaw" : "big smile and a wink", "btw" : "by the way", "bwl" : "bursting with laughter", "c/o" : "care of", "cet" : "central european time", "cf" : "compare", "cia" : "central intelligence agency", "csl" : "can not stop laughing", "cu" : "see you", "cul8r" : "see you later", "cv" : "curriculum vitae", "cwot" : "complete waste of time", "cya" : "see you", "cyt" : "see you tomorrow", "dae" : "does anyone else", "dbmib" : "do not bother me i am busy", "diy" : "do it yourself", "dm" : "direct message", "dwh" : "during work hours", "e123" : "easy as one two three", "eet" : "eastern european time", "eg" : "example", "embm" : "early morning business meeting", "encl" : "enclosed", "encl." : "enclosed", "etc" : "and so on", "faq" : "frequently asked questions", "fawc" : "for anyone who cares", "fb" : "facebook", "fc" : "fingers crossed", "fig" : "figure", "fimh" : "forever in my heart",
"ft." : "feet", "ft" : "featuring", "ftl" : "for the loss", "ftw" : "for the win", "fwiw" : "for what it is worth", "fyi" : "for your information", "g9" : "genius", "gahoy" : "get a hold of yourself", "gal" : "get a life", "gcse" : "general certificate of secondary education", "gfn" : "gone for now", "gg" : "good game", "gl" : "good luck", "glhf" : "good luck have fun", "gmt" : "greenwich mean time", "gmta" : "great minds think alike", "gn" : "good night", "g.o.a.t" : "greatest of all time", "goat" : "greatest of all time", "goi" : "get over it", "gps" : "global positioning system", "gr8" : "great", "gratz" : "congratulations", "gyal" : "girl", "h&c" : "hot and cold", "hp" : "horsepower", "hr" : "hour", "hrh" : "his royal highness", "ht" : "height", "ibrb" : "i will be right back", "ic" : "i see", "icq" : "i seek you", "icymi" : "in case you missed it", "idc" : "i do not care", "idgadf" : "i do not give a damn fuck", "idgaf" : "i do not give a fuck", "idk" : "i do not know", "ie" : "that is", "i.e" : "that is", "ifyp" : "i feel your pain", "IG" : "instagram", "iirc" : "if i remember correctly", "ilu" : "i love you", "ily" : "i love you", "imho" : "in my humble opinion", "imo" : "in my opinion", "imu" : "i miss you", "iow" : "in other words", "irl" : "in real life", "j4f" : "just for fun", "jic" : "just in case", "jk" : "just kidding", "jsyk" : "just so you know", "l8r" : "later", "lb" : "pound", "lbs" : "pounds", "ldr" : "long distance relationship", "lmao" : "laugh my ass off", "lmfao" : "laugh my fucking ass off", "lol" : "laughing out loud", "ltd" : "limited", "ltns" : "long time no see", "m8" : "mate", "mf" : "motherfucker", "mfs" : "motherfuckers", "mfw" : "my face when", "mofo" : "motherfucker", "mph" : "miles per hour", "mr" : "mister", "mrw" : "my reaction when", "ms" : "miss", "mte" : "my thoughts exactly", "nagi" : "not a good idea", "nbc" : "national broadcasting company", "nbd" : "not big deal", "nfs" : "not for sale", "ngl" : "not going to lie", "nhs" : "national health service", "nrn" : "no reply necessary", "nsfl" : "not safe for life", "nsfw" : "not safe for work", "nth" : "nice to have", "nvr" : "never", "nyc" : "new york city", "oc" : "original content", "og" : "original", "ohp" : "overhead projector", "oic" : "oh i see", "omdb" : "over my dead body", "omg" : "oh my god", "omw" : "on my way", "p.a" : "per annum", "p.m" : "after midday", "pm" : "prime minister", "poc" : "people of color", "pov" : "point of view", "pp" : "pages", "ppl" : "people", "prw" : "parents are watching", "ps" : "postscript", "pt" : "point", "ptb" : "please text back", "pto" : "please turn over", "qpsa" : "what happens", #"que pasa", "ratchet" : "rude", "rbtl" : "read between the lines", "rlrt" : "real life retweet",
"rofl" : "rolling on the floor laughing", "roflol" : "rolling on the floor laughing out loud", "rotflmao" : "rolling on the floor laughing my ass off", "rt" : "retweet", "ruok" : "are you ok", "sfw" : "safe for work", "sk8" : "skate", "smh" : "shake my head", "sq" : "square", "srsly" : "seriously",
"ssdd" : "same stuff different day", "tbh" : "to be honest", "tbs" : "tablespooful", "tbsp" : "tablespooful", "tfw" : "that feeling when", "thks" : "thank you", "tho" : "though", "thx" : "thank you", "tia" : "thanks in advance", "til" : "today i learned", "tl;dr" : "too long i did not read", "tldr" : "too long i did not read", "tmb" : "tweet me back", "tntl" : "trying not to laugh", "ttyl" : "talk to you later", "u" : "you", "u2" : "you too", "u4e" : "yours for ever", "utc" : "coordinated universal time", "w/" : "with", "w/o" : "without", "w8" : "wait", "wassup" : "what is up", "wb" : "welcome back", "wtf" : "what the fuck", "wtg" : "way to go", "wtpa" : "where the party at", "wuf" : "where are you from", "wuzup" : "what is up", "wywh" : "wish you were here", "yd" : "yard", "ygtr" : "you got that right", "ynk" : "you never know", "zzz" : "sleeping bored and tired"

#more
,"pls":"please"

}

#Connecting to Spark
#initialize spark
spark = (
  SparkSession
    .builder
    .appName("y8m")
    .master("local[*]")
    .getOrCreate()
)
#broadcast global variables
spark.sparkContext.broadcast(punctuation_table)
spark.sparkContext.broadcast(SLANGS)

#Load datasets from HDFS
#id	channel_id	categories	dislike_count	like_count	fulltitle	upload_date	description	tags	average_rating	duration	view_count
df_videos_raw = spark.read.json("hdfs://localhost:9000/user/daniel/video/*.json") \
  .select("id", "channel_id", "categories", "dislike_count", "like_count", "fulltitle", "upload_date", "description", "average_rating", "duration", "view_count")
#cache
df_videos_raw.cache()

#load transcriptions dataset
#load full xml srv3 format
df_transcriptions_raw = spark.read \
  .text("hdfs://localhost:9000/user/daniel/content/*.en.srv3", wholetext=True) \
  .withColumn("filename", input_file_name()) \
  .withColumn("filename", regexp_replace('filename', '^file:/content/transcriptions/', '')) \
  .withColumn("filename", regexp_replace('filename', '.en.srv3$', '')) \
  .withColumnRenamed("value", "transcription") \
  .withColumnRenamed("filename", "id")
#cache
df_transcriptions_raw.cache()
df_transcriptions_raw.show(5)
# join dataset
df_all = df_videos_raw.join(df_transcriptions_raw, 'id', 'left')

#cache
df_all.cache()

#show
df_all.show()

#get final df with renamed columns
df_all = (
  df_all
    #change date format
    .withColumn("Date", to_date("upload_date", 'yyyyMMdd'))
    .drop("upload_date")
    #get first category - raw data is just one category
    .withColumn("Category", f.col("categories")[0])
    .drop("categories")
    .withColumnRenamed("id", "Id")
    .withColumnRenamed("channel_id", "ChannelId")
    .withColumnRenamed("dislike_count", "DislikeCount")
    .withColumnRenamed("like_count", "LikeCount")
    .withColumnRenamed("average_rating", "Rating")
    .withColumnRenamed("duration", "Duration")
    .withColumnRenamed("view_count", "ViewCount")
    .withColumnRenamed("fulltitle", "Title")
    .withColumnRenamed("description", "Description")
    .withColumnRenamed("transcription", "Transcription")
)

#cache
df_all.cache()

#Generate PKs
#create video PK
df_video_pk = df_all.select('Id').withColumn("VideoPK", monotonically_increasing_id())
#cache
df_video_pk.cache()
#show
df_video_pk.show()

#create channel PK
df_channel_pk = df_all.select('ChannelId').distinct().withColumn("ChannelPK", monotonically_increasing_id())
#cache
df_channel_pk.cache()
#show
df_channel_pk.show()

#create channel PK
df_category_pk = df_all.select('Category').distinct().withColumn("CategoryPK", monotonically_increasing_id())
#cache
df_category_pk.cache()
#show
df_category_pk.show()

#change dataframes to PK
#video id
df_all = df_all.join(df_video_pk, on='Id').join(df_channel_pk, on='ChannelId').join(df_category_pk, on='Category').drop('Id', 'ChannelId', 'Category')

#cache
df_all.cache()

#Relations
#Video y Channel, Video y Category, Channel y Category
#Video y Channel
df_video_channel_relation = df_all.select('VideoPK', 'ChannelPK')
#cache
df_video_channel_relation.cache()
#show
df_video_channel_relation.show()

#Video y Category
df_video_category_relation = df_all.select('VideoPK', 'CategoryPK')
#cache
df_video_category_relation.cache()
#show
df_video_category_relation.show()

#Channel y Category
df_channel_category_relation = df_all.select('ChannelPK', 'CategoryPK').distinct()
#cache
df_channel_category_relation.cache()
#show
df_channel_category_relation.show()

#Cleanup Data
# filter non english texts
def set_language(text):
  language = '__label__en'
  min_accuracy = 0.4
  max_languages = 3

  fasttext_model = fasttext.load_model('lid.176.ftz')
  lng, acc = fasttext_model.predict(text.replace('\r', ' ').replace('\n', ' '), k=max_languages)

  # the first is english - with any accuracy
  if lng[0] == language:
    return True

  # search for english and accuracy
  for i in range(0, max_languages):
    if lng[i] == language and acc[i] > min_accuracy:
      return True

  return False


udf_set_language = udf(set_language)


def convert_srv3(df, column):
  return (df
          # remove break line
          .withColumn(column, regexp_replace(column, '[\r\n]', ' '))
          # remove html tags
          .withColumn(column, regexp_replace(column, '<[^<]+?>', ''))
          # trim
          .withColumn(column, f.trim(column))
          )


def convert_lower_case(df, column):
  # lower
  return df.withColumn(column, f.lower(column))


def remove_punctuation(df, column):
  # remove string.punctuation - scaped before
  # remove apostrophes
  return df.withColumn(column, regexp_replace(column, punctuation_table, ' '))


def remove_multiple_spaces(df, column):
  # remove multiple_spaces
  return df.withColumn(column, regexp_replace(column, '\s+', ' '))


def remove_start_end_spaces(df, column):
  # remove multiple_spaces
  return df.withColumn(column, f.trim(
    column))  # .withColumn(column, regexp_replace(column, '\s', '*')).withColumn(column, concat(lit('*'), col(column), lit('*')))


def convert_tokens(df, from_column, to_column):
  # tokenize
  tokenizer = Tokenizer(inputCol=from_column, outputCol=to_column)
  return tokenizer.transform(df)


def convert_ngrams(df, column):
  # convert tokens to ngram

  n = 5

  # convert to ngrams 2 to n ngrams
  for i in range(1, n + 1):
    ngram = NGram(n=i, inputCol=column, outputCol='{}_{}'.format(column, i))
    df = ngram.transform(df)

  return df.withColumn(column, concat(*['{}_{}'.format(column, i) for i in range(1, n + 1)])).drop(
    *['{}_{}'.format(column, i) for i in range(1, n + 1)])


#  ngram = NGram(n = n, inputCol = column, outputCol = 'convert_ngrams')
#  df_result = ngram.transform(df)
#  return df_result.drop(column).withColumnRenamed('convert_ngrams', column)

def remove_stop_words(df, column):
  # remove stop words
  remover = StopWordsRemover(inputCol=column, outputCol='remove_stop_words')
  return remover.transform(df).drop(column).withColumnRenamed('remove_stop_words', column)


def remove_not_alphabetic(tokens):
  # isalpha remove words with numbers
  # return [w for w in tokens if w.isalpha()]
  # isalnum allow words and numbers
  return [w for w in tokens if w.isalnum()]


# udf_remove_not_alphabetic = udf(remove_not_alphabetic, ArrayType(StringType()))
udf_remove_not_alphabetic = udf(lambda tokens: [w for w in tokens if w.isalnum()], ArrayType(StringType()))


def remove_only_numbers(tokens):
  return [w for w in tokens if not w.isnumeric()]


# udf_remove_only_numbers = udf(remove_only_numbers, ArrayType(StringType()))
udf_remove_only_numbers = udf(lambda tokens: [w for w in tokens if not w.isnumeric()], ArrayType(StringType()))


def remove_only_spaces(tokens):
  return [w for w in tokens if not w.isspace()]


# udf_remove_only_spaces = udf(remove_only_spaces, ArrayType(StringType()))
udf_remove_only_spaces = udf(lambda tokens: [w for w in tokens if not w.isspace()], ArrayType(StringType()))


def remove_small_words(tokens):
  min = 3
  return [w for w in tokens if len(w) > min]


# udf_remove_small_words = udf(remove_small_words, ArrayType(StringType()))
udf_remove_small_words = udf(lambda tokens: [w for w in tokens if len(w) >= 3], ArrayType(StringType()))


def convert_json_array(text):
  return json.loads(text)


# udf_convert_json_array = udf(convert_json_array)
udf_convert_json_array = udf(lambda text: json.loads(text))


def remove_accented_chars(text):
  return unidecode(text)


# udf_remove_accented_chars = udf(remove_accented_chars)
udf_remove_accented_chars = udf(lambda text: unidecode(text))


def remove_html(df, column):
  return df.withColumn(column, regexp_replace(column, '<[^<]+?>', ''))


def convert_imoji(text):
  return emoji.demojize(text, delimiters=("", ""))


# udf_convert_imoji = udf(convert_imoji)
udf_convert_imoji = udf(lambda text: emoji.demojize(text, delimiters=("", "")))


def convert_emoticons(text):
  for emot, desc in EMOTICONS.items():
    text = re.sub(u'(' + emot + ')', desc, text)
  return text


udf_convert_emoticons = udf(convert_emoticons)


def convert_contractions(text):
  return contractions.fix(text)


# udf_convert_contractions = udf(convert_contractions)
udf_convert_contractions = udf(lambda text: contractions.fix(text))


def convert_numbers_to_text(text):
  return ' '.join([num2words(w) if w.isdigit() else w for w in text.split()])


# udf_convert_numbers_to_text = udf(convert_numbers_to_text, ArrayType(StringType()))
udf_convert_numbers_to_text = udf(lambda text: ' '.join([num2words(w) if w.isdigit() else w for w in text.split()]),
                                  ArrayType(StringType()))


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


udf_convert_date_to_text = udf(convert_date_to_text, ArrayType(StringType()))


def convert_slang(text):
  return ' '.join([SLANGS[w] if w in SLANGS.keys() else w for w in text.split()])


# udf_convert_slang = udf(convert_slang, ArrayType(StringType()))
udf_convert_slang = udf(lambda text: ' '.join([SLANGS[w] if w in SLANGS.keys() else w for w in text.split()]),
                        ArrayType(StringType()))


def convert_elongation(df, column):
  return df.withColumn(column, regexp_replace(column, '(.)\1+', '\1\1'))


def convert_null_zero(df, column):
  return df.na.fill(0, column)

def preprocess(df, column, token_column):
  df = convert_lower_case(df, column)
  df = remove_html(df, column)

  df = (
      df
      #emoji
      .withColumn(column, udf_convert_imoji(f.col(column)))
      #emoticons
      .withColumn(column, udf_convert_emoticons(f.col(column)))
      #contractions
      .withColumn(column, udf_convert_contractions(f.col(column)))
      #slang
      .withColumn(column, udf_convert_slang(f.col(column)))
      #date_to_text
      .withColumn(column, udf_convert_date_to_text(f.col(column)))
      #numbers_to_text
      .withColumn(column, udf_convert_numbers_to_text(f.col(column)))
      #accented_chars
      .withColumn(column, udf_remove_accented_chars(f.col(column)))
  )

  #elongation
  df = convert_elongation(df, column)
  #puntuation
  df = remove_punctuation(df, column)
  #multiple_spaces
  df = remove_multiple_spaces(df, column)
  #start_end_spaces
  df = remove_start_end_spaces(df, column)

  #tokenizer
  df = convert_tokens(df, column, token_column)
  #stop words
  df = remove_stop_words(df, token_column)
  #small_words
  df = df.withColumn(token_column, udf_remove_small_words(f.col(token_column)))

  df = (
      df
      #not_alphabetic
      .withColumn(token_column, udf_remove_not_alphabetic(f.col(token_column)))
      #only_numbers
      .withColumn(token_column, udf_remove_only_numbers(f.col(token_column)))
      #only_spaces
      .withColumn(token_column, udf_remove_only_spaces(f.col(token_column)))
  )

  #convert to ngrams
  df = convert_ngrams(df, token_column)

  return df

#filter english text
df = df_all.withColumn('isEnglish', udf_set_language(concat_ws(' ', f.col('Title'), f.col('Description')))).filter('isEnglish == True').drop('isEnglish')

#Transcription format
df = convert_srv3(df, 'Transcription')

#numbers
#convert null to 0
df = convert_null_zero(df, 'DislikeCount')
df = convert_null_zero(df, 'LikeCount')
df = convert_null_zero(df, 'Rating')
df = convert_null_zero(df, 'Duration')
df = convert_null_zero(df, 'ViewCount')

#texts
#df = preprocess(df, 'Title', 'TitleWords')
#df = preprocess(df, 'Description', 'DescriptionWords')
#df = preprocess(df, 'Transcription', 'TranscriptionWords')

#text to NLP
df = df.withColumn("Text", concat_ws(' ', f.col("Title"), f.col("Description"), f.col("Transcription"))).drop("Title", "Description", "Transcription")
df = preprocess(df, 'Text', 'TextWords')
df = df.drop("Text")

#drop unused columns
df = df.drop("CategoryPK")

#cache results
df.cache()

#df by channel id
df2 = (
  df
  .select("ChannelPK", "DislikeCount", "LikeCount", "Rating", "Duration", "ViewCount", "TextWords")
  #group by channe id
  .groupBy("ChannelPK")
  #calculate distinct Category
  .agg(
      #calculate DislikeCount
      sum("DislikeCount")
      #calculate LikeCount
      ,sum("LikeCount")
      #calculate Rating
      ,sum("Rating")
      #calculate Duration
      ,sum("Duration")
      #calculate ViewCount
      ,sum("ViewCount")
      #calculate textwords
      ,flatten(f.collect_list("TextWords"))
      #count videos
      ,f.count("*")
  )
  #rename
  .withColumnRenamed("sum(DislikeCount)", "DislikeCount")
  .withColumnRenamed('sum(LikeCount)', "LikeCount")
  .withColumnRenamed("sum(Rating)", "Rating")
  .withColumnRenamed("sum(Duration)", "Duration")
  .withColumnRenamed("sum(ViewCount)", "ViewCount")
  .withColumnRenamed("flatten(collect_list(TextWords))", "TextWords")
  .withColumnRenamed("count(1)", "VideoCount")
)

#cache results
df2.cache()

#STORE IN A INTERMEDIARY DB (MONGODB, HBASE, ...)