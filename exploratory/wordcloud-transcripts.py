# Imports
import numpy as np
from PIL import Image
from wordcloud import WordCloud, ImageColorGenerator
import matplotlib.pyplot as plt

import re
#spark
from pyspark.sql import SparkSession
import nltk
import ssl
try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context
nltk.download('words')

spark = (
  SparkSession
    .builder
    .appName("y8m")
    .master("local[*]")
    .getOrCreate()
)

df_transcriptions_raw = spark.read \
  .text("hdfs://localhost:9000/user/daniel/transcripts/*", wholetext=True) \
  .withColumnRenamed("value", "transcription") \

dfPandas = df_transcriptions_raw.toPandas()

#dfPandas.to_csv('wordcloud.csv')

#Cleanup Data
def remove_tags(text):
  result = re.sub('<[^<]+?>','',text)
  return result

def to_lower(text):
  result = text.lower()
  return result

def remove_special_characters(text):
  pattern = r'[^a-zA-Z0-9\s]'
  result=re.sub(pattern, '', text)
  return result

def remove_non_english_words(text):
  words = set(nltk.corpus.words.words())
  result = " ".join(w for w in nltk.wordpunct_tokenize(text) if w.lower() in words or not w.isalpha())
  return result

def clean_text(text):
  text = re.sub(r"I&#39;m", " I am ", text)
  text = re.sub(r"what&#39;is", " what is ", text)
  text = re.sub(r"can&#39;t", " can not ", text)
  text = re.sub(r"that&#39;s", " that is ", text)
  text = re.sub(r"we&#39;re", " we are ", text)
  text = re.sub(r"you&#39;re", " you are ", text)
  text = re.sub(r"he&#39;s", " he is ", text)
  text = re.sub(r"she&#39;s", " she is ", text)
  text = re.sub(r"don&#39;t", " do not ", text)
  text = re.sub(r"didn&#39;t", " did not ", text)
  text = re.sub(r"couldn&#39;t", " could not ", text)
  text = re.sub(r"wouldn&#39;t", " would not ", text)
  text = re.sub(r"isn&#39;t", " is not ", text)
  text = re.sub(r"aren&#39;t", " are not ", text)
  text = re.sub(r"it&#39;s", " it is ", text)
  text = re.sub(r"It&#39;s", " It is ", text)
  text = re.sub(r"there&#39;s", " there is ", text)
  text = re.sub(r"they&#39;re", " they are ", text)
  text = re.sub(r"&quot;", " ", text)
  text = re.sub('\n', ' ', text)
  text = re.sub('\s+', ' ', text)
  text = re.sub(r'[^\w\s]', '', text)
  return text

dfPandas['transcription']=dfPandas['transcription'].apply(lambda f:remove_tags(f))
dfPandas['transcription']=dfPandas['transcription'].apply(lambda f:to_lower(f))
dfPandas['transcription']=dfPandas['transcription'].apply(lambda f:remove_special_characters(f))
dfPandas['transcription']=dfPandas['transcription'].apply(lambda f:remove_non_english_words(f))
dfPandas['transcription']=dfPandas['transcription'].apply(lambda f:clean_text(f))
dfPandas['transcription'].replace('',np.nan,inplace=True)
dfPandas.dropna(subset=['transcription'],inplace=True)

from nltk.corpus import stopwords

stop=stopwords.words('english')
dfPandas['transcription'].apply(lambda x:[item for item in x if item not in stop])

print(dfPandas)

import requests
from io import BytesIO

url = "https://havecamerawilltravel.com/photographer/files/2020/01/youtube-logo-new.jpg"
response = requests.get(url)
img = Image.open(BytesIO(response.content))
mask=np.array(img)

#char_mask=np.array(Image.open("/Users/daniel/LocalFiles for TFM/youtubeProjectTFM/image/youtube.jpeg"))
char_mask=np.array(img)

wc = WordCloud(background_color="white",max_words=200, width=500,height=500,
               mask=char_mask, random_state=1).generate(str(dfPandas['transcription']))
plt.figure(figsize=(8, 8), facecolor=None)
plt.imshow(wc)
plt.axis("off")
plt.tight_layout(pad=0)

plt.show()