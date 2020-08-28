# Imports
import numpy as np
from PIL import Image
from wordcloud import WordCloud, ImageColorGenerator
import matplotlib.pyplot as plt

import re
#spark
from pyspark.sql import SparkSession

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

dfPandas['transcription']=dfPandas['transcription'].apply(lambda f:remove_tags(f))
dfPandas['transcription']=dfPandas['transcription'].apply(lambda f:to_lower(f))
dfPandas['transcription']=dfPandas['transcription'].apply(lambda f:remove_special_characters(f))
dfPandas['transcription']=dfPandas['transcription'].replace(r'\n','',regex=True)
dfPandas['transcription'].replace('',np.nan,inplace=True)
dfPandas.dropna(subset=['transcription'],inplace=True)
print(dfPandas)

from nltk.corpus import stopwords

stop=stopwords.words('english')
dfPandas['transcription'].apply(lambda x:[item for item in x if item not in stop])

print(dfPandas)

char_mask=np.array(Image.open("/Users/daniel/LocalFiles for TFM/youtubeProjectTFM/image/youtube.jpeg"))

wc = WordCloud(background_color="white",max_words=200, width=500,height=500,
               mask=char_mask, random_state=1).generate(str(dfPandas['transcription']))
plt.figure(figsize=(8, 8), facecolor=None)
plt.imshow(wc)
plt.axis("off")
plt.tight_layout(pad=0)

plt.show()