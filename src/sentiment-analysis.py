'''Inspiraton source: https://github.com/spotify2junkie/youtube'''
import httplib2
import os
import sys
import nltk
''' 
    The try / catch below is an exception to an SSL issue while downloading the vader_lexicon.
    Workaround found here: https://github.com/gunthercox/ChatterBot/issues/930
'''
import ssl
import re

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context
nltk.download('vader_lexicon')
import csv #TO BE REMOVED

from nltk.sentiment.vader import SentimentIntensityAnalyzer
from googleapiclient.discovery import build_from_document
from googleapiclient.errors import HttpError
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client.tools import argparser,run_flow

CLIENT_SECRETS_FILE = "client_secrets.json"

YOUTUBE_DISCOVERY_FILE="youtube-v3-discoverydocument.json"
YOUTUBE_READ_WRITE_SSL_SCOPE = "https://www.googleapis.com/auth/youtube.force-ssl"
YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"

MISSING_CLIENT_SECRETS_MESSAGE = os.path.abspath(os.path.join(os.path.dirname(__file__),
                                   CLIENT_SECRETS_FILE))

def get_authenticated_service(args):
    flow = flow_from_clientsecrets(os.path.abspath(os.path.join(
      os.path.dirname(__file__),CLIENT_SECRETS_FILE)),scope=YOUTUBE_READ_WRITE_SSL_SCOPE,
                                   message=MISSING_CLIENT_SECRETS_MESSAGE)
    storage = Storage("%s-oauth2.json" % sys.argv[0])
    credentials = storage.get()
    if credentials is None or credentials.invalid:
        credentials=run_flow(flow, storage, args)
    with open(os.path.abspath(os.path.join(os.path.dirname(__file__),YOUTUBE_DISCOVERY_FILE)),"r") as f:
        doc = f.read()
        return build_from_document(doc,http=credentials.authorize(httplib2.Http()))

def clean_text(text):
    """
        Disclaimer:
        This is a ongoing cleaning function.
        The transformations below are proposed at first, but more cleasing might be included.
    """
    text = text.lower()
    text = re.sub(r"what's", "what is ", text)
    text = re.sub(r"\'s", " ", text)
    text = re.sub(r"\'ve", " have ", text)
    text = re.sub(r"can't", "can not ", text)
    text = re.sub(r"n't", " not ", text)
    text = re.sub(r"i'm", "i am ", text)
    text = re.sub(r"he's", "he is ", text)
    text = re.sub(r"she's", "she is ", text)
    text = re.sub(r"\'re", " are ", text)
    text = re.sub(r"\'d", " would ", text)
    text = re.sub(r"\'ll", " will ", text)
    text = re.sub(r"\'scuse", " excuse ", text)
    text = re.sub(r"\'don't", " do not ", text)
    text = re.sub(r"\'didn't", " did not ", text)
    text = re.sub('\W', ' ', text)
    text = re.sub('\s+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = text.strip(' ')
    return text

def get_comment_threads(youtube,video_id,comments=[],token=""):
    results=youtube.commentThreads().list(part="snippet",
                                          pageToken=token,
                                          videoId=video_id,
                                          textFormat="plainText",
                                          #Optionally we can leave only 50 comments for performance purposes. It does not change anything in terms of the quota usage for the YouTube API
                                          maxResults=50).execute()
    #Get the object topLevelComment and textDisplay from the API result
    for item in results["items"]:
        comment=item["snippet"]["topLevelComment"]
        text=comment["snippet"]["textDisplay"]
        text_clean=clean_text(text)
        comments.append(text_clean)

    #Recursive call to this method if there is more comments. Otherwise, return the list of comments appended
    if "nextPageToken" in results:
        return get_comment_threads(youtube, video_id, comments, results["nextPageToken"])
    else:
        return comments

def count_comments_per_sentiment(video_comment_threads):
    positive=0
    negative=0
    neutral=0
    for i in range(0, len(video_comment_threads)):
        comment = video_comment_threads[i]
        score = sia.polarity_scores(comment)
        if score["compound"] >= 0.05:
            positive += 1
        elif score["compound"] > -0.05 and score["compound"] < 0.05:
            neutral += 1
        else:
            negative +=1
    return positive,neutral,negative

if __name__=="__main__":
    argparser.add_argument("--videoid", help="Required; ID for video for which the comment will be inserted.")
    args=argparser.parse_args()
    if not args.videoid:
        exit("Please specify videoid")

    youtube=get_authenticated_service(args)
    try:
        video_comment_threads=get_comment_threads(youtube,args.videoid)
        #Perform sentiment analysis and return a compound score
        sia=SentimentIntensityAnalyzer()
        #open and write the compounds file
        with open('compounds.csv','w') as csvfile:
            writer=csv.writer(csvfile)
            positive_comments,neutral_comments,negative_comments=count_comments_per_sentiment(video_comment_threads)
            writer.writerow(["Video Id", "Positive Comments","Neutral Comments" ,"Negative Comments"])
            writer.writerow([args.videoid,positive_comments, neutral_comments,negative_comments])
            #REMOVE THESE LINES BELOW AFTER SHOWING TO BRUNO
            for i in range(0, len(video_comment_threads)):
                comment = video_comment_threads[i]
                score = sia.polarity_scores(comment)
                writer.writerow([comment,score["compound"]])
            #
    except HttpError as e:
        print("An Http error %d occurred:\n%s" % (e.resp.status,e.content))
        pass
