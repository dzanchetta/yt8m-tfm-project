import httplib2
import os
import sys
import json

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

def get_video_details(youtube,video_id,token=""):
    results=[]
    statistics=[]
    results=youtube.videos().list(part="snippet,statistics",
                                          pageToken=token,
                                          id=video_id).execute()
    for item in results["items"]:
        statistics = item["statistics"]

    return statistics

if __name__=="__main__":
    #argparser.add_argument("--videoid", help="Required; ID for video for which the comment will be inserted.")
    args=argparser.parse_args()
    #if not args.videoid:
    #    exit("Please specify videoid")
    youtube=get_authenticated_service(args)

    with open('/Users/daniel/LocalFiles for TFM/youtubeProjectTFM/src/definitive_videoids_updated_20200901.txt','r') as input:
        for line in input:
            videoid = line.strip()
            try:
                video_metadata=get_video_details(youtube,videoid)
                #open and write the compounds file
                filePath = '/Users/daniel/LocalFiles for TFM/Files/video-likes/'
                with open(filePath+videoid+'.json','a') as outputfile:
                    json.dump(video_metadata,outputfile)
            except Exception as e:
                print("An Http error %d occurred:\n%s" % (e.resp.status,e.content) + 'Video Id: '+videoid)