import pymongo
from yandex.Translater import Translater
import pandas as pd
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from textblob import TextBlob
import re

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["indonesia"]
mycol = mydb["test4"]
mysave = mydb["test7"]

factory = StemmerFactory()
stemmer = factory.create_stemmer()

tr = Translater()
tr.set_key('trnsl.1.1.20191105T142939Z.b7e2df135b69d172.03f8d4635086f151f8b025984598a17b9af20ca6') # Api key found on https://translate.yandex.com/developers/keys
tr.set_from_lang('id')
tr.set_to_lang('en')

def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|"
                           "(\s([@#][\w_-]+)|(#\\S+))|((www\.[^\s]+)|(https?://[^\s]+)|(http?://[^\s]+))", "", tweet).split())

# df = pd.read_csv("mainData.csv")
for tweet in mycol.find({}, {"id", "created_at", "text", "source", "user", "geo", "coordinate", "place","lang"}):  # Selectin _id
    print(tweet)
    df = pd.read_json(tweet)





for index, row in df.iterrows():
    test = row['text']
    n = len(test)
    ges = test[2:n-1]
    print(ges)
    gas = ges.strip()
    blob = clean_tweet(gas)
    hasil = stemmer.stem(blob)
    print(hasil)
    blob1 = str(hasil)
    tr.set_text(blob1)
    bersih = tr.translate()
    kedas = TextBlob(bersih)
    print(bersih)
    # if kedas.sentiment.polarity > 0:
    #     test1 = 1
    #     kata = 'positive'
    #     print(kata,test1)
    # elif kedas.sentiment.polarity < 0:
    #     test1 = -1
    #     kata = 'negative'
    #     print(kata,test1)
    # elif kedas.sentiment.polarity == 0.0:
    #     test1 = 0
    #     kata = 'neutral'
    #     print(kata,test1)

    # mongo = {
    #     # "created_at":row['created_at'],
    #     # "tweed_id":row['tweet_id'],
    #     # "favorite": row['favorite_count'],
    #     # "retweet": row['retweet_count'],
    #     # "location": row['location'],
    #     # "country": row['country'],
    #     "text":hasil }
    #     # "source": row['source'],
    #     # "hashtag":row['hashtag']}
    # x = mysave.insert_one(mongo)