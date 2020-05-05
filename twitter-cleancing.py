from pymongo import MongoClient
import pymongo
from yandex.Translater import Translater
import pandas as pd
from Sastrawi.Stemmer.StemmerFactory import StemmerFactory
from textblob import TextBlob
import re
import json

# Connection to MongoDB
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["indonesia"]
mycol = mydb["test10"]

# Stemmer
factory = StemmerFactory()
stemmer = factory.create_stemmer()

# Translater
tr = Translater()
tr.set_key('trnsl.1.1.20191105T142939Z.b7e2df135b69d172.03f8d4635086f151f8b025984598a17b9af20ca6') # Api key found on https://translate.yandex.com/developers/keys
tr.set_from_lang('id')
tr.set_to_lang('en')


def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://localhost:27017/' % (username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)
    return conn[db]


def read_mongo(db, collection, query={}, host='localhost', port=27017, username=None, password=None, no_id=True):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port, username=username, password=password, db=db)

    # Make a query to the specific DB and Collection
    cursor = db[collection].find(query)

    # Expand the cursor and construct the DataFrame
    df =  pd.DataFrame(list(cursor))

    # Delete the _id
    if no_id and '_id' in df:
        del df['_id']

    return df

# Function to Clean
def clean_tweet(tweet):
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|(_[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)|"
                           "(\s([@#][\w_-]+)|(#\\S+))|((www\.[^\s]+)|(https?://[^\s]+)|(http?://[^\s]+))", "", tweet).split())



if __name__ == '__main__':
    df = read_mongo('indonesia', 'test9', {})
    df.to_csv('5.csv', index=False)

    # Import File CSV to Cleancing
    df = pd.read_csv("5.csv")

    # Looping for Cleaning
    for index, row in df.iterrows():
        #  get Tweet Kotor
        test = row['text']
        print("Tweet Kotor :", test)

        # Pengurangan karakter ke n
        n = len(test)
        ges = test[0:n - 0]
        # print("Pengurangan Character :",ges)

        # toLowerCase Character
        gas = ges.strip()
        blob = clean_tweet(gas)
        hasil = stemmer.stem(blob)
        print("Lowering Case :",hasil)

        # Clean Tweets
        blob1 = str(hasil)
        print("Tweet Bersih :", blob1)

        # Translate Tweets
        tr.set_text(blob1)
        bersih = tr.translate()
        kedas = TextBlob(bersih)
        print("Translate Tweet :",bersih)
        print("==============================================================================================================================")

        # Variable After Cleanning Tweets
        # mongo = {
        #     "id": row["id"],
        #     "created_at": row["created_at"],
        #     "username": row["username"],
        #     "followers": row["followers"],
        #     "text": hasil,
        #     "source": row["source"],
        #     "url":row["url"],
        #     "place_type": row["place_type"],
        #     "name": row["name"],
        #     "full_name": row["full_name"],
        #     "country_code": row["country_code"],
        #     "country": row["country"],
        #     "language": row["language"]
        # }
        #
        # # Print Variable MongoDB
        # print(mongo)

        # Save to MongoDB
        # x = mycol.insert_one(mongo)


