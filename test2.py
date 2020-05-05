import pymongo

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["indonesia"]
mycol = mydb["test8"]
mysave = mydb["tyu"]


if __name__ == "__main__":

    for tweet in mycol.find():  # Selectin _id
        print(tweet)
        mysave.save(tweet)
