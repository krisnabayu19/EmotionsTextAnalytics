import pymongo

# Connection to MongoDB
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["indonesia"]
mycol = mydb["test8"]
mysave = mydb["test9"]


if __name__ == "__main__":

  # Looping for Selecting Coloumn we Needed
  for tweet in mycol.find():

    tweet_id = tweet['id_str']  # The Tweet ID from Twitter in string format
    dt = tweet['created_at']  # The timestamp of when the Tweet was created
    username = tweet['user']['screen_name']  # The username of the Tweet author
    followers = tweet['user']['followers_count']  # The number of followers the Tweet author has
    text = tweet['text']  # The entire body of the Tweet
    source = tweet['source']
    url = tweet['place']['url']
    place_type = tweet['place']['place_type']
    name = tweet['place']['name']
    full_name = tweet['place']['full_name']
    country_code = tweet['place']['country_code']
    country = tweet['place']['country']
    language = tweet['lang']  # The language of the Tweet

    # Save Coloumn to Variable tweets
    tweets = {'id': tweet_id,
              'created_at' : dt,
              'username' : username,
              'followers' : followers,
              'text' : text,
              'source' : source,
              'url' : url,
              'place_type' : place_type,
              'name' : name,
              'full_name' : full_name,
              'country_code' : country_code,
              'country' : country,
              'language' : language}

    # Print Tweets
    print(tweets)

    # Save to MongoDB
    mysave.save(tweets)
