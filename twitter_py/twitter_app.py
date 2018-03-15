#!/usr/bin/env python
# # -*- coding: utf-8 -*-

import socket
import sys
import requests
import requests_oauthlib
import json

# --------------------------------------------------------------------------------------------------------
# Step 1: Enter your Twitter API Credentials.
# Go to https://apps.twitter.com and look up your Twitter API Credentials, or create an app to create them.
# add the variables that will be used in OAuth for connecting to Twitter
ACCESS_TOKEN = '...'
ACCESS_SECRET = '...'
CONSUMER_KEY = '...'
CONSUMER_SECRET = '...'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,ACCESS_TOKEN, ACCESS_SECRET)


# call the Twitter API URL and return the response for a stream of tweets
def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'fr'), ('locations', '-130,-20,100,50'),('track','#')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


# function that takes the response from the above one and extracts the tweets’ text from the whole tweets’ JSON object.
# After that, it sends every tweet to Spark Streaming instance (will be discussed later) through a TCP connection
def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text.encode('utf-8'))
            print ("------------------------------------------")
            tcp_connection.send(tweet_text + '\n')
        except:
            e = sys.exc_info()[0]
            print("Error: %s" % e)


# Now, we’ll make the main part which will make the app host socket connections that spark will connect with.
# We’ll configure the IP here to be localhost as all will run on the same machine and the port 9009.
# Then we’ll call the get_tweets method, which we made above, for getting the tweets from Twitter
# and pass its response along with the socket connection to send_tweets_to_spark for sending the tweets to Spark.
TCP_IP = "localhost"
TCP_PORT = 9008
conn = None

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind((TCP_IP, TCP_PORT))
s.listen(1)
print("Waiting for TCP connection...")
conn, addr = s.accept()
print("Connected... Starting getting tweets.")

resp = get_tweets()
send_tweets_to_spark(resp, conn)

