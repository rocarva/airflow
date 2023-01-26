import os
import sys


sys.path.append("/opt/airflow")


import json
from datetime import datetime, timedelta
import time
from airflow.providers.http.hooks.http import HttpHook
import sys
import tweepy




"""
This Python code is a class called TwitterHook that is used to extract tweets from the Twitter API. It requires four parameters: search_query, no_of_tweets, day and language. 

It first authenticates the API using the keys declared in the beginning and then uses the search_tweets() method of tweepy to extract the required tweets. It returns a json string containing the attributes of the tweets.

The run() method calls the extrai_twitter() method to extract the tweets.

If __name__ == "__main__": is used to define the main execution of the code.
"""

#This Python code is creating a class called TwitterHook. The class is used to connect to the Twitter API and extract tweets with a given search query, language, and number of tweets. 

#It imports the necessary packages and libraries to run the code, including airflow.providers.http.hooks.http, tweepy, dotenv, json, os, datetime, and timedelta.

#It then uses os.environ to set the consumer key, consumer secret, access token, and access token secret from environment variables. 

#The class itself has an __init__ function that sets the parameters for the search query, number of tweets, day, and language.

#The autentica_api function authenticates the API and the extrai_twitter function extracts the tweets from the API.

#The run function calls the extrai_twitter function and returns the results.

#Finally, the code calls the class and sets the parameters for the search query, number of tweets, day, and language.



from dotenv import load_dotenv , dotenv_values

load_dotenv(".env")


consumer_key = str(os.getenv("API_KEY"))
consumer_secret = str(os.getenv("API_KEY_SECRET"))
access_token = str(os.getenv("ACCESS_TOKEN"))
access_token_secret = str(os.getenv("ACCESS_TOKEN_SECRET"))



class TwitterHook():
    def __init__(self, search_query, no_of_tweets, day, language):
        self.search_query = search_query
        self.no_of_tweets = no_of_tweets
        self.day = day
        self.language = language
        super().__init__()

    def authenticate_api(self):
        auth = tweepy.OAuth1UserHandler(
            consumer_key,
            consumer_secret,
            access_token,
            access_token_secret
        )
        api = tweepy.API(auth, wait_on_rate_limit=True)
        return api

    def extract_tweets(self):
        search_query = self.search_query
        no_of_tweets = self.no_of_tweets
        day = self.day
        language = self.language
        tweets = self.authenticate_api().search_tweets(
            q=search_query,
            count=no_of_tweets,
            lang=language,
            until=day
        )

        attributes_container = [
            [
                tweet.user.name,
                tweet.id,
                tweet.created_at,
                tweet.id,
                tweet.in_reply_to_user_id,
                tweet.lang,
                tweet.text
            ]
            for tweet in tweets
        ]
        json_string = json.dumps(
            attributes_container,
            indent=4,
            sort_keys=True,
            default=str
        )
        return json_string  
        
    
    def run(self):        
        return self.extract_tweets()

if __name__ == "__main__":
    day = time.strftime("%Y-%m-%d")
    language = "pt-br"
    search_query = "data science"
    no_of_tweets = 150
       
    json_twitter = (TwitterHook(search_query, no_of_tweets, day, language).run())
    with open("extract_twitter.json","w") as outfile:
        outfile.write(json_twitter)
    
    
    