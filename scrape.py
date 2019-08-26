# SERVERSIDE SCRIPT

from confirm import confirm
confirm()

import pandas as pd
import numpy as np

from ratelimiter import RateLimiter

from pymongo import MongoClient
from bson.objectid import ObjectId

import boto3

import pprint
p = lambda x: pprint.PrettyPrinter(depth=6).pprint(x)

db = MongoClient('mongodb://@54.183.229.143', 27017)

# Creating DB
reviews = db.project.reviews.find({},{
    '_id':1,
    'review_id':1,
    'title':1,
    'content':1
})

df = pd.DataFrame(reviews)

df = df[['_id', 'review_id', 'title', 'content']]
df['corpus'] = df[['title','content']].apply(lambda x: ' '.join(x), axis=1)
combined_df = df[['_id', 'review_id', 'corpus']]


### Creating funcition to pull from AWS API
client = boto3.client('comprehend')
response = lambda x: client.batch_detect_sentiment(
    TextList=x,
    LanguageCode='en'
)

rate_limiter = RateLimiter(max_calls=20, period=1)

counter = 0

for i, v in combined_df.iterrows():
    with rate_limiter:
        try:
            _ = response([v['corpus']])
            db.project.sentiment_reviews.insert({'review_id': v['review_id'], 'sentiment_response': _})
            print(counter)
            print(_['ResultList'])
            
        except:
            db.project.sentiment_reviews.insert({'review_id': v['review_id'], 'sentiment_response': None})
            print(counter)
            print(_['ResultList'])
            pass

