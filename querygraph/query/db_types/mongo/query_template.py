import pprint
import datetime
from pymongo import MongoClient

client = MongoClient()

db = client['test-database']

posts = db.posts

post = {"author": "Mike",
         "text": "My first blog post!",
        "tags": ["mongodb", "python", "pymongo"],
         "date": datetime.datetime.utcnow()}

post_id = posts.insert_one(post).inserted_id
pprint.pprint(posts.find_one())