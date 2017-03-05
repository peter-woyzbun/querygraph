import pprint
import json
import datetime
from pymongo import MongoClient

from querygraph.query.template import QueryTemplate
from querygraph.query.db_types.postgres.template_parameter import PostgresParameter

client = MongoClient()

db = client['test-database']

posts = db.posts

post = {"author": "Mike",
         "text": "My first blog post!",
        "tags": ["mongodb", "python", "pymongo"],
         "date": datetime.datetime.utcnow()}

post_id = posts.insert_one(post).inserted_id
pprint.pprint(posts.find_one())


class MongoTemplate(QueryTemplate):

    def __init__(self, template_str, db_connector, field_types):
        self.field_types = field_types
        QueryTemplate.__init__(self,
                               template_str=template_str,
                               db_connector=db_connector,
                               parameter_class=PostgresParameter)

    def _post_render_value(self, render_value):
        return json.loads(render_value)