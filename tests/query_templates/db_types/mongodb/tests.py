import unittest


from querygraph.query.templates.mongodb.query_template import MongoDbTemplate
from querygraph.db.connectors import MongoDb





test_connector = MongoDb(host='', db_name='querygraph-test', collection='seasons')
template_str = """{"tags" : {"$in" : {{ seasons_tags|value_list:str }} }}"""
query_template = MongoDbTemplate(template_str=template_str, db_connector=test_connector, fields=['season'])

df = query_template.execute(seasons_tags=['hot', 'melting'])
print df