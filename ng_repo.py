
from pymongo import MongoClient

client = MongoClient()
db = client['kevin_newsguard']
col = db['domain']

# domain:
# {
# 	last_fetched: 12058210359,
#	
# }

class Repository:
	def get(self, domain):
		return col.find_one(
			{ '_id': domain }
		)

	def save(self, doc):
		col.insert_one(doc)

repository = Repository()
