from threading import Thread

import pymongo
import nltk
from nltk.tokenize import RegexpTokenizer

import nytimes_api

class Service():
	def __init__(self):
		self.most_popular = nytimes_api.Connection(api_key='***')
		self.community = nytimes_api.Connection(api_key='***')
		self.connection = pymongo.MongoClient('mongodb://localhost')
		self.db = self.connection.wnn
		self.tokenizer = RegexpTokenizer('\w+')

	def generate_tokens(self, url):
		comments = self.community.get_comments(url)
		list_of_tokens = {}

		for comment in comments['results']['comments']:
			tokens = self.tokenizer.tokenize(comment['commentBody'])
			for token in tokens:
				if token in list_of_tokens:
					list_of_tokens[token] += 1
				else:
					list_of_tokens[token] = 1
		return list_of_tokens

		
	def verify_post(self, post_id, collection):
		if self.db[collection].find({'url': post_id}):
			self.db[collection].remove({'url': post_id})

	def handle_thread(self, section, collection):
		posts = self.most_popular.get_mostviewed(section)
		for post in posts['results']:
			self.verify_post(post['url'], collection)
			tokens = self.generate_tokens(post['url'])
			list_of_tokens = [{'word': i, 'count': j} for i, j in tokens.items()]
			document = {'post_id': post['id'], 'url': post['url'], 'title': post['title'], 
				'facets': post['des_facet'], 'pub_date': post['published_date'], 'section': 'U.S.', 
				'tokens': list_of_tokens}
			self.db[collection].insert(document)

def main():
	service = Service()
	sections = {'U.S.': 'us', 'World': 'world', 'Opinion': 'opinion'}
	for key in sections:
		try:
			e = Thread(None,service.handle_thread,None,(key, sections[key]))
			e.start()
			e.join()
		except Exception as err:
			print err

if __name__ == '__main__':
    main()
