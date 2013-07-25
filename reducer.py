import datetime
from threading import Thread
import textwrap
import re
import json

import pymongo
import tweepy
import nltk
from nltk.tokenize import RegexpTokenizer

import MapReduce

connection = pymongo.MongoClient('mongodb://localhost')
db = connection.wnn

delta = str(datetime.datetime.now() + datetime.timedelta(days=-1))

tokenizer = RegexpTokenizer('\w+')
posex = re.compile('^[NVJ]')

auth = tweepy.auth.OAuthHandler('***', '***')
auth.set_access_token('***', '***')
api = tweepy.API(auth)

def hashtagger(facets):
	list_of_tags = ' '
	list_of_facets = ' '.join(set(facets))
	tokens = tokenizer.tokenize(list_of_facets)
	tags = nltk.pos_tag(tokens)
	for tag in tags:
		word, pos = tag
		if posex.match(pos):
			list_of_tags += '#' + word + ' '
	return list_of_tags

def mapper(doc):
	list_of_tokens = doc['tokens']
	for token in list_of_tokens:
		clue = db.clues.find_one({'word': token['word']})
		if clue is not None:
			mr.emit_intermediate(clue['polarity'], token['count'])

def reducer(key, list_of_values):
	total = 0
	for v in list_of_values:
		total += v
	mr.emit((key, total))

def handle_thread(collection):
	documents = db[collection].find({'pub_date':{'$gte': delta}}, {'_id': 0})
	for doc in documents:
		if len(doc['tokens']) > 0:
			global mr
			mr = MapReduce.MapReduce()
			results = mr.execute(doc, mapper, reducer)
			tags = hashtagger(doc['facets'])
			
			status = "%s: %s %s" % (doc['title'], json.dumps(dict(results)), tags)
			status = textwrap.wrap(status,140)[0]
			try:
				print status
				#api.update_status(status)
			except:
				pass

def main():
	sections = ['world', 'us', 'opinion']
	for key in sections:
		try:
			e = Thread(group=None,target=handle_thread,name=None,args=(key,))
			e.start()
			e.join()
		except Exception as errtxt:
			print errtxt

if __name__ == '__main__':
   main()