from datetime import datetime,timedelta
from pymongo import MongoClient,errors
import pymongo

class MongoQueue:

	OUTSTANDING,PROCESSING,COMPLETE = range(3)
	def __init__(self,client=None,timeout=300):
		#self.client = MongoClient("localhost", 27017) if client is None else client
		self.db = pymongo.MongoClient("localhost", 27017).cache
		self.timeout = timeout

	def __nonzero__(self):
		record = self.db.crawl_queue.find_one(
			{'status':{'$ne':self.COMPlETE}}
			)
		return True if record else None

	def push(self,url):
		try:
			self.db.crawl_queue.insert({'_id':url,'status':self.OUTSTANDING})
		except errors.DuplicateKeyError as e:
			pass

	def pop(self):
		record = self.db.crawl_queue.find_and_modify(
			query={'status':self.OUTSTANDING},
			update={'$set':{'status':self.PROCESSING,'timestamp':datetime.now()}})
		if record:
			return record['_id']
		else:
			self.repair()
			raise KeyError()

	def peek(self):
		record = self.db.crawl_queue.find_one({'status':self.OUTSTANDING})
		if record:
			return record['_id']

	def complete(self,url):
		self.db.crawl_queue.update({'_id':url},{'$set':{'status':self.COMPLETE}})

	def repair(self):
		record = self.db.crawl_queue.find_and_modify(
			query = {'timestamp':{'$lt':datetime.now()-timedelta(seconds=self.timeout)},
			'status':{'$ne':self.COMPLETE}},
			update = {'$set':{'status':self.OUTSTANDING}})
		if record:
			print('Released',record['_id'])

	def clear(self):
		self.db.crawl_queue.drop()
