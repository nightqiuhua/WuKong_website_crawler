import re
import lxml.html
import pymongo
from pymongo import MongoClient,errors


class Scrape_Callback:
	def __init__(self):
		self.db = pymongo.MongoClient("localhost", 27017).cache


	def __call__(self,html):
		#html = Downloader(url).decode('utf-8')
		tree = lxml.html.fromstring(html)
		nodes = tree.xpath('//ul[@class="suban clearfix"]/li')
		for node in nodes:
			items = {}
			items['url'] = node.xpath('./a/@href')[0]
			items['title'] = node.xpath('./a/div[@class="infobox"]/div/p/text()')[0]
			items['flat_type'] = node.xpath('./a/div[@class="infobox"]/p[@class="fz14 yearbox"]/span/text()')[0]
			items['area'] = node.xpath('./a/div[@class="infobox"]/p[@class="fz14 yearbox"]/span/text()')[1]
			if len(node.xpath('./a/div[@class="infobox"]/p[@class="fz14 yearbox"]/span[3]/text()')):
				items['year'] = node.xpath('./a/div[@class="infobox"]/p[@class="fz14 yearbox"]/span[3]/text()')[0]
			else:
				items['year'] = None
			items['address'] = node.xpath('./a/div[@class="infobox"]/p[@class="address"]/text()')[0]
			items['to_price'] = node.xpath('./a/div[@class="infobox"]/div[@class="moneyinfo"]/p/em/text()')[0]
			items['per_price'] = node.xpath('./a/div[@class="infobox"]/div[@class="moneyinfo"]/p[@class="area"]/text()')[0]
			try:
				self.db.items_info.insert({'_id':items['url'],'items':items})
			except errors.DuplicateKeyError as e:
				pass
			



