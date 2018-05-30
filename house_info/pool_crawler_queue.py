from multiprocessing import Pool 
from downloader_p3 import Downloader
from mongo_queue_p3 import MongoQueue
from mongo_cache_p3 import MongoCache 
from scrape_callback_p3 import Scrape_Callback
import threading
import urllib.parse
import multiprocessing
import time 
import os
import lxml.html


SLEEP_TIME = 2
DEFAULT_CACHE = MongoCache()
DEFAULT_SC_CALLBACK = Scrape_Callback()
def threaded_crawler(seed_url,delay=5,cache=DEFAULT_CACHE,scrape_callback=DEFAULT_SC_CALLBACK,user_agent='wswp',proxies=None,num_retries=1,max_threads=10,timeout=60):
	crawl_queue = MongoQueue()
	crawl_queue.clear()
	crawl_queue.push(seed_url)
	D = Downloader(cache=cache,delay=delay,proxies=proxies,num_retries=num_retries,timeout=timeout)
	def process_queue():
		while True:
			try:
				url = crawl_queue.pop()
			except KeyError:
				break
			else:
				response = D(url).decode('utf-8')
				if scrape_callback:
					scrape_callback.__call__(response)
				html = lxml.html.fromstring(response)
				links = []
				links.extend(html.xpath('//ul[@class="pagination"]/li/a/@href'))
				for link in links:
					crawl_queue.push(normalize(seed_url,link))
				crawl_queue.complete(url)
	threads = []
	while threads or crawl_queue.peek():
		for thread in threads:
			if not thread.is_alive():
				threads.remove()
		while len(threads) < max_threads and crawl_queue.peek():
			thread = threading.Thread(target = process_queue)
			thread.setDaemon(True)
			thread.start()
			threads.append(thread)
		time.sleep(SLEEP_TIME)
def normalize(seed_url,link):
	link,_= urllib.parse.urldefrag(link)
	return urllib.parse.urljoin(seed_url,link)

def pool_crawler(args):
	num_cpu = multiprocessing.cpu_count()
	pool = Pool(2)
	print('Start {} processing'.format(num_cpu))
	for num in range(2):
		#print('kwargs=',kwargs)
		pool.apply_async(func=threaded_crawler,args=(args,))
	pool.close()
	pool.join()


