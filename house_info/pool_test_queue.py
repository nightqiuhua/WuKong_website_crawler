import sys
from pool_crawler_queue import pool_crawler
from mongo_cache_p3 import MongoCache 
from pic_downloader import pic_donwloader
from scrape_callback_p3 import Scrape_Callback

def main(max_threads):
	seed_url = 'https://www.wkzf.com/shanghai/esf'
	pool_crawler(seed_url)

if __name__ == '__main__':
	max_threads =3
	main(max_threads)
