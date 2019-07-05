
from sklearn.externals import joblib

import json
import urllib
import requests
import lxml.html
import re
import math
from collections import deque
import traceback

from time import time, sleep

from threading import Thread, Lock

from ng_repo import repository

def main_loop():
	predictor.main_loop()

class Predictor:
	max_queue = 1000

	fields = {
		'Alexa Reach Rank': 'int',
		'Alexa USA Rank': 'int',
		'Canonical URL': 'str',
		'Citation Flow': 'int',
		'Domain Analysis For': 'str',
		'Domain Authority': 'int',
		'EDU Backlinks': 'int',
		'EDU Domains': 'int',
		'External Backlinks': 'int',
		'GOV Backlinks': 'int',
		'GOV Domains': 'int',
		'Global Rank': 'int',
		'Google Directory listed': 'bool',
		'Google PageRank': 'int',
		'Indexed URLs': 'int',
		'PR Quality': 'str',
		'Page Authority': 'int',
		'Referring Domains': 'int',
		'Root IP': 'str',
		'Spam Score': 'int',
		'Topic Value': 'int',
		'Topic': 'str',
		'Trust Flow': 'int',
		'Trust Metric': 'int',
		'cPR Score': 'float'
	}

	x_cols = [
		'Google PageRank',
		'cPR Score',
		'Domain Authority',
		'Page Authority',
		'Trust Metric',
		'Trust Flow',
		'Citation Flow',
		'Global Rank',
		'Alexa USA Rank',
		'Alexa Reach Rank',
		'Spam Score',
		'External Backlinks',
		'Referring Domains',
		'EDU Backlinks',
		'EDU Domains',
		'GOV Backlinks',
		'GOV Domains',
		'Topic Value',
		'Indexed URLs'
	]

	log_cols = [
		'Global Rank',
		'Alexa USA Rank',
		'Alexa Reach Rank',
		'External Backlinks',
		'Referring Domains',
		'EDU Backlinks',
		'EDU Domains',
		'GOV Backlinks',
		'GOV Domains',
		'Indexed URLs'
	]


	def __init__(self):
		self.last_query = 0
		self.lock = Lock()
		self.queue = deque([])
		self.running = False

		self.model = joblib.load('linear-noft-sw-5.joblib')

	def request(self, link):
		ident = self.clean_url(link)
		
		data = repository.get(ident)
		if data is not None:
			print('request known')
			del data['_id']
			data['query'] = link
			data['domain'] = ident
			return data

		else:
			self.lock.acquire()
			if self.running or time() < self.last_query + 30:
				print('request enqueue')
				# enqueue
				if ident in self.queue:
					queue = self.queue.index(ident) + 1
				elif len(self.queue) < Predictor.max_queue:
					self.queue.append(ident)
					queue = len(self.queue)
				else:
					queue = -1

				# trigger if not running
				if not self.running:
					self.running = True
					Thread(target=main_loop).start()

				self.lock.release()
				return {
					'query': link,
					'domain': ident,
					'queue': queue
				}

			else:
				print('request immediate')
				score, features = self.predict(ident)
				self.last_query = time()
				self.lock.release()
				return {
					'query': link,
					'domain': ident,
					'score': score,
					'features': features
				}

	def enqueue(self, link):
		self.lock.acquire()
		self.queue.append(link)
		self.lock.release()

		self.trigger()

	def trigger(self):
		self.lock.acquire()
		if not self.running:
			self.running = True
			Thread(target='main_loop')
		self.lock.release()

	def main_loop(self):
		while True:
			tts = self.last_query + 30 - time()
			if tts > 0:
				print('sleeping for', tts, 'seconds')
				sleep(self.last_query + 30 - time())
			
			ident = self.queue.popleft()
			
			self.predict(ident)
			retries = 0

			self.lock.acquire()
			if len(self.queue) == 0:
				self.running = False
				self.lock.release()
				break
			else:
				self.lock.release()

			self.last_query = time()

	def predict(self, link):
		try:
			ident = self.clean_url(link)

			data = repository.get(ident)
			if data is None:
				features = self.fetch_features(ident)

				# features = {'Domain Analysis For': 'harddrop.com', 'Google PageRank': 4, 'cPR Score': 4.4, 'Domain Authority': 44, 'Page Authority': 38, 'Trust Flow': 21, 'Trust Metric': 21, 'Global Rank': 188216, 'Alexa USA Rank': 59853, 'Alexa Reach Rank': 186334, 'Spam Score': 5, 'External Backlinks': 119770, 'Referring Domains': 321, 'EDU Backlinks': 0, 'EDU Domains': 0, 'GOV Backlinks': 0, 'GOV Domains': 0, 'PR Quality': 'Moderate', 'Canonical URL': 'harddrop.com/', 'Root IP': '192.30.32.125', 'Topic': 'Arts/Animation',
				# 	'Topic Value': 19, 'Indexed URLs': 513357, 'Google Directory listed': True, 'DMOZ.org listed': False, 'Citation Flow': '37'}

				inp = []
				for col in Predictor.x_cols:
					if col in Predictor.log_cols:
						inp.append(math.log10(max(0,float(features[col]))+1))
					else:
						inp.append(float(features[col]))
				
				score = self.model.predict([inp])[0]

				data = {
					'_id': ident,
					'lastUpdated': time(),
					'score': score,
					'features': features
				}
				repository.save(data)
			
			return data['score'], data['features']
		
		except:
			repository.save({
				'_id': ident,
				'lastUpdated': time(),
				'error': traceback.format_exc()
			})
			return None, None

	def fetch_features(self, ident):
		print('fetching', ident)

		# post request
		payload = {'name': ident}
		r = requests.post("https://checkpagerank.net/check-page-rank.php/POST", data=payload)

		try:
			# extract pdf
			doc = lxml.html.fromstring(r.text)

			elem = list(doc.xpath('//div[@id="html-2-pdfwrapper"]'))[0]
			pdf = elem.text_content()
			
			features = self.read_cpr_pdf(pdf)
			features['Citation Flow'] = re.search(r'<div class="col-md-5">Citation Flow: ([0-9]*)<\/div>', r.text).group(1)

			print(features)

		except:
			raise Exception(r.text)

		return features
	
	def read_cpr_pdf(self, pdf):
		# read cpr pdf and split into entries
		pat = re.compile('\\s*(.*:)\\s*')
		vals = pat.split(pdf)

		doc = {}
		j = 0
		while j < len(vals):
			if ':' in vals[j]:
				key = vals[j][:-1]

				if key in self.fields:
					val_str = vals[j+1]
					field_type = self.fields[key]

					val = []
					if field_type == 'int':
						if val_str == 'N/A' or len(val_str) == 0:
							val = None
						elif len(val_str.split('/')[0].strip().replace(',', '')) == 0:
							print('val_str is empty', key, val_str)
						else:
							val = int(val_str.split('/')[0].strip().replace(',', ''))
					elif field_type == 'float':
						if val_str == 'N/A' or len(val_str) == 0:
							val = None
						elif len(val_str.split('/')[0].strip().replace(',', '')) == 0:
							print('val_str is empty', key, val_str)
						else:
							val = float(val_str.split('/')[0].strip())
					elif field_type == 'bool':
						if val_str.strip() == 'YES':
							val = True
						elif val_str.strip() == 'NO':
							val = False
					elif field_type == 'str':
						val = val_str
					
					if val is list:
						raise key + ': ' + val_str
					doc[key] = val

			j += 1
		
		return doc

	def clean_url(self, url):
		##########################################################
		# remove "http(s)://" prefix and "/" suffix
		##########################################################
		if url[:7] == 'http://':
			url = url[7:]
		elif url[:8] == 'https://':
			url = url[8:]

		# remove www.
		if url[:4] == 'www.':
			url = url[4:]

		if url[-1] == '/':
			url = url[:-1]

		return url.split('/')[0]

predictor = Predictor()

# # first --> immediate result
# print('first immediate result')
# print(predictor.request('http://harddrop.com/forums/index.php?showtopic=6730'))
# sleep(1)

# # second --> enqueue (0)
# print('second enqueue (0)')
# print(predictor.request('bbc.com'))
# sleep(1)

# # known --> immediate
# print('known --> immediate')
# print(predictor.request('dctribune.org'))

# # third --> enqueue (1)
# print('third enqueue (1)')
# print(predictor.request('https://www.mercurynews.com/2019/07/04/top-images-from-president-donald-trumps-salute-to-america-celebration/'))

# # fourth --> enqueue (2)
# print('fourth enqueue (2)')
# print(predictor.request('https://www.wsj.com/articles/trump-delivers-elaborate-fourth-of-july-event-showcasing-military-11562282904'))

# # second is now ready
# sleep(30)
# print('second is now ready')
# print(predictor.request('bbc.com'))

# # third is not yet ready
# print('third is not yet ready')
# print(predictor.request('https://www.mercurynews.com/2019/07/04/top-images-from-president-donald-trumps-salute-to-america-celebration/'))

# #
# sleep(30)
# print('third is now ready')
# print(predictor.request('https://www.mercurynews.com/2019/07/04/top-images-from-president-donald-trumps-salute-to-america-celebration/'))

# sleep(30)
# print('fourth is now ready')
# print(predictor.request('https://www.wsj.com/articles/trump-delivers-elaborate-fourth-of-july-event-showcasing-military-11562282904'))
