#! /usr/bin/env python
# -*- coding: utf-8 -*-
#======================================================================
#
# tradelib.py - 
#
# Created by skywind on 2018/06/30
# Last Modified: 2018/06/30 20:50:25
#
#======================================================================
from __future__ import print_function
import sys
import time
import os
import json
import math


#----------------------------------------------------------------------
# python 2/3 compatible
#----------------------------------------------------------------------
if sys.version_info[0] >= 3:
	long = int
	unicode = str
	xrange = range


#----------------------------------------------------------------------
# OrderBook
#----------------------------------------------------------------------
class OrderBook (object):

	def __init__ (self, source = None):
		self.time = None
		self.reset()
		self._load_source(source)
	
	def reset (self):
		self.bids = []
		self.asks = []
		self.bids_sum = 0.0
		self.asks_sum = 0.0

	def _load_source (self, source):
		self.reset()
		if source is None:
			return False
		if isinstance(source, dict):
			self.load_dict(source)
		elif isinstance(source, str) or isinstance(source, str):
			self.load_json(source)
		else:
			return False
		return True

	# price must be sorted from low to high
	def asks_push (self, price, size):
		total = price * size
		self.asks_sum += total	
		self.asks.append((price, size, total, self.asks_sum))

	# price must be sorted from high to low
	def bids_push (self, price, size):
		total = price * size
		self.bids_sum += total
		self.bids.append((price, size, total, self.bids_sum))

	# price must be sorted from low to high
	def asks_push_list (self, asks):
		for item in asks:
			price = item[0]
			size = item[1]
			total = price * size
			self.asks_sum += total
			self.asks.append((price, size, total, self.asks_sum))
		return len(asks)

	# price must be sorted from high to low
	def bids_push_list (self, bids):
		for item in bids:
			price = item[0]
			size = item[1]
			total = price * size
			self.bids_sum += total
			self.bids.append((price, size, total, self.bids_sum))
		return len(bids)

	# sort list
	def sort (self):
		asks = self.asks
		bids = self.bids
		self.reset()
		asks.sort()
		bids.sort(reverse = True)
		self.asks_push_list(asks)
		self.bids_push_list(bids)

	# load from dict
	def load_dict (self, source):
		self.reset()
		if source is None:
			return False
		if not isinstance(source, dict):
			return False
		asks = source.get('asks', None)
		bids = source.get('bids', None)
		if not asks:
			asks = []
		if not bids:
			bids = []
		if not isinstance(asks, list):
			return False
		if not isinstance(bids, list):
			return False
		self.time = source.get('timestamp', None)
		self.asks_push_list(asks)
		self.bids_push_list(bids)
		return True

	# dump to dict
	def save_dict (self):
		data = {}
		if self.time is not None:
			data['timestamp'] = self.time
		data['asks'] = [ [n[0], n[1]] for n in self.asks ]
		data['bids'] = [ [n[0], n[1]] for n in self.bids ]
		if self.time is not None:
			data['timestamp'] = self.time
		return data

	# load from json
	def load_json (self, text):
		source = json.loads(text)
		self.load_dict(source)

	# save to json string
	def save_json (self):
		data = self.save_dict()
		return json.dumps(data)

	# repr
	def __repr__ (self):
		name = 'OrderBook'
		if __name__ != '__main__':
			name = __name__ + '.OrderBook'
		return '%s(%s)'%(name, repr(self.save_dict()))

	# best bid
	def best_bid (self):
		if not self.bids:
			return None
		return self.bids[0]

	# best ask
	def best_ask (self):
		if not self.asks:
			return None
		return self.asks[0]

	# fmt can be orgtbl
	def tabulify (self, fmt = None):
		headers = ['total', 'size', 'bids', 'asks', 'size', 'total']
		import tabulate
		rows = []
		ask_size = len(self.asks)
		bid_size = len(self.bids)
		size = max(len(self.bids), len(self.asks))
		for index in xrange(size):
			cols = [ '' ] * 6
			if index < bid_size:
				bid = self.bids[index]
				cols[2] = bid[0]
				cols[1] = bid[1]
				cols[0] = bid[2]
			if index < ask_size:
				ask = self.asks[index]
				cols[3] = ask[0]
				cols[4] = ask[1]
				cols[5] = ask[2]
			rows.append(cols)
		return tabulate.tabulate(rows, headers, tablefmt = fmt)


#----------------------------------------------------------------------
# tools
#----------------------------------------------------------------------
class BookHelp (object):

	# load object
	def load_json (self, filename):
		try:
			text = open(filename, 'rb').read()
			if text is None:
				return None
			if sys.version_info[0] < 3:
				if text[:3] == '\xef\xbb\xbf':  	# remove BOM+
					text = text[3:]
				return json.loads(text, encoding = "utf-8")
			else:
				if text[:3] == b'\xef\xbb\xbf':		# remove BOM+
					text = text[3:]
				text = text.decode('utf-8', 'ignore')
				return json.loads(text)
		except:
			return None
		return None

	# save object
	def save_json (self, filename, obj):
		if sys.version_info[0] < 3:
			text = json.dumps(obj, indent = 4, encoding = "utf-8") + '\n'
		else:
			text = json.dumps(obj, indent = 4) + '\n'
			text = text.encode('utf-8', 'ignore')
		try:
			fp = open(filename, 'wb')
			fp.write(text)
			fp.close()
		except:
			return False
		return True

	# load file
	def load_orderbook (self, filename):
		obj = self.load_json(filename)
		if obj is None:
			return None
		return OrderBook(obj)

	# save file
	def save_orderbook (self, filename, orderbook):
		obj = orderbook.save_dict()
		return self.save_json(filename, obj)


#----------------------------------------------------------------------
# book view
#----------------------------------------------------------------------
class BookView (object):

	def __init__ (self):
		self.minimal_amount = 0.01

	def price_at_volume (self, orderbook, side, volume):
		if side in ('buy', 'bid', 'bids', 'biding', 'buyer'):
			items = orderbook.bids
		else:
			items = orderbook.asks
		for item in items:
			price, amount = item[0], item[1]
			if amount >= volume:
				return price
			volume -= amount
		return -1

	def price_avg_volume (self, orderbook, side, volume):
		if side in ('buy', 'bid', 'bids', 'biding', 'buyer'):
			items = orderbook.bids
		else:
			items = orderbook.asks
		amount = volume
		total_price = 0.0
		if amount <= 0.0:
			return 0
		for item in items:
			price, quantity = item[0], item[1]
			if amount > quantity:
				amount -= quantity
				total_price += quantity * price
			else:
				total_price += amount * price
				amount = 0.0
				break
		if amount > 0:
			return -1
		return total_price / volume

	def volume_at_price (self, orderbook, side, price_limit):
		if side in ('buy', 'bid', 'bids', 'biding', 'buyer'):
			volume = 0.0
			for item in orderbook.bids:
				price, amount = item[0], item[1]
				volume += amount
				if price <= price_limit:
					return volume
		else:
			volume = 0.0
			for item in orderbook.asks:
				price, amount = item[0], item[1]
				volume += amount
				if price >= price_limit:
					return volume
		return -1

	# returns (volume, totalcost) or None
	def buy_budget_to_volume (self, orderbook, budget):
		volume = 0.0
		cost = 0.0
		for item in orderbook.asks:
			price = item[0]
			amount = item[1]
			total = item[2]
			if budget < total:
				size = budget / price
				if size < self.minimal_amount:
					size = 0
				volume += size
				cost += size * price
				budget -= size * price
				break
			volume += amount
			cost += total
			budget -= total
		if volume <= 0.0:
			return None
		return (volume, cost)

	# returns (volume, profit) or None
	def sell_volume_to_profit (self, orderbook, volume):
		sumvol = 0.0
		profit = 0.0
		for item in orderbook.bids:
			price = item[0]
			amount = item[1]
			total = item[2]
			if volume < amount:
				size = volume
				if size < self.minimal_amount:
					size = 0
				volume -= size
				sumvol += size
				profit += price * size
				break
			volume -= amount
			sumvol += amount
			profit += total
		if sumvol <= 0.0:
			return None
		return (sumvol, profit)



#----------------------------------------------------------------------
# bookview
#----------------------------------------------------------------------
bookhelp = BookHelp()
bookview = BookView()



#----------------------------------------------------------------------
# testing case
#----------------------------------------------------------------------
if __name__ == '__main__':
	def test1():
		ob = OrderBook()
		ob.asks_push(1.2, 20)
		ob.asks_push(1.1, 10)
		ob.asks_push(1.3, 30)
		ob.asks_push(1.4, 45)
		ob.bids_push(1.0, 1)
		ob.bids_push(0.9, 5)
		ob.bids_push(0.8, 10)
		ob.bids_push(0.7, 20)
		ob.sort()
		print(ob.tabulify('orgtbl'))
		print(ob.save_dict())
		print(ob.save_json())
		print(repr(ob))
		f = eval(repr(ob))
		print(f)
		return 0
	def test2():
		asks = [ (1.1, 10), (1.2, 20), (1.3, 30), ]
		ob = OrderBook({'asks':asks})
		print(ob.tabulify('orgtbl'))
		# print(ob.price_at_volume('ask', 60))
		print(bookview.price_at_volume(ob, 'ask', 60))
		bookhelp.save_orderbook('orderbook1.txt', ob)
		ob = bookhelp.load_orderbook('orderbook1.txt')
		print(ob.tabulify('orgtbl'))
	def test3():
		asks = [ (1.1, 10), (1.2, 20), (1.3, 30), ]
		bids = [ (0.9, 5), (0.8, 6), (0.7, 7), ]
		ob = OrderBook({'asks':asks, 'bids':bids})
		print(ob.tabulify('orgtbl'))
		# print(bookview.buy_budget_to_volume(ob, 1))
		print(bookview.sell_volume_to_profit(ob, 100))

	def test4():
		lbaeth = bookhelp.load_orderbook('ob-lbaeth.txt')
		print(lbaeth.tabulify('orgtbl'))
		return 0

	test4()



