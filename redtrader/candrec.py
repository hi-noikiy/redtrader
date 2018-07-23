#! /usr/bin/env python
# -*- coding: utf-8 -*-
#======================================================================
#
# candrec.py - candle stick database
#
# Created by skywind on 2018/07/23
# Last Modified: 2018/07/23 00:50:42
#
#======================================================================
from __future__ import print_function
import sys
import time
import os
import io
import codecs
import decimal
import sqlite3

try:
	import json
except:
	import simplejson as json

MySQLdb = None


#----------------------------------------------------------------------
# python3 compatible
#----------------------------------------------------------------------
if sys.version_info[0] >= 3:
	unicode = str
	long = int
	xrange = range


#----------------------------------------------------------------------
# CandleStick
#----------------------------------------------------------------------
class CandleStick (object):
	def __init__ (self, t = 0, o = 0.0, h = 0.0, l = 0.0, c = 0.0, v = 0):
		self.ts = t
		self.open = o
		self.high = h
		self.low = l
		self.close = c
		self.volume = v
	def __repr__ (self):
		text = 'CandleStick({}, {}, {}, {}, {}, {})'
		v = self.ts, self.open, self.high, self.low, self.close, self.volume
		return text.format(*v)
	def __str__ (self):
		text = 'ts={}, open={}, high={}, low={}, close={}, vol={}'
		v = self.ts, self.open, self.high, self.low, self.close, self.volume
		return '{' + text.format(*v) + '}'



#----------------------------------------------------------------------
# CandleLite
#----------------------------------------------------------------------
class CandleLite (object):

	def __init__ (self, filename, verbose = False):
		self.__dbname = os.path.abspath(filename)
		self.__conn = None
		self.__verbose = verbose
		self.__open()

	def __open (self):
		sql = '''
		CREATE TABLE IF NOT EXISTS "{name}" (
			"id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
			"ts" INTEGER DEFAULT(0) NOT NULL,
			"symbol" VARCHAR(16) NOT NULL,
			"open" REAL DEFAULT(0),
			"high" REAL DEFAULT(0),
			"low" REAL DEFAULT(0),
			"close" REAL DEFAULT(0),
			"volume" REAL DEFAULT(0)
		);
		CREATE UNIQUE INDEX IF NOT EXISTS "{name}_1" ON {name} (ts, symbol);
		CREATE UNIQUE INDEX IF NOT EXISTS "{name}_2" ON {name} (symbol, ts);
		CREATE INDEX IF NOT EXISTS "{name}_3" ON {name} (ts);
		CREATE INDEX IF NOT EXISTS "{name}_4" ON {name} (symbol);
		'''

		# CREATE UNIQUE INDEX IF NOT EXISTS "{name}_2" ON {name} (ts, symbol);
		self.__conn = sqlite3.connect(self.__dbname, isolation_level = "IMMEDIATE")
		self.__conn.isolation_level = "IMMEDIATE"

		sql = '\n'.join([ n.strip('\t') for n in sql.split('\n') ])
		sql = sql.strip('\n')

		sqls = []
		sqls.append(sql.replace('{name}', 'candle_1m'))
		sqls.append(sql.replace('{name}', 'candle_5m'))
		sqls.append(sql.replace('{name}', 'candle_15m'))
		sqls.append(sql.replace('{name}', 'candle_30m'))
		sqls.append(sql.replace('{name}', 'candle_60m'))
		sqls.append(sql.replace('{name}', 'candle_1d'))
		sqls.append(sql.replace('{name}', 'candle_1w'))
		sqls.append(sql.replace('{name}', 'candle_1m'))

		sql = '\n\n'.join(sqls)

		self.__conn.executescript(sql)
		self.__conn.commit()

		fields = ('id', 'ts', 'symbol', 'open', 'high', 'low', 'close', 'volume')
		self.__fields = tuple([(fields[i], i) for i in range(len(fields))])
		self.__names = {}
		for k, v in self.__fields:
			self.__names[k] = v
		self.__enable = self.__fields[3:]

		return 0


#----------------------------------------------------------------------
# testing case
#----------------------------------------------------------------------
if __name__ == '__main__':
	def test1():
		c = CandleStick(1, 2, 3, 4, 5, 100)
		print(c)
		cl = CandleLite('test.db')
		return 0
	test1()




