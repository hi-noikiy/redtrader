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
	def record (self):
		v = self.ts, self.open, self.high, self.low, self.close, self.volume
		return v



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
			"volume" REAL DEFAULT(0),
			CONSTRAINT 'ukey' UNIQUE (ts, symbol)
		);
		CREATE UNIQUE INDEX IF NOT EXISTS "{name}_1" ON {name} (ts, symbol);
		CREATE UNIQUE INDEX IF NOT EXISTS "{name}_2" ON {name} (symbol, ts);
		CREATE UNIQUE INDEX IF NOT EXISTS "{name}_3" ON {name} (symbol, ts desc);
		CREATE INDEX IF NOT EXISTS "{name}_4" ON {name} (ts);
		CREATE INDEX IF NOT EXISTS "{name}_5" ON {name} (symbol);
		'''

		self.__conn = sqlite3.connect(self.__dbname, isolation_level = "IMMEDIATE")
		self.__conn.isolation_level = "IMMEDIATE"

		sql = '\n'.join([ n.strip('\t') for n in sql.split('\n') ])
		sql = sql.strip('\n')

		sqls = []
		sqls.append(sql.replace('{name}', 'candle_1'))
		sqls.append(sql.replace('{name}', 'candle_5'))
		sqls.append(sql.replace('{name}', 'candle_15'))
		sqls.append(sql.replace('{name}', 'candle_30'))
		sqls.append(sql.replace('{name}', 'candle_60'))
		sqls.append(sql.replace('{name}', 'candle_d'))
		sqls.append(sql.replace('{name}', 'candle_w'))
		sqls.append(sql.replace('{name}', 'candle_m'))

		sql = '\n\n'.join(sqls)

		self.__conn.executescript(sql)
		self.__conn.commit()

		fields = ('id', 'ts', 'symbol', 'open', 'high', 'low', 'close', 'volume')
		self.__fields = tuple([(fields[i], i) for i in range(len(fields))])
		self.__names = {}
		for k, v in self.__fields:
			self.__names[k] = v
		self.__enable = self.__fields[3:]

		tabnames = {}
		tabnames['1'] = 'candle_1'
		tabnames['5'] = 'candle_5'
		tabnames['15'] = 'candle_15'
		tabnames['30'] = 'candle_30'
		tabnames['60'] = 'candle_60'
		tabnames['h'] = 'candle_60'
		tabnames['d'] = 'candle_d'
		tabnames['w'] = 'candle_w'
		tabnames['m'] = 'candle_m'

		self.__tabname = tabnames

		return 0

	def close (self):
		if self.__conn:
			self.__conn.close()
		self.__conn = None

	def __del__ (self):
		self.close()

	def out (self, text):
		if self.__verbose:
			print(text)
		return True

	def verbose (self, verbose):
		self.__verbose = verbose

	def __get_table_name (self, mode):
		return self.__tabname[str(mode).lower()]

	def read (self, symbol, start, end, mode = 'd'):
		tabname = self.__get_table_name(mode)
		sql = 'select ts, open, high, low, close, volume '
		sql += ' from %s where symbol = ? '%tabname
		sql += ' and ts >= ? and ts < ? order by ts;'
		record = []
		c = self.__conn.cursor()
		c.execute(sql, (symbol, start, end))
		for obj in c.fetchall():
			cs = CandleStick(*obj)
			record.append(cs)
		c.close()
		return record

	def read_first (self, symbol, mode = 'd'):
		tabname = self.__get_table_name(mode)
		sql = 'select ts, open, high, low, close, volume '
		sql += ' from %s where symbol = ? order by ts limit 1;'%tabname
		c = self.__conn.cursor()
		c.execute(sql, (symbol, ))
		record = c.fetchone()
		c.close()
		if record is None:
			return None
		return CandleStick(*record)

	def read_last (self, symbol, mode = 'd'):
		tabname = self.__get_table_name(mode)
		sql = 'select ts, open, high, low, close, volume '
		sql += ' from %s where symbol = ? order by ts desc limit 1;'%tabname
		c = self.__conn.cursor()
		c.execute(sql, (symbol, ))
		record = c.fetchone()
		c.close()
		if record is None:
			return None
		return CandleStick(*record)

	def write (self, symbol, candles, mode = 'd', rep = True, commit = True):
		tabname = self.__get_table_name(mode)
		records = []
		if isinstance(candles, CandleStick):
			records.append(candles.record())
		else:
			for candle in candles:
				if isinstance(candle, CandleStick):
					records.append(candle.record())
				elif isinstance(candle, list):
					records.append(tuple(candle))
				elif isinstance(candle, tuple):
					records.append(candle)

		symbol = symbol.replace('\'', '').replace('"', '')
		sql = '%s INTO %s (symbol, ts, open, high, low, close, volume)'
		sql = sql%(rep and 'REPLACE' or 'INSERT', tabname)
		sql += " values('%s', ?, ?, ?, ?, ?, ?);"%symbol

		try:
			self.__conn.executemany(sql, records)
		except sqlite3.InternalError as e:
			self.out(str(e))
			return False
		except sqlite3.Error as e:
			self.out(str(e))
			return False

		if commit:
			self.__conn.commit()

		return True

	def commit (self):
		if self.__conn:
			self.__conn.commit()
		return True

	def delete (self, symbol, start, end, mode = 'd', commit = True):
		tabname = self.__get_table_name(mode)
		sql = 'DELETE FROM %s WHERE symbol = ? and ts >= ? and ts < ?;'
		try:
			self.__conn.execute(sql%tabname, (symbol, start, end))
			if commit:
				self.__conn.commit()
		except sqlite3.InternalError as e:
			self.out(str(e))
			return False
		except sqlite3.Error as e:
			self.out(str(e))
			return False
		return True

	def delete_all (self, symbol, mode = 'd'):
		tabname = self.__get_table_name(mode)
		sql = 'DELETE FROM %s WHERE symbol = ?;'%tabname
		try:
			self.__conn.execute(sql, (symbol, ))
			self.__conn.commit()
		except sqlite3.InternalError as e:
			self.out(str(e))
			return False
		except sqlite3.Error as e:
			self.out(str(e))
			return False
		return True

			

#----------------------------------------------------------------------
# testing case
#----------------------------------------------------------------------
if __name__ == '__main__':
	def test1():
		c = CandleStick(1, 2, 3, 4, 5, 100)
		print(c)
		cl = CandleLite('test.db')
		print(cl.read('ETH/USDT', 0, 0xffffffff))
		return 0
	def test2():
		cc = CandleLite('test.db')
		cc.verbose(True)
		cc.delete_all('ETH/USDT')
		c1 = CandleStick(1, 2, 3, 4, 5, 100)
		c2 = CandleStick(2, 2, 3, 4, 5, 100)
		c3 = CandleStick(3, 2, 3, 4, 5, 100)
		hr = cc.write('ETH/USDT', [c1, c2], rep = True)
		hr = cc.write('ETH/USDT', [c2, c3], rep = True)
		print(hr)
		for n in cc.read('ETH/USDT', 0, 0xffffffff):
			print(n)
		return 0
	test2()




