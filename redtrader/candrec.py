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
	def __init__ (self, t = 0, o = 0.0, hi = 0.0, lo = 0.0, c = 0.0, v = 0):
		self.ts = t
		self.open = o
		self.high = hi
		self.low = lo
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

		self.__tabname = {}
		self.__tabname['1'] = 'candle_1'
		self.__tabname['5'] = 'candle_5'
		self.__tabname['15'] = 'candle_15'
		self.__tabname['30'] = 'candle_30'
		self.__tabname['60'] = 'candle_60'
		self.__tabname['h'] = 'candle_60'
		self.__tabname['d'] = 'candle_d'
		self.__tabname['w'] = 'candle_w'
		self.__tabname['m'] = 'candle_m'

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

	def write (self, symbol, candle, mode = 'd', rep = True, commit = True):
		tabname = self.__get_table_name(mode)
		record = None
		if isinstance(candle, CandleStick):
			record = candle.record()
		elif isinstance(candle, tuple):
			record = candle
		else:
			record = tuple(candle)
		symbol = symbol.replace('\'', '').replace('"', '')
		sql = '%s INTO %s (symbol, ts, open, high, low, close, volume)'
		sql = sql%(rep and 'REPLACE' or 'INSERT', tabname)
		sql += " values('%s', ?, ?, ?, ?, ?, ?);"%symbol
		try:
			self.__conn.execute(sql, record)
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
# CandleDB
#----------------------------------------------------------------------
class CandleDB (object):

	def __init__ (self, desc, init = False, timeout = 10, verbose = False):
		self.__argv = {}
		self.__uri = {}
		if isinstance(desc, dict):
			argv = desc
		else:
			argv = self.__url_parse(desc)
		for k, v in argv.items():
			self.__argv[k] = v
			if k not in ('engine', 'init', 'db', 'verbose'):
				self.__uri[k] = v
		self.__uri['connect_timeout'] = timeout
		self.__conn = None
		self.__verbose = verbose
		self.__init = init
		if 'db' not in argv:
			raise KeyError('not find db name')
		self.__open()

	def __mysql_startup (self):
		global MySQLdb
		if MySQLdb is not None:
			return True
		try:
			import MySQLdb as _mysql
			MySQLdb = _mysql
		except ImportError:
			return False
		return True

	def __open (self):
		self.__mysql_startup()
		if MySQLdb is None:
			raise ImportError('No module named MySQLdb')
		self.__dbname = self.__argv.get('db', 'CandleDB')
		self.__tabname = {}
		self.__tabname['1'] = 'candle_1'
		self.__tabname['5'] = 'candle_5'
		self.__tabname['15'] = 'candle_15'
		self.__tabname['30'] = 'candle_30'
		self.__tabname['60'] = 'candle_60'
		self.__tabname['h'] = 'candle_60'
		self.__tabname['d'] = 'candle_d'
		self.__tabname['w'] = 'candle_w'
		self.__tabname['m'] = 'candle_m'
		if not self.__init:
			uri = {}
			for k, v in self.__uri.items():
				uri[k] = v
			uri['db'] = self.__dbname
			self.__conn = MySQLdb.connect(**uri)
		else:
			self.__conn = MySQLdb.connect(**self.__uri)
			return self.init()
		return True

	# 输出日志
	def out (self, text):
		if self.__verbose:
			print(text)
		return True

	# 初始化数据库与表格
	def init (self):
		database = self.__argv.get('db', 'CandleDB')
		self.out('create database: %s'%database)
		self.__conn.query('SET sql_notes = 0;')
		self.__conn.query('CREATE DATABASE IF NOT EXISTS %s;'%database)
		self.__conn.query('USE %s;'%database)
		sql = '''
			CREATE TABLE IF NOT EXISTS `%s`.`{name}` (
			`id` INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
			`ts` INT UNSIGNED DEFAULT 0,
			`symbol` VARCHAR(16) NOT NULL,
			`open` DOUBLE DEFAULT 0,
			`high` DOUBLE DEFAULT 0,
			`low` DOUBLE DEFAULT 0,
			`close` DOUBLE DEFAULT 0,
			`volume` DOUBLE DEFAULT 0,
			UNIQUE KEY `tssym` (`ts`, `symbol`),
			UNIQUE KEY `symts` (`symbol`, `ts`),
			KEY(`ts`),
			KEY(`symbol`)
			)
		'''%(database)

		sql = '\n'.join([ n.strip('\t') for n in sql.split('\n') ])
		sql = sql.strip('\n')
		sql += ' ENGINE=MyISAM DEFAULT CHARSET=utf8;'

		self.__conn.query(sql.replace('{name}', 'candle_1'))
		self.__conn.query(sql.replace('{name}', 'candle_5'))
		self.__conn.query(sql.replace('{name}', 'candle_15'))
		self.__conn.query(sql.replace('{name}', 'candle_30'))
		self.__conn.query(sql.replace('{name}', 'candle_60'))
		self.__conn.query(sql.replace('{name}', 'candle_d'))
		self.__conn.query(sql.replace('{name}', 'candle_w'))
		self.__conn.query(sql.replace('{name}', 'candle_m'))
		self.__conn.commit()

		return True

	# 读取 mysql://user:passwd@host:port/database
	def __url_parse (self, url):
		if url[:8] != 'mysql://':
			return None
		url = url[8:]
		obj = {}
		part = url.split('/')
		main = part[0]
		p1 = main.find('@')
		if p1 >= 0:
			text = main[:p1].strip()
			main = main[p1 + 1:]
			p1 = text.find(':')
			if p1 >= 0:
				obj['user'] = text[:p1].strip()
				obj['passwd'] = text[p1 + 1:].strip()
			else:
				obj['user'] = text
		p1 = main.find(':')
		if p1 >= 0:
			port = main[p1 + 1:]
			main = main[:p1]
			obj['port'] = int(port)
		main = main.strip()
		if not main:
			main = 'localhost'
		obj['host'] = main.strip()
		if len(part) >= 2:
			obj['db'] = part[1]
		return obj

	def close (self):
		if self.__conn:
			self.__conn.close()
		self.__conn = None

	def __del__ (self):
		self.close()

	def verbose (self, verbose):
		self.__verbose = verbose

	def __get_table_name (self, mode):
		return self.__tabname[str(mode).lower()]

	def read (self, symbol, start, end, mode = 'd'):
		tabname = self.__get_table_name(mode)
		sql = 'select ts, open, high, low, close, volume '
		sql += ' from {} where symbol = %s '.format(tabname)
		sql += ' and ts >= %s and ts < %s order by ts;'
		record = []
		with self.__conn as c:
			c.execute(sql, (symbol, start, end))
			for obj in c.fetchall():
				cs = CandleStick(*obj)
				record.append(cs)
		return record

	def read_first (self, symbol, mode = 'd'):
		tabname = self.__get_table_name(mode)
		sql = 'select ts, open, high, low, close, volume '
		sql += ' from {} where symbol = %s order by ts limit 1;'
		with self.__conn as c:
			c.execute(sql.format(tabname), (symbol, ))
			record = c.fetchone()
		if record is None:
			return None
		return record

	def read_last (self, symbol, mode = 'd'):
		tabname = self.__get_table_name(mode)
		sql = 'select ts, open, high, low, close, volume '
		sql += ' from {} where symbol = %s order by ts desc limit 1;'
		with self.__conn as c:
			c.execute(sql.format(tabname), (symbol, ))
			record = c.fetchone()
		if record is None:
			return None
		return record

	def write (self, symbol, candle, mode = 'd', rep = True, commit = True):
		tabname = self.__get_table_name(mode)
		record = None
		if isinstance(candle, CandleStick):
			record = candle.record()
		elif isinstance(candle, tuple):
			record = candle
		else:
			record = tuple(candle)
		symbol = symbol.replace('\'', '').replace('"', '')
		sql = '%s INTO %s (symbol, ts, open, high, low, close, volume)'
		sql = sql%(rep and 'REPLACE' or 'INSERT', tabname)
		sql += " values('{}', %s, %s, %s, %s, %s, %s);".format(symbol)
		try:
			with self.__conn as c:
				c.execute(sql, record)
		except MySQLdb.Error as e:
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
		sql = 'DELETE FROM {} WHERE symbol = %s and ts >= %s and ts < %s;'
		try:
			with self.__conn as c:
				c.execute(sql.format(tabname), (symbol, start, end))
			if commit:
				self.__conn.commit()
		except MySQLdb.Error as e:
			self.out(str(e))
			return False
		return True

	def delete_all (self, symbol, mode = 'd'):
		tabname = self.__get_table_name(mode)
		sql = 'DELETE FROM %s WHERE symbol = ?;'%tabname
		try:
			with self.__conn as c:
				c.execute(sql, (symbol, ))
				c.commit()
		except MySQLdb.Error as e:
			self.out(str(e))
			return False
		return True


#----------------------------------------------------------------------
# testing case
#----------------------------------------------------------------------
if __name__ == '__main__':
	my = {'host':'127.0.0.1', 'user':'skywind', 'passwd':'000000', 'db':'skywind_t2'}
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
	def test3():
		records1 = []
		records2 = []
		for i in xrange(1000):
			records1.append(CandleStick(i))
			records2.append(CandleStick(1000000 + i))
		cc = CandleLite('test.db')
		print('remove')
		cc.delete_all('ETH/USDT')
		print('begin')
		t1 = time.time()
		for rec in records1:
			cc.write('ETH/USDT', rec, commit = False)
		print('time', time.time() - t1)
		t1 = time.time()
		for rec in records2:
			cc.write('ETH/USDT', rec, commit = False)
		cc.commit()
		print('time', time.time() - t1)
		print(cc.read_first('ETH/USDT'))
		print(cc.read_last('ETH/USDT'))
		print()
		for n in cc.read('ETH/USDT', 10, 20):
			print(n)
		return 0
	def test4():
		cc = CandleDB(my, init = True)
		symbol = 'ETH/USDT'
		for n in cc.read(symbol, 0, 0xffffffff):
			print(n)
		return 0

	test4()




