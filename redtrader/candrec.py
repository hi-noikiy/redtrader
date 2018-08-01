#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim: set ts=4 sw=4 tw=0 noet :
#======================================================================
#
# candrec.py - candle stick database
#
# Created by skywind on 2018/07/23
# Last Modified: 2018/07/30 16:44
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
	def __init__ (self, ts = 0, open = 0, high = 0, low = 0, 
			close = 0, volume = 0, extra = None):
		self.ts = ts
		self.open = open
		self.high = high
		self.low = low
		self.close = close
		self.volume = volume
		self.extra = extra
	def __repr__ (self):
		text = 'CandleStick({}, {}, {}, {}, {}, {}, {})'
		v = (self.ts, repr(self.open), repr(self.high), repr(self.low), 
				repr(self.close), repr(self.volume), repr(self.extra))
		return text.format(*v)
	def __add__ (self, other):
		nc = CandleStick(min(self.t, other.t),
			self.open, max(self.high, other.high),
			min(self.low, other.low), other.close,
			self.volume + other.volume)
		return nc


#----------------------------------------------------------------------
# TickData
#----------------------------------------------------------------------
class TickData (object):
	def __init__ (self, ts, obj):
		self.ts = ts
		self.obj = obj
	def __repr__ (self):
		return 'TickData({}, {})'.format(self.ts, repr(self.obj))


#----------------------------------------------------------------------
# CandleLite
#----------------------------------------------------------------------
class CandleLite (object):

	def __init__ (self, filename, verbose = False):
		self.__dbname = filename
		if filename != ':memory:':
			if '~' in filename:
				filename = os.path.expanduser(filename)
			os.path.abspath(filename)
		self.__conn = None
		self.verbose = verbose
		self.decimal = 0
		if sys.platform[:3] != 'win':
			self.uri = 'sqlite://' + self.__dbname
		else:
			self.uri = 'sqlite://' + self.__dbname.replace('\\', '/')
		self.ctime = None
		self.atime = None
		self.__open()

	def __open (self):
		sql = '''
		CREATE TABLE IF NOT EXISTS "{name}" (
			"id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
			"ts" INTEGER DEFAULT(0) NOT NULL,
			"symbol" VARCHAR(16) NOT NULL,
			"open" DECIMAL(32, 16) DEFAULT(0),
			"high" DECIMAL(32, 16) DEFAULT(0),
			"low" DECIMAL(32, 16) DEFAULT(0),
			"close" DECIMAL(32, 16) DEFAULT(0),
			"volume" DECIMAL(32, 16) DEFAULT(0),
			"extra" TEXT,
			CONSTRAINT 'tssym' UNIQUE (ts, symbol)
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

		sql = '''
		CREATE TABLE IF NOT EXISTS "{name}" (
			"id" INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
			"ts" INTEGER DEFAULT(0) NOT NULL,
			"symbol" VARCHAR(16) NOT NULL,
			"data" TEXT,
			CONSTRAINT 'tssym' UNIQUE (ts, symbol)
		);
		CREATE UNIQUE INDEX IF NOT EXISTS "{name}_1" ON {name} (ts, symbol);
		CREATE UNIQUE INDEX IF NOT EXISTS "{name}_2" ON {name} (symbol, ts);
		CREATE UNIQUE INDEX IF NOT EXISTS "{name}_3" ON {name} (symbol, ts desc);
		CREATE INDEX IF NOT EXISTS "{name}_4" ON {name} (ts);
		CREATE INDEX IF NOT EXISTS "{name}_5" ON {name} (symbol);
		'''

		sql = '\n'.join([ n.strip('\t') for n in sql.split('\n') ])
		sql = sql.strip('\n')

		sqls.append(sql.replace('{name}', 'tick_1'))
		sqls.append(sql.replace('{name}', 'tick_2'))
		sqls.append(sql.replace('{name}', 'tick_3'))
		sqls.append(sql.replace('{name}', 'tick_4'))

		sql = '''
		CREATE TABLE IF NOT EXISTS "meta" (
			"name" VARCHAR(16) PRIMARY KEY COLLATE NOCASE NOT NULL UNIQUE,
			"value" TEXT,
			"ctime" DATETIME NOT NULL DEFAULT (datetime('now', 'localtime')),
			"mtime" DATETIME NOT NULL DEFAULT (datetime('now', 'localtime'))
		);
		'''

		sql = '\n'.join([ n.strip('\t') for n in sql.split('\n') ])
		sqls.append(sql.strip('\n'))
		
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
		if self.verbose:
			print(text)
		return True

	def commit (self):
		if self.__conn:
			self.__conn.commit()
		return True

	def __get_candle_table (self, mode):
		return self.__tabname[str(mode).lower()]

	def __get_tick_table (self, mode):
		return 'tick_{}'.format(str(mode))

	def __record2candle (self, record):
		if record is None:
			return None
		cs = CandleStick()
		cs.ts = int(record[0])
		if self.decimal == 0:
			cs.open = record[1]
			cs.high = record[2]
			cs.low = record[3]
			cs.close = record[4]
			cs.volume = record[5]
		elif self.decimal == 1:
			cs.open = decimal.Decimal(record[1])
			cs.high = decimal.Decimal(record[2])
			cs.low = decimal.Decimal(record[3])
			cs.close = decimal.Decimal(record[4])
			cs.volume = decimal.Decimal(record[5])
		else:
			cs.open = float(record[1])
			cs.high = float(record[2])
			cs.low = float(record[3])
			cs.close = float(record[4])
			cs.volume = float(record[5])
		if record[6] is None:
			cs.extra = None
		else:
			try:
				cs.extra = json.loads(record[6])
			except:
				pass
		return cs

	def __candle2record (self, cs):
		e = None
		if cs.extra is not None:
			e = json.dumps(cs.extra)
		return (cs.ts, cs.open, cs.high, cs.low, cs.close, cs.volume, e)

	def __record2tick (self, record):
		if record is None:
			return None
		tick = TickData(record[0], None)
		if record[1] is not None:
			try:
				tick.obj = json.loads(record[1])
			except:
				pass
		return tick

	def __tick2record (self, tick):
		e = None
		if tick.obj is not None:
			e = json.dumps(tick.obj)
		return (tick.ts, e)

	def candle_read (self, symbol, start, end, mode = 'd'):
		tabname = self.__get_candle_table(mode)
		sql = 'select ts, open, high, low, close, volume, extra '
		sql += ' from %s where symbol = ? '%tabname
		sql += ' and ts >= ? and ts < ? order by ts;'
		record = []
		c = self.__conn.cursor()
		c.execute(sql, (symbol, start, end))
		for obj in c.fetchall():
			cs = self.__record2candle(obj)
			if cs is not None:
				record.append(cs)
		c.close()
		return record

	# pos: head(-2), tail(-1)	
	def candle_pick (self, symbol, pos, mode = 'd'):
		tabname = self.__get_candle_table(mode)
		c = self.__conn.cursor()
		sql = 'select ts, open, high, low, close, volume, extra from %s'%tabname
		if pos < 0:
			if pos == -1:
				sql += ' where symbol = ? order by ts desc limit 1;'
			else:
				sql += ' where symbol = ? order by ts limit 1;'
			c.execute(sql, (symbol, ))
		else:
			sql += ' where symbol = ? and ts <= ? order by ts desc limit 1;'
			c.execute(sql, (symbol, pos))
		record = c.fetchone()
		c.close()
		return self.__record2candle(record)

	def candle_write (self, symbol, candles, mode = 'd', commit = True):
		tabname = self.__get_candle_table(mode)
		if isinstance(candles, CandleStick):
			records = [ self.__candle2record(candles) ]
		else:
			records = [ self.__candle2record(candle) for candle in candles ]
		symbol = symbol.replace('\'', '').replace('"', '').replace('\\', '')
		sql = 'REPLACE INTO %s '%tabname
		sql += '(symbol, ts, open, high, low, close, volume, extra)'
		sql += ' VALUES(\'{}\', ?, ?, ?, ?, ?, ?, ?);'.format(symbol)
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

	def candle_erase (self, symbol, start, end, mode = 'd', commit = True):
		tabname = self.__get_candle_table(mode)
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

	def candle_empty (self, symbol, mode = 'd'):
		tabname = self.__get_candle_table(mode)
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

	def tick_read (self, symbol, start, end, mode = 1):
		tabname = self.__get_tick_table(mode)
		sql = 'select ts, data from {} where symbol = ?'.format(tabname)
		sql += ' and ts >= ? and ts < ? order by ts;'
		record = []
		c = self.__conn.cursor()
		c.execute(sql, (symbol, start, end))
		for obj in c.fetchall():
			tick = self.__record2tick(obj)
			if tick is not None: 
				record.append(tick)
		c.close()
		return record

	# pos: head(-2), tail(-1)	
	def tick_pick (self, symbol, pos, mode = 1):
		tabname = self.__get_tick_table(mode)
		c = self.__conn.cursor()
		sql = 'select ts, data from %s'%tabname
		if pos < 0:
			if pos == -1:
				sql += ' where symbol = ? order by ts desc limit 1;'
			else:
				sql += ' where symbol = ? order by ts limit 1;'
			c.execute(sql, (symbol, ))
		else:
			sql += ' where symbol = ? and ts <= ? order by ts desc limit 1;'
			c.execute(sql, (symbol, pos))
		record = c.fetchone()
		c.close()
		return self.__record2tick(record)

	def tick_write (self, symbol, ticks, mode = 1, commit = True):
		tabname = self.__get_tick_table(mode)
		if isinstance(ticks, TickData):
			records = [ self.__tick2record(ticks) ]
		else:
			records = [ self.__tick2record(tick) for tick in ticks ]
		symbol = symbol.replace('\'', '').replace('"', '').replace('\\', '')
		sql = 'REPLACE INTO %s (symbol, ts, data) '%tabname
		sql += ' VALUES (\'{}\', ?, ?);'.format(symbol)
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

	def tick_erase (self, symbol, start, end, mode = 1, commit = True):
		tabname = self.__get_tick_table(mode)
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

	def tick_empty (self, symbol, mode = 1):
		tabname = self.__get_tick_table(mode)
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

	# write meta information
	def meta_write (self, name, value, commit = True):
		sql1 = 'insert or ignore into meta(name, value, ctime, mtime)'
		sql1 += ' values(?, ?, ?, ?);'
		sql2 = 'UPDATE meta SET value=?, mtime=? WHERE name=?;'
		now = time.strftime('%Y-%m-%d %H:%M:%S')
		value = json.dumps(value)
		try:
			self.__conn.execute(sql1, (name, value, now, now))
			self.__conn.execute(sql2, (value, now, name))
			if commit:
				self.__conn.commit()
		except sqlite3.IntegrityError:
			return False
		return True

	# read meta infomation
	def meta_read (self, name):
		c = self.__conn.cursor()
		c.execute('select value, ctime, mtime from meta where name=?;', (name,))
		record = c.fetchone()
		if record is None:
			return None
		self.ctime = record[1]
		self.mtime = record[2]
		return json.loads(record[0])



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
		self.verbose = verbose
		self.__init = init
		self.decimal = 0
		if 'db' not in argv:
			raise KeyError('not find db name')
		self.uri = 'mysql://'
		if 'user' in argv:
			self.uri += argv['user']
			if 'passwd' in argv:
				self.uri += ':' + argv['passwd']
			self.uri += '@'
		self.uri += argv['host']
		if 'port' in argv:
			self.uri += ':' + str(argv['port'])
		self.uri += '/' + argv['db']
		self.ctime = None
		self.mtime = None
		self.__open()

	def __mysql_startup (self):
		global MySQLdb
		if MySQLdb is not None:
			return True
		try:
			import MySQLdb as _mysql
			MySQLdb = _mysql
		except ImportError:
			try:
				import pymysql
				MySQLdb = pymysql
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
		if self.verbose:
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
			`open` DECIMAL(32, 16) DEFAULT 0,
			`high` DECIMAL(32, 16) DEFAULT 0,
			`low` DECIMAL(32, 16) DEFAULT 0,
			`close` DECIMAL(32, 16) DEFAULT 0,
			`volume` DECIMAL(32, 16) DEFAULT 0,
			`extra` TEXT,
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

		sql = '''
			CREATE TABLE IF NOT EXISTS `%s`.`{name}` (
			`id` INT PRIMARY KEY NOT NULL AUTO_INCREMENT,
			`ts` INT UNSIGNED DEFAULT 0,
			`symbol` VARCHAR(16) NOT NULL,
			`data` TEXT,
			UNIQUE KEY `tssym` (`ts`, `symbol`),
			UNIQUE KEY `symts` (`symbol`, `ts`),
			KEY(`ts`),
			KEY(`symbol`)
			)
		'''%(database)

		sql = '\n'.join([ n.strip('\t') for n in sql.split('\n') ])
		sql = sql.strip('\n')
		sql += ' ENGINE=MyISAM DEFAULT CHARSET=utf8;'

		self.__conn.query(sql.replace('{name}', 'tick_1'))
		self.__conn.query(sql.replace('{name}', 'tick_2'))
		self.__conn.query(sql.replace('{name}', 'tick_3'))
		self.__conn.query(sql.replace('{name}', 'tick_4'))

		sql = '''
			CREATE TABLE IF NOT EXISTS `%s`.`meta` (
			`name` VARCHAR(16) PRIMARY KEY NOT NULL UNIQUE,
			`value` TEXT,
			`ctime` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00',
			`mtime` DATETIME NOT NULL DEFAULT '0000-00-00 00:00:00'
			)
		'''%(database)

		sql = '\n'.join([ n.strip('\t') for n in sql.split('\n') ])
		sql = sql.strip('\n')
		sql += ' ENGINE=InnoDB DEFAULT CHARSET=utf8;'

		self.__conn.query(sql)
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

	def commit (self):
		if self.__conn:
			self.__conn.commit()
		return True

	def __get_candle_table (self, mode):
		return self.__tabname[str(mode).lower()]

	def __get_tick_table (self, mode):
		return 'tick_{}'.format(str(mode))

	def __record2candle (self, record):
		if record is None:
			return None
		cs = CandleStick()
		cs.ts = int(record[0])
		if self.decimal == 0:
			cs.open = record[1]
			cs.high = record[2]
			cs.low = record[3]
			cs.close = record[4]
			cs.volume = record[5]
		elif self.decimal == 1:
			cs.open = decimal.Decimal(record[1])
			cs.high = decimal.Decimal(record[2])
			cs.low = decimal.Decimal(record[3])
			cs.close = decimal.Decimal(record[4])
			cs.volume = decimal.Decimal(record[5])
		else:
			cs.open = float(record[1])
			cs.high = float(record[2])
			cs.low = float(record[3])
			cs.close = float(record[4])
			cs.volume = float(record[5])
		if record[6] is None:
			cs.extra = None
		else:
			try:
				cs.extra = json.loads(record[6])
			except:
				pass
		return cs

	def __candle2record (self, cs):
		e = None
		if cs.extra is not None:
			e = json.dumps(cs.extra)
		return (cs.ts, cs.open, cs.high, cs.low, cs.close, cs.volume, e)

	def __record2tick (self, record):
		if record is None:
			return None
		tick = TickData(record[0], None)
		if record[1] is not None:
			try:
				tick.obj = json.loads(record[1])
			except:
				pass
		return tick

	def __tick2record (self, tick):
		e = None
		if tick.obj is not None:
			e = json.dumps(tick.obj)
		return (tick.ts, e)

	def candle_read (self, symbol, start, end, mode = 'd'):
		tabname = self.__get_candle_table(mode)
		sql = 'select ts, open, high, low, close, volume, extra '
		sql += ' from {} where symbol = %s '.format(tabname)
		sql += ' and ts >= %s and ts < %s order by ts;'
		record = []
		with self.__conn as c:
			c.execute(sql, (symbol, start, end))
			for obj in c.fetchall():
				cs = self.__record2candle(obj)
				if cs is not None:
					record.append(cs)
		return record

	# pos: head(-2), tail(-1)
	def candle_pick (self, symbol, pos, mode = 'd'):
		tabname = self.__get_candle_table(mode)
		sql = 'select ts, open, high, low, close, volume, extra from %s'%tabname
		with self.__conn as c:
			if pos < 0:
				if pos == -1:
					sql += ' where symbol = %s order by ts desc limit 1;'
					c.execute(sql, (symbol, ))
				else:
					sql += ' where symbol = %s order by ts limit 1;'
					c.execute(sql, (symbol, ))
			else:
				sql += ' where symbol = %s and ts <= %s order by ts desc limit 1;'
				c.execute(sql, (symbol, pos))
			record = c.fetchone()
		return self.__record2candle(record)

	def candle_write (self, symbol, candles, mode = 'd', commit = True):
		tabname = self.__get_candle_table(mode)
		if isinstance(candles, CandleStick):
			records = [ self.__candle2record(candles) ]
		else:
			records = [ self.__candle2record(candle) for candle in candles ]
		symbol = symbol.replace('\'', '').replace('"', '').replace('\\', '')
		sql = 'REPLACE INTO %s'%tabname
		sql += ' (symbol, ts, open, high, low, close, volume, extra)'
		sql += " values(\'{}\', %s, %s, %s, %s, %s, %s, %s);".format(symbol)
		try:
			with self.__conn as c:
				c.executemany(sql, records)
			if commit:
				self.__conn.commit()
		except MySQLdb.Error as e:
			self.out(str(e))
			return False
		return True

	def candle_erase (self, symbol, start, end, mode = 'd', commit = True):
		tabname = self.__get_candle_table(mode)
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

	def candle_empty (self, symbol, mode = 'd'):
		tabname = self.__get_candle_table(mode)
		sql = 'DELETE FROM {} WHERE symbol = %s;'.format(tabname)
		try:
			with self.__conn as c:
				c.execute(sql, (symbol, ))
				self.__conn.commit()
		except MySQLdb.Error as e:
			self.out(str(e))
			return False
		return True

	def tick_read (self, symbol, start, end, mode = 1):
		tabname = self.__get_tick_table(mode)
		sql = 'select ts, data from {} where symbol = %s'.format(tabname)
		sql += ' and ts >= %s and ts < %s order by ts;'
		record = []
		with self.__conn as c:
			c.execute(sql, (symbol, start, end))
			for obj in c.fetchall():
				tick = self.__record2tick(obj)
				if tick is not None:
					record.append(tick)
		return record

	# pos: head(-2), tail(-1)
	def tick_pick (self, symbol, pos, mode = 1):
		tabname = self.__get_tick_table(mode)
		sql = 'select ts, data from %s'%tabname
		with self.__conn as c:
			if pos < 0:
				if pos == -1:
					sql += ' where symbol = %s order by ts desc limit 1;'
					c.execute(sql, (symbol, ))
				else:
					sql += ' where symbol = %s order by ts limit 1;'
					c.execute(sql, (symbol, ))
			else:
				sql += ' where symbol = %s and ts <= %s order by ts desc limit 1;'
				c.execute(sql, (symbol, pos))
			record = c.fetchone()
		return self.__record2tick(record)

	def tick_write (self, symbol, ticks, mode = 1, commit = True):
		tabname = self.__get_tick_table(mode)
		if isinstance(ticks, TickData):
			records = [ self.__tick2record(ticks) ]
		else:
			records = [ self.__tick2record(tick) for tick in ticks ]
		symbol = symbol.replace('\'', '').replace('"', '').replace('\\', '')
		sql = 'REPLACE INTO %s (symbol, ts, data)'%tabname
		sql += " values(\'{}\', %s, %s);".format(symbol)
		try:
			with self.__conn as c:
				c.executemany(sql, records)
			if commit:
				self.__conn.commit()
		except MySQLdb.Error as e:
			self.out(str(e))
			return False
		return True

	def tick_erase (self, symbol, start, end, mode = 1, commit = True):
		tabname = self.__get_tick_table(mode)
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

	def tick_empty (self, symbol, mode = 1):
		tabname = self.__get_tick_table(mode)
		sql = 'DELETE FROM {} WHERE symbol = %s;'.format(tabname)
		try:
			with self.__conn as c:
				c.execute(sql, (symbol, ))
				self.__conn.commit()
		except MySQLdb.Error as e:
			self.out(str(e))
			return False
		return True

	# write meta information
	def meta_write (self, name, value, commit = True):
		sql1 = 'insert ignore into meta(name, value, ctime, mtime)'
		sql1 += ' values(%s, %s, %s, %s);'
		sql2 = 'UPDATE meta SET value=%s, mtime=%s WHERE name=%s;'
		now = time.strftime('%Y-%m-%d %H:%M:%S')
		value = json.dumps(value)
		try:
			with self.__conn as c:
				c.execute(sql1, (name, value, now, now))
				c.execute(sql2, (value, now, name))
			if commit:
				self.__conn.commit()
		except MySQLdb.Error as e:
			self.out(str(e))
			return False
		return True

	# read meta infomation
	def meta_read (self, name):
		with self.__conn as c:
			c.execute('select value, ctime, mtime from meta where name=%s;', (name,))
			record = c.fetchone()
		if record is None:
			return None
		self.ctime = record[1]
		self.mtime = record[2]
		return json.loads(record[0])


#----------------------------------------------------------------------
# ToolHelp
#----------------------------------------------------------------------
class ToolHelp (object):
	def __init__ (self):
		self.datefmt = '%Y-%m-%d %H:%M:%S'


#----------------------------------------------------------------------
# useful functions
#----------------------------------------------------------------------
tools = ToolHelp()

def connect(uri, init = False):
	if uri.startswith('mysql://'):
		cc = CandleDB(uri, init = init)
	else:
		head = 'sqlite://'
		if uri.startswith(head):
			name = uri[len(head):]
		else:
			name = uri
		if name != ':memory:':
			if '~' in name:
				name = os.path.expanduser(name)
			name = os.path.abspath(name)
			if init:
				dirname = os.path.dirname(name)
				if not os.path.exists(dirname):
					os.makedirs(dirname)
		cc = CandleLite(name)
	return cc


#----------------------------------------------------------------------
# testing case
#----------------------------------------------------------------------
if __name__ == '__main__':
	my = {'host':'127.0.0.1', 'user':'skywind', 'passwd':'000000', 'db':'skywind_t2'}
	def test1():
		cc = CandleLite('candrec.db')
		cc.verbose = True
		cc.candle_empty('ETH/USDT')
		c1 = CandleStick(1, 2, 3, 4, 5, 100, {'name': 'skywind'})
		c2 = CandleStick(2, 2, 3, 4, 5, 100, 'haha')
		c3 = CandleStick(3, 2, 3, 4, 5, 100)
		hr = cc.candle_write('ETH/USDT', c1)
		hr = cc.candle_write('ETH/USDT', c2)
		hr = cc.candle_write('ETH/USDT', c2)
		hr = cc.candle_write('ETH/USDT', c3)
		print(hr)
		for n in cc.candle_read('ETH/USDT', 0, 0xffffffff):
			print(n)
		return 0
	def test2():
		records1 = []
		records2 = []
		for i in xrange(1000):
			records1.append(CandleStick(i, 100, time.time(), extra = 'f%d'%i))
			records2.append(CandleStick(1000000 + i))
		cc = CandleLite('candrec.db')
		# cc = CandleLite(':memory:')
		cc.decimal = 2
		print(cc.uri)
		print('remove')
		cc.candle_empty('ETH/USDT')
		print('begin')
		t1 = time.time()
		for rec in records1:
			cc.candle_write('ETH/USDT', rec, commit = False)
		print('time', time.time() - t1)
		t1 = time.time()
		cc.candle_write('ETH/USDT', records2, commit = False)
		cc.commit()
		print('time', time.time() - t1)
		print(cc.candle_pick('ETH/USDT', -2))
		print(cc.candle_pick('ETH/USDT', -1))
		print(cc.candle_pick('ETH/USDT', 50))
		print()
		for n in cc.candle_read('ETH/USDT', 10, 20):
			print(n)
		print()
		cc.meta_write('name', 'skywind')
		print(cc.meta_read('name'))
		print(cc.mtime, cc.ctime)
		cc.meta_write('nAme', 'linwei')
		cc.commit()
		print(cc.meta_read('Name'))
		print(cc.mtime, cc.ctime)
		return 0
	def test3():
		cc = CandleDB(my, init = True)
		cc.verbose = True
		cc.decimal = 2
		cc.candle_empty('ETH/USDT')
		c1 = CandleStick(1, 2, 3, 4, 5, 100, {'name': 'skywind'})
		c2 = CandleStick(2, 2, 3, 4, 5, 100, 'haha')
		c3 = CandleStick(3, 2, 3, 4, 5, 100)
		hr = cc.candle_write('ETH/USDT', c1)
		hr = cc.candle_write('ETH/USDT', c2)
		hr = cc.candle_write('ETH/USDT', c2)
		hr = cc.candle_write('ETH/USDT', c3)
		print(hr)
		for n in cc.candle_read('ETH/USDT', 0, 0xffffffff):
			print(n)
		return 0
	def test4():
		records1 = []
		records2 = []
		for i in xrange(1000):
			records1.append(CandleStick(i, 100, time.time(), extra = 'f%d'%i))
			records2.append(CandleStick(1000000 + i))
		# cc = CandleLite('test.db')
		ts = time.time()
		cc = CandleDB(my, init = True)
		ts = time.time() - ts
		print(cc.uri)
		cc.decimal = 2
		cc.verbose = True
		print('init', ts)
		cc.candle_empty('ETH/USDT')
		print('begin')
		t1 = time.time()
		for rec in records1:
			cc.candle_write('ETH/USDT', rec, commit = True)
		print('time', time.time() - t1)
		t1 = time.time()
		# for rec in records2:
		# 	cc.candle_write('ETH/USDT', rec, commit = False)
		cc.candle_write('ETH/USDT', records2)
		cc.commit()
		print('time', time.time() - t1)
		print(cc.candle_pick('ETH/USDT', -2))
		print(cc.candle_pick('ETH/USDT', -1))
		print(cc.candle_pick('ETH/USDT', 50))
		print()
		for n in cc.candle_read('ETH/USDT', 10, 20):
			print(n)
		print()
		cc.meta_write('name', 'skywind')
		print(cc.meta_read('name'))
		print(cc.mtime, cc.ctime)
		cc.meta_write('nAme', 'linwei')
		cc.commit()
		print(cc.meta_read('Name'))
		print(cc.mtime, cc.ctime)
		return 0
	def test5():
		uri = 'mysql://skywind:000000@127.0.0.1/skywind_t2'
		uri = 'sqlite://candrec.db'
		cc = connect(uri)
		cc.verbose = True
		symbol = 'ETH/USDT'
		cc.tick_empty(symbol)
		print(cc.tick_read(symbol, 0, 0xffff))
		cc.tick_write(symbol, TickData(10, 'hello'))
		cc.tick_write(symbol, TickData(20, 'fuck'))
		cc.tick_write(symbol, TickData(20, 'suck'))
		cc.tick_write(symbol, TickData(30, 'you'))
		cc.tick_write(symbol, TickData(35, 'foo'))
		cc.commit()
		print(cc.tick_pick(symbol, -2))
		print(cc.tick_pick(symbol, -1))
		print(cc.tick_pick(symbol, 20))
		print()
		for tick in cc.tick_read(symbol, 0, 0xffffffff):
			print(tick)
		return 0
	def test6():
		uri = 'mysql://skywind:000000@127.0.0.1/skywind_t2'
		uri = 'sqlite://~/.cache/candle/candrec.db'
		cc = connect(uri, True)
		cc.verbose = True
		sym1 = 'BTC/USDT'
		sym2 = 'EOS/USDT'
		cc.decimal = 2
		cc.candle_write(sym1, CandleStick(1, 10, 20, 30))
		cc.candle_write(sym1, CandleStick(2, 10, 20, 30))
		cc.candle_write(sym1, CandleStick(3, 10, 20, 30))
		cc.candle_write(sym2, CandleStick(10, 80, 90))
		cc.candle_write(sym2, CandleStick(20, 81, 95))
		cc.candle_write(sym2, CandleStick(30, 82, 99))
		for candle in cc.candle_read(sym1, 0, 0xffff):
			print(candle)
		print()
		for candle in cc.candle_read(sym2, 0, 0xffff):
			print(candle)
		return 0

	test6()




