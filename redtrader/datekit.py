#! /usr/bin/env python
# -*- coding: utf-8 -*-
#======================================================================
#
# datekit.py - 
#
# Created by skywind on 2018/08/02
# Last Modified: 2018/08/02 15:55:48
#
#======================================================================
from __future__ import print_function
import sys
import time
import datetime


#----------------------------------------------------------------------
# timezone
#----------------------------------------------------------------------
class timezone(datetime.tzinfo):
	"""Backport of datetime.timezone.
	Notes
	-----
	Backport of datetime.timezone for Python 2.7, from Python 3.6
	documentation (https://tinyurl.com/z4cegu9), copyright Python Software
	Foundation (https://docs.python.org/3/license.html)
	"""
	__slots__ = '_offset', '_name'

	# Sentinel value to disallow None
	_Omitted = object()

	def __new__(cls, offset, name=_Omitted):
		if not isinstance(offset, datetime.timedelta):
			raise TypeError("offset must be a timedelta")
		if name is cls._Omitted:
			if not offset:
				return cls.utc
			name = None
		elif not isinstance(name, str):
			raise TypeError("name must be a string")
		if not cls._minoffset <= offset <= cls._maxoffset:
			raise ValueError("offset must be a timedelta "
							 "strictly between -timedelta(hours=24) and "
							 "timedelta(hours=24).")
		if (offset.microseconds != 0 or offset.seconds % 60 != 0):
			raise ValueError("offset must be a timedelta "
							 "representing a whole number of minutes")
		return cls._create(offset, name)

	@classmethod
	def _create(cls, offset, name=None):
		self = datetime.tzinfo.__new__(cls)
		self._offset = offset
		self._name = name
		return self

	def __getinitargs__(self):
		"""pickle support"""
		if self._name is None:
			return (self._offset,)
		return (self._offset, self._name)

	def __eq__(self, other):
		if not isinstance(other, timezone):
			return False
		return self._offset == other._offset

	def __lt__(self, other):
		raise TypeError("'<' not supported between instances of"
						" 'datetime.timezone' and 'datetime.timezone'")

	def __hash__(self):
		return hash(self._offset)

	def __repr__(self):
		if self is self.utc:
			return '%s.%s.utc' % (self.__class__.__module__,
								  self.__class__.__name__)
		if self._name is None:
			return "%s.%s(%r)" % (self.__class__.__module__,
								  self.__class__.__name__,
								  self._offset)
		return "%s.%s(%r, %r)" % (self.__class__.__module__,
								  self.__class__.__name__,
								  self._offset, self._name)

	def __str__(self):
		return self.tzname(None)

	def utcoffset(self, dt):
		if isinstance(dt, datetime.datetime) or dt is None:
			return self._offset
		raise TypeError("utcoffset() argument must be a datetime instance"
						" or None")

	def tzname(self, dt):
		if isinstance(dt, datetime.datetime) or dt is None:
			if self._name is None:
				return self._name_from_offset(self._offset)
			return self._name
		raise TypeError("tzname() argument must be a datetime instance"
						" or None")

	def dst(self, dt):
		if isinstance(dt, datetime.datetime) or dt is None:
			return None
		raise TypeError("dst() argument must be a datetime instance"
						" or None")

	def fromutc(self, dt):
		if isinstance(dt, datetime.datetime):
			if dt.tzinfo is not self:
				raise ValueError("fromutc: dt.tzinfo "
								 "is not self")
			return dt + self._offset
		raise TypeError("fromutc() argument must be a datetime instance"
						" or None")

	_maxoffset = datetime.timedelta(hours=23, minutes=59)
	_minoffset = -_maxoffset

	@staticmethod
	def _name_from_offset(delta):
		if not delta:
			return 'UTC'
		if delta < datetime.timedelta(0):
			sign = '-'
			delta = -delta
		else:
			sign = '+'
		hours, rest = divmod(delta.total_seconds(), 3600)
		hours = int(hours)
		minutes = rest // datetime.timedelta(minutes=1).total_seconds()
		minutes = int(minutes)
		return 'UTC{}{:02d}:{:02d}'.format(sign, hours, minutes)


timezone.utc = timezone._create(datetime.timedelta(0))
timezone.min = timezone._create(timezone._minoffset)
timezone.max = timezone._create(timezone._maxoffset)
timezone.cst = timezone._create(datetime.timedelta(hours = 8))

_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=timezone.utc)


if sys.version_info[0] < 3:
	datetime.timezone = timezone



#----------------------------------------------------------------------
# Tools
#----------------------------------------------------------------------
class DateTool (object):

	def __init__ (self):
		self.datefmt = '%Y-%m-%d %H:%M:%S'

	def ts2datetime (self, ts, tz = None):
		return datetime.datetime.fromtimestamp(ts, tz)

	def datetime2ts (self, dt):
		if hasattr(dt, 'timestamp'):
			return dt.timestamp()
		epoch = datetime.datetime.fromtimestamp(0, dt.tzinfo)
		return (dt - epoch).total_seconds()

	def datetime2str (self, dt):
		return dt.strftime(self.datefmt)

	def str2datetime (self, text, tz = None):
		dt = datetime.datetime.strptime(text, self.datefmt)
		if tz:
			if hasattr(tz, 'localize'):
				# in case, we have pytz
				dt = tz.localize(dt)
			else:
				dt = dt.replace(tzinfo = tz)
		return dt

	def ts2str (self, ts, tz = None):
		return self.datetime2str(self.ts2datetime(ts, tz))

	def str2ts (self, text, tz = None):
		return self.datetime2ts(self.str2datetime(text, tz))



#----------------------------------------------------------------------
# global definition
#----------------------------------------------------------------------
tools = DateTool()



#----------------------------------------------------------------------
# testing case
#----------------------------------------------------------------------
if __name__ == '__main__':
	def test1():
		ts = time.time()
		uc = datetime.datetime.utcfromtimestamp(ts)
		uc = tools.ts2datetime(ts, timezone.utc)
		print(ts)
		print(uc.tzinfo)
		print(tools.datetime2ts(uc))
		return 0
	test1()



