#! /usr/bin/env python
# -*- coding: utf-8 -*-
#======================================================================
#
# talib2.py - Technical Analysis Library 2
#
# Created by skywind on 2018/07/11
# Last Modified: 2018/07/11 22:09:26
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
# EMA
#----------------------------------------------------------------------
class EMA (object):
	def __init__ (self, mean, factor = 2):
		self.init = False
		self.m = mean
		self.f = factor
		self.x = 0.0
	def update (self, x):
		if not self.init:
			self.x = x
			self.init = True
		else:
			m, f = self.m, self.f
			self.x = ((self.x * (m + 1 - f)) + f * x) / (m + 1.0)
		return self.x


#----------------------------------------------------------------------
# Simple Moving Average
#----------------------------------------------------------------------
class SMA (object):
	def __init__ (self, size):
		self.n = size
		self.d = {}
		self.x = 0
		self.y = 0
		self.i = 0
	def update (self, x):
		self.d[self.i] = x
		self.x += x
		if len(self.d) > self.n:
			last = self.i - self.n
			self.x -= self.d[last]
			del self.d[last]
		self.i += 1
		self.y = float(self.x) / len(self.d)
		return self.y


#----------------------------------------------------------------------
# Simple Moving Deviation
#----------------------------------------------------------------------
class SMD (object):
	def __init__ (self, size):
		self.n = size
		self.d = {}
		self.x = 0
		self.m = 0
		self.y = 0
		self.i = 0
	def update (self, x):
		self.d[self.i] = x
		self.x += x
		if len(self.d) > self.n:
			last = self.i - self.n
			self.x -= self.d[last]
			del self.d[last]
		self.i += 1
		self.m = float(self.x) / len(self.d)
		z = sum([ ((v - self.m) ** 2) for v in self.d.itervalues() ])
		self.y = math.sqrt(z / len(self.d))
		return self.y


#----------------------------------------------------------------------
# MACD
#----------------------------------------------------------------------
class MACD (object):
	def __init__ (self, short_mean = 12, long_mean = 26, diff_mean = 9):
		self.short_mean = short_mean
		self.long_mean = long_mean
		self.diff_mean = diff_mean
		self.EMA12 = 0
		self.EMA26 = 0
		self.DIFF = 0
		self.DEA = 0
		self.BAR = 0
		self.init = False
	def update (self, x):
		if not self.init:
			self.EMA12 = x
			self.EMA26 = x
			self.DIFF = 0
			self.DEA = 0
			self.BAR = 0
			self.init = True
		else:
			sm = self.short_mean
			lm = self.long_mean
			dm = self.diff_mean
			self.EMA12 = (self.EMA12 * (sm - 1) + 2 * x) / (sm + 1.0)
			self.EMA26 = (self.EMA26 * (lm - 1) + 2 * x) / (lm + 1.0)
			self.DIFF = self.EMA12 - self.EMA26
			self.DEA = (self.DEA * (dm - 1) + 2 * self.DIFF) / (dm + 1.0)
			self.BAR = 2 * (self.DIFF - self.DEA)
		return self.DIFF


#----------------------------------------------------------------------
# Stochastic Oscillator
#----------------------------------------------------------------------
class KDJ (object):
	def __init__ (self, period = 9, km = 3, dm = 3):
		self.n = period
		self.km = km
		self.dm = dm
		self.K = 50.0
		self.D = 50.0
		self.J = self.K * 3.0 - self.D * 2.0
		self.highs = []
		self.lows = []
		self.RSV = 0.0
	def update (self, high, low, close):
		self.highs.append(high)
		self.lows.append(low)
		if len(self.highs) > self.n:
			del self.highs[0]
		if len(self.lows) > self.n:
			del self.lows[0]
		high = max(self.highs)
		low = min(self.lows)
		if high == low:
			self.RSV = 0.0
		else:
			self.RSV = (close - low) * 100.0 / (high - low)
		self.K = (self.K * (self.km - 1.0) + self.RSV) / self.km
		self.D = (self.D * (self.dm - 1.0) + self.K) / self.dm
		self.J = (3 * self.K) - (2 * self.J)
		return (self.K, self.D, self.J)


#----------------------------------------------------------------------
# Relative strength index
#----------------------------------------------------------------------
class RSI (object):
	def __init__ (self, n = 6):
		self.us = []
		self.ds = []
		self.n = n
		self.last = 0
		self.init = False
		self.rsi = 0.0
	def update (self, x):
		if not self.init:
			U = 0.0
			D = 0.0
			self.init = True
		elif x > self.last:
			U = x - self.last
			D = 0
		elif x == self.last:
			U = D = 0
		else:
			U = 0
			D = self.last - x
		self.last = x
		self.us.append(U)
		self.ds.append(D)
		if len(self.us) > self.n:
			del self.us[0]
		if len(self.ds) > self.n:
			del self.ds[0]
		u = sum(self.us) / float(self.n)
		d = sum(self.ds) / float(self.n)
		x = u + d
		if x == 0:
			self.rsi = 0.0
		else:
			self.rsi = (100.0 * u) / x
		return self.rsi


#----------------------------------------------------------------------
# Bollinger Bands
#----------------------------------------------------------------------
class BOLL (object):
	def __init__ (self, n = 10, k = 2):
		self.ma = SMA(n)
		self.md = SMD(n)
		self.k = k
	def update (self, x):
		self.md.update(x)
		self.BOLL = self.ma.update(x)
		self.MD = self.md.update(x)
		self.UPPER = BOLL + self.k * self.MD
		self.LOWER = BOLL - self.k * self.MD
		return (self.BOLL, self.UPPER, self.LOWER)


#----------------------------------------------------------------------
# Parabolic SAR
#----------------------------------------------------------------------
class SAR (object):
	def __init__ (self, af = 0.02, afmax = 0.2):
		self.af = af
		self.afmax = afmax
		self.init = False
		self.prev_high = 0.0
		self.prev_low = 0.0
		self.prev_sar = 0.0
	def update (self, high, low):
		if not self.init:
			self.prev_sar = (high + low) * 0.5
			self.prev_high = high
			self.prev_low = low
			self.sig0 = True
			self.xpt0 = high
			self.af0 = self.af
			self.init = True
		else:
			sig1 = self.sig0
			xpt1 = self.xpt0
			af1 = self.af0
			lmin = min(self.prev_low, low)
			lmax = max(self.prev_high, high)
			self.prev_low = low
			self.prev_high = high
			if sig1:
				self.sig0 = (low > self.prev_sar)
				self.xpt0 = max(lmax, xpt1)
			else:
				self.sig0 = (high >= self.prev_sar)
				self.xpt0 = min(lmin, xpt1)
			if self.sig0 == sig1:
				sari = self.prev_sar + (xpt1 - self.prev_sar) * af1
				af0 = min(self.afmax, af1 + self.af)
				if self.sig0:
					if self.xpt0 > xpt1:
						self.af0 = af0
					else:
						self.af0 = af1
					sari = min(sari, lmin)
				else:
					if self.xpt0 < xpt1:
						self.af0 = af0
					else:
						self.af0 = af1
					sari = max(sari, lmax)
			else:
				self.af0 = self.af
				sari = self.xpt0
			self.prev_sar = sari
		self.bull = self.sig0 and 1 or 0
		return self.prev_sar, self.bull



#----------------------------------------------------------------------
# Average true range
#----------------------------------------------------------------------
class ATR (object):
	def __init__ (self, n = 14):
		self.n = n
		self.prev = 0
		self.init = False
		self.TR = 0.0
		self.ATR = 0.0
	def update (self, high, low, close):
		tr1 = abs(high - low)
		tr2 = abs(high - self.prev)
		tr3 = abs(low - self.prev)
		self.TR = max(tr1, tr2, tr3)
		self.prev = close
		if not self.init:
			self.TR = tr1
			self.ATR = self.TR
			self.init = True
		else:
			self.ATR = (self.ATR * (self.n - 1.0) + self.TR) / self.n
		return self.ATR
	def current (self, high, low):
		tr1 = abs(high - low)
		tr2 = abs(high - self.prev)
		tr3 = abs(low - self.prev)
		tr = max(tr1, tr2, tr3)
		if not self.init:
			return tr1
		return (self.ATR + (self.n - 1.0) + tr) / self.n


#----------------------------------------------------------------------
# Indicator
#----------------------------------------------------------------------
class Indicator (object):

	def EMA (self, array, n, m = 2):
		ema = EMA(n, m)
		return [ ema.update(x) for x in array ]

	def SMA (self, array, n):
		sma = SMA(n)
		return [ sma.update(x) for x in array ]

	def SMD (self, array, n):
		smd = SMD(n)
		return [ smd.update(x) for x in array ]

	def MACD (self, array, sm = 12, lm = 26, dm = 9):
		macd = MACD(sm, lm, dm)
		r = []
		for x in array:
			macd.update(x)
			r.append((macd.DIFF, macd.DEA, macd.BAR))
		return r

	def KDJ (self, highs, lows, prices, n = 9, km = 3, dm = 3):
		kdj = KDJ(n, km, dm)
		r = []
		for high, low, price in zip(highs, lows, prices):
			x = kdj.update(high, low, price)
			r.append(x)
		return r

	def RSI (self, array, n = 6):
		rsi = RSI(n)
		return [ rsi.update(x) for x in array ]

	def BOLL (self, array, n = 10, k = 2):
		boll = BOLL(n, k)
		return [ boll.update(x) for x in array ]

	def SAR (self, highs, lows, af = 0.02, maxaf = 0.2):
		sar = SAR(af, maxaf)
		r = []
		for high, low in zip(highs, lows):
			x = sar.update(high, low)
			r.append(x)
		return r

	def ATR (self, highs, lows, prices, n = 14):
		atr = ATR(n)
		r = []
		for high, low, price in zip(highs, lows, prices):
			x = atr.update(high, low, price)
			r.append(x)
		return r


#----------------------------------------------------------------------
# Benchmark
#----------------------------------------------------------------------
class Benchmark (object):

	def drawdown (self, array):
		maxima = None
		drawdown = []
		for x in array:
			if maxima is None:
				maxima = x
				y = 0
			else:
				maxima = max(maxima, x)
				if maxima == 0:
					y = 0
				else:
					y = float(maxima - x) / maxima
			drawdown.append(y)
		return drawdown

	def max_drawdown (self, array):
		return max(self.drawdown(array))


#----------------------------------------------------------------------
# instance
#----------------------------------------------------------------------
indicator = Indicator()
benchmark = Benchmark()


#----------------------------------------------------------------------
# testing case
#----------------------------------------------------------------------
if __name__ == '__main__':
	def test1():
		n = [95, 85, 75, 65, 55, 45]
		m = [73, 72, 71, 69, 68, 67]
		d = SMD(100)
		e = SMD(100)
		for x in n:
			print(d.update(x))
		print('')
		for x in m:
			print(e.update(x))
		print()
		import numpy as np
		print(np.std(n))
		print(np.std(m))
		print(indicator.MACD(n))
		return 0

	test1()



