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

