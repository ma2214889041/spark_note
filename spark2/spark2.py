#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May 10 00:17:55 2022

@author: maqiang
"""

from pyspark import SparkConf,SparkContext

conf = SparkConf().setAppName('spark2').setMaster('local[*]')

sc=SparkContext(conf=conf)

import pandas as pd