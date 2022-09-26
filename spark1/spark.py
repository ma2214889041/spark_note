#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon May  9 16:56:00 2022

@author: maqiang
"""
from pyspark import  SparkConf,SparkContext

if __name__ == "__main__":
    conf=SparkConf().setMaster('local[*]').setAppName("hello_world")
    
    #用conf 配置 SparkContext
    sc =SparkContext(conf=conf)
    
    file_rdd=sc.textFile('spark_1.txt')
    world_rdd=file_rdd.flatMap(lambda line: line.split(" "))
    word_with_one_rdd=world_rdd.map(lambda x:(x,1))
    result_rdd=word_with_one_rdd.reduceByKey(lambda a,b:a+b)
    
    result=result_rdd.collect()
    print(result)
     