#!/usr/bin/env python
# -*- coding: utf-8 -*-


"""
@version: v1.0
@author: huichuan
@time: 2019/11/17 11:47
"""

import os
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    """
    基于PySpark实现大数据经典程序案例：WordCount词频统计
    """


    # 构建SparkContext类的实例对象，用于读取数据
    spark_conf = SparkConf()\
        .setAppName("Python_Spark_WordCount")\
        .setMaster("local[2]")
    sc = SparkContext(conf=spark_conf)

    # 读取文件数据
    input_rdd = sc.textFile("wc.data")

    print(input_rdd.count())
    print(input_rdd.first())

    # WEB UI
    import time
    time.sleep(100000)

    # 关闭资源
    sc.stop()
