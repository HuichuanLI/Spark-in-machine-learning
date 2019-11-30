import findspark
import os
# 从spark.sql模块中导入SparkSession
from pyspark.sql import SparkSession

if __name__ == "__main__":
    """
    基于Pyspark实现词频统计
    """
    print("hello pyspark!")
    # appName 设置名字
    # getOrCreate() 创建

    spark = SparkSession.builder \
        .config("spark.sql.warehouse.dir", "../spark-warehouse") \
        .appName('My_App') \
        .master('local[*]') \
        .getOrCreate()
    df = spark.read.csv('flights.csv', header=True)
    print(type(df))
    df.printSchema()
    # DataFrame对象的show方法用于查看数据框的内容
    # 查看前5条记录
    # truncate 超过20个直接取消显示
    df.show(5, truncate=False)


    # 统计
