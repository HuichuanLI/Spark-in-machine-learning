import findspark
import os
# 从spark.sql模块中导入SparkSession
from pyspark.sql import SparkSession
import os

# Spark 框架有很多的模块
# spark core -> RDD dataframe
# SparkStreaming

# SparkSQL 类比与Pandas中的Dataframe的API函数:
# 使用Mysql类似使用
# 读取文件数据SparkSession
from pyspark import SparkConf, SparkContext

if __name__ == "__main__":
    """
    基于Pyspark实现词频统计
    """
    print("hello pyspark!")
    # appName 设置名字
    # getOrCreate() 创建
    # 创建sparkConf 对象,spark 设置appName
    spark = SparkSession.builder.appName("SparkSessionExample").master("local[3]").getOrCreate()
    # SarkContext 初始化使用spark_conf

    # 读取数据
    readme_data = spark.read.text("README.md")
    print(type(readme_data))
    readme_data.printSchema()

    readme_data.show(5, truncate=False)
    # 统计包含Spark单词的函数

    nums_spark = readme_data.filter(readme_data.value.contains('Spark')).count()
    nums_pyspark = readme_data.filter(readme_data.value.contains('pysprark')).count()
    print("Lines with Spark:" + str(nums_spark))
    print("Lines with Pyspark:" + str(nums_pyspark))

    """
    读取CSV文件
    """

    csv_df = spark.read.csv("flights.csv", header=True, inferSchema=True)
    print(type(csv_df))
    csv_df.printSchema()
    csv_df.show(10, truncate=False)
    spark.stop()
