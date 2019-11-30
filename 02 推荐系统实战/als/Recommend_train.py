from pyspark.sql import SparkSession
# 设置环境变量
import os
# 导入模块
from pyspark.mllib.recommendation import Rating, ALS, MatrixFactorizationModel


def create_context():
    """
    创建SoarkContext
    :return: SparkContext对象
    """

    # 构建SparkSession实例对象
    spark = SparkSession.builder \
        .appName("SparkSessionExample") \
        .master("local[*]") \
        .getOrCreate()

    # 获取SparkContext实例对象
    sc = spark.sparkContext
    return sc


def prepare_data(spark_context):
    """
    读取电影评分数据，转换数据格式为rdd[Rating]
    :param spark_context: SparkContext上下文对象，用于读取数据
    :return: 返回rdd
    """
    # 读取数据
    raw_ratings_rdd = spark_context.textFile("../ml-100k/u.data")
    ratings_rdd = raw_ratings_rdd.map(lambda line: line.split('\t')[:3])
    # 将数据转换为ALS算法要求数据格式
    ratings_datas = ratings_rdd.map(lambda x: Rating(int(x[0]), int(x[1]), float(x[2])))
    return ratings_datas


def save_model(spark_context, model):
    try:
        model.save(spark_context, '../datas/asl-model')
    except Exception:
        print("Model 已经存在！！！！！！！")


if __name__ == "__main__":
    """
    1. 数据准备阶段：
        读取u.data数据，经过处理后产生评分数据ratings_rdd
    2. 训练阶段：
        评分数据ratings_rdd经过ALS.train()训练产生模型Model
    3. 存储模型
        存储模型Model在本地或者HDFS
    """

    sc = create_context()

    ratings_rdd = prepare_data(sc)

    als_model = ALS.train(ratings_rdd, 10, 10, 0.1)

    save_model(sc, als_model)
