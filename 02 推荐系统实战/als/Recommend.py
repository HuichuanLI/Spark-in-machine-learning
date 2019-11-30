#!/usr/bin/env python
# -*- coding: utf-8 -*-


# 从spark.sql中导入SparkSession类
from pyspark.sql import SparkSession
# 设置环境变量
import os
import sys
# 导入模块
from pyspark.mllib.recommendation import Rating, ALS, MatrixFactorizationModel


def create_context():
    """
    创建SoarkContext失落的心
    :return: SparkContext对象
    """

    # 构建SparkSession实例对象
    spark = SparkSession.builder \
        .appName("RmdMovieUserExample") \
        .master("local[*]") \
        .getOrCreate()

    # 获取SparkContext实例对象
    return spark.sparkContext


def prepare_data(spark_context):
    # 读取 u.item 电影信息数据
    item_rdd = sc.textFile("../ml-100k/u.item")

    # 创建   电影名称 与 电影ID  映射的字典
    movie_title = item_rdd \
        .map(lambda line: line.split("|")) \
        .map(lambda a: (float(a[0]), a[1]))

    # 将RDD转换字典
    movie_title_dict = movie_title.collectAsMap()

    return movie_title_dict


def load_model(spark_context):
    try:
        model = MatrixFactorizationModel.load(spark_context, '../datas/asl-model')
        print(model)
        return model
    except Exception:
        print("加载模型出错")


def recommend_movies(als, movies, user_id):
    rmd_movies = als.recommendProducts(user_id, 10)
    print("针对用户ID: " + str(user_id) + ", 推荐以下电影：")
    for rmd in rmd_movies:
        print("\t 针对用户ID: {0}, 推荐电影: {1}, 推荐评分: {2}".format(rmd[0], movies[rmd[1]], rmd[2]))


def recommend_users(als, movies, movie_id):
    # 为了每个电影推荐十个用户
    rmd_users = als.recommendUsers(movie_id, 10)
    print("针对电影ID: {0}, 电影名：{1}, 推荐下列十个用户: ".format(movie_id, movies[movie_id]))
    for rmd in rmd_users:
        print('\t 推荐用户ID: {0}, 推荐评分: {1}'.format(rmd[0], rmd[2]))


def recommend(als_model, movie_dic):
    # 推荐电影给用户
    if sys.argv[1] == '--U':
        recommend_movies(als_model, movie_dic, int(sys.argv[2]))
    # 推荐用户给电影
    if sys.argv[1] == '--M':
        recommend_users(als_model, movie_dic, int(sys.argv[2]))


if __name__ == "__main__":
    """
    -a. 加载模型
    -b. 为用户对推荐电影
    -c. 为电影对推荐用户
    """

    # 设置运行python脚本的时候，传递两个参数，决定如何推荐
    if len(sys.argv) != 3:
        print("请输入2个参数： 要么是: --U user_id，要么是: --M moive_id")
        exit(-1)

    sc = create_context()

    # 数据准备，就是加载电影数据信息，转换字典
    print('============= 数据准备 =============')
    movie_title_dic = prepare_data(sc)

    print('============= 加载模型 =============')
    als_load_model = load_model(sc)
    type(als_load_model)

    print('============= 预测推荐 =============')
    recommend(als_load_model, movie_title_dic)
