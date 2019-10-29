package com.axin.statistics

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Created by Axin in 2019/10/28 22:32
  *
  *
  * 统计的main方法
  *
  * 数据流程
  * spark读取mongodb中的数据，离线统计后，将统计结果写入到 mongodb
  *
  * 1、目标
  * （1）优质电影
  *       获取所有历史数据中，评分个数最多的电影集合，统计每个电影评分个数 -->RateMoreMovies
  *  (2)近期热门电影
  *       按照月统计，这个月中评分最多的电影我们认为是热门电影，统计每个月中的每个电影的评分数量 -->RateMoreRecentlyMovie
  *  (3)电影的平均分
  *       把每个电影，所有用户评分进行平均，计算出每个电影的平均分 -> AverageMovies
  *  (4)每种类别电影的Top10
  *       将每种类别的电影中，评分最高的10个电影计算出来  ->  GenresTopMovies
  */
object StatisticsApp extends App {


  //MongoDB中的表Collection
  //Movie在MongoDB中的Collection名称【表】
  val MOVIES_COLLECTION_NAME = "Movie"
  //Rating在MongoDB中的Collection名称【表】
  val RATINGS_COLLECTION_NAME = "Rating"
  //Tag在MongoDB中的Collection名称【表】
  val TAGS_COLLECTION_NAME = "Tag"

  val params = scala.collection.mutable.Map[String,Any]()
  params += "spark.cores" -> "local[2]"
  params += "mongo.uri" -> "mongodb://192.168.110.110:27017/recom"
  params += "mongo.db" -> "recom"

  var conf = new SparkConf().setAppName("Statistics")
                              .setMaster(params("spark.cores").asInstanceOf[String])

  val spark = SparkSession.builder().config(conf).getOrCreate()

  //操作mongodb

  implicit val mongoConfig = new MongoConfig(params("mongo.uri").asInstanceOf[String],
                   params("mongo.db").asInstanceOf[String])


    /**
      * 读取mongodb数据
      *
      *  1.生产环境中一般很少将原始表进行cache
      *  2.生产环境中可能会从Hive中读取数据
      */


    import spark.implicits._
  //读取标签表
  var ratings = spark.read
            .option("uri",mongoConfig.uri)
            .option("collection",RATINGS_COLLECTION_NAME)
            .format("com.mongodb.spark.sql")
            .load()
            .as[Rating]
            .cache



  // 读取电影表
  var movies = spark.read
    .option("uri",mongoConfig.uri)
    .option("collection",MOVIES_COLLECTION_NAME)
    .format("com.mongodb.spark.sql")
    .load()
    .as[Movie]
    .cache

  //视图：注册成为一张表，会存入sparkSession中
  ratings.createOrReplaceTempView("ratings")


  //统计评分最多电影
  //StatisticsSystem.rateMore(spark)

  //近期热门电影
  //StatisticsSystem.rateMoreRecently(spark)

  //TOP10
  StatisticsSystem.genresTop10(spark)(movies)

  ratings.unpersist()
  movies.unpersist()

  spark.close()



}
