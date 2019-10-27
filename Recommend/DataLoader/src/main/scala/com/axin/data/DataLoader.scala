package com.axin.data

import com.axin.data.DataLoader.MOVIES_COLLECTION_NAME
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Created by Axin in 2019/10/26 23:14
  *
  *
  * 将数据导入到系统，MongoDB,ES
  *
  * 使用Spark SQL（支持多数据源，并且也是最常用的一种方式）, 导入数据
  * ####################################################################
  *  movies.csv :电影基本信息
  *  分隔符： ^
  * 8275^
  * College (1927)^
  * To reconcile with his girlfriend	 a bookish college student tries to become an athlete.^
  * 66 minutes^
  * January 11	 2000^
  * 1927^
  * English ^
  * Comedy ^
  * Buster Keaton|Anne Cornwall|Flora Bramley|Harold Goodwin|Snitz Edwards|Carl Harbaugh|Sam Crawford|Florence Turner|Buster Keaton|Anne Cornwall|Flora Bramley|Harold Goodwin|Snitz Edwards ^
  * Buster Keaton|James W. Horne
  * 电影 ID
  * 电影的名称
  * 电影描述
  * 电影时长
  * 电影的发行日期
  * 电影拍摄日期
  * 语言
  * 类型
  * 演员
  * 导演
  *####################################################################
  *ratings.csv ：用户对电影的评分数据集
  * 分隔符：,
  * 1,
  * 1029,
  * 3.0,
  * 1260759179
  * 用户id
  * 电影id
  * 用户对电影的评分
  * 用户对电影评分的时间
  *####################################################################
  *tags.csv ：用户对电影的标签数据集
  * 分隔符：,
  * 15,
  * 339,
  * sandra 'boring' bullock,
  * 1138537770
  * 用户id
  * 电影id
  * 标签内容
  * 时间
  **/
object DataLoader {

  //MongoDB中的表Collection
  //Movie在MongoDB中的Collection名称【表】
  val MOVIES_COLLECTION_NAME = "Movie"
  //Rating在MongoDB中的Collection名称【表】
  val RATINGS_COLLECTION_NAME = "Rating"
  //Tag在MongoDB中的Collection名称【表】
  val TAGS_COLLECTION_NAME = "Tag"
  def main(args: Array[String]): Unit = {

    val DATAFILE_MOVIES = "E:\\Workspace_IDEA\\MovieRecommendSystem\\Recommend\\DataLoader\\src\\data\\movies.csv"
    val DATAFILE_RATINGS = "E:\\Workspace_IDEA\\MovieRecommendSystem\\Recommend\\DataLoader\\src\\data\\ratings.csv"
    val DATAFILE_TAGS ="E:\\Workspace_IDEA\\MovieRecommendSystem\\Recommend\\DataLoader\\src\\data\\tags.csv"

    //创建全局配置
    var params = Map[String,Any]()
    params += "spark.cores" -> "local[2]"
    params += "mongo.uri" -> "mongodb://192.168.110.110:27017/recom"
    params += "mongo.db" -> "recom"

    //声明Spark 环境
    var config = new SparkConf().setAppName("DataLoader")
                              .setMaster(params("spark.cores").asInstanceOf[String])

    //隐式对象
    implicit val mongoConfig = new MongoConfig(params("mongo.uri").asInstanceOf[String],
                          params("mongo.db").asInstanceOf[String])

    val spark = SparkSession.builder().config(config).getOrCreate()

    //加载数据集 Movie Rating Tag
    var movieRDD = spark.sparkContext.textFile(DATAFILE_MOVIES)
    var ratingRDD = spark.sparkContext.textFile(DATAFILE_RATINGS)
    var tagRDD = spark.sparkContext.textFile(DATAFILE_TAGS)

    //将RDD 转成DataFrame

    //.toDF()时需要一个隐式转换
    import spark.implicits._

    val movieDF = movieRDD.map(line =>{
      val x = line.split("\\^")
      Movie(x(0).trim().toInt, x(1).trim(), x(2).trim(),x(3).trim(),x(4).trim(),
          x(5).trim(),x(6).trim(),x(7).trim(),x(8).trim(),x(9).trim())
    }).toDF()

    val ratingDF = ratingRDD.map(f = line => {
      val x = line.split(",")
      Rating(x(0).trim().toInt, x(1).trim().toInt
        , x(2).trim().toDouble, x(3).trim().toInt)
    }).toDF()

    val tagDF = ratingRDD.map(f = line => {
      val x = line.split(",")
      Tag(x(0).trim().toInt, x(1).trim().toInt
        , x(2).trim(), x(3).trim().toInt)
    }).toDF()

    //将数据保存到Mongodb
    storeDataInMongo(movieDF,ratingDF,tagDF)


  }


  /**
    * 将数据保存到Mongodb
    * @param movieDF
    * @param ratingDF
    * @param tagDF
    */
  private def storeDataInMongo(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {


    //创建到MongoDB的连接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    //先删除表中的数据
    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).dropCollection()

    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).dropCollection()

    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).dropCollection()

    //注意：如果MongoDB中不存在对应库，MongoDB会自动创建

    //将Movie数据集写入到MongoDB
    movieDF.write.option("uri",mongoConfig.uri)
                .option("collection",MOVIES_COLLECTION_NAME)
                .mode("overwrite")
                .format("com.mongodb.spark.sql")
                .save()

    //将Rating数据集写入到MongoDB
    movieDF.write.option("uri",mongoConfig.uri)
              .option("collection",RATINGS_COLLECTION_NAME)
              .mode("overwrite")
              .format("com.mongodb.spark.sql")
              .save()

    //将Tag数据集写入到MongoDB
    movieDF.write.option("uri",mongoConfig.uri)
            .option("collection",TAGS_COLLECTION_NAME)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()


    //创建索引
    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))

    //关闭mongodb连接
    mongoClient.close()
  }


}
