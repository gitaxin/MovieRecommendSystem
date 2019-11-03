package com.hjx.offlineRecommender

import breeze.numerics.sqrt
import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by Axin in 2019/10/30 22:48
  *
  *训练模型最优参数
  *
  * val (rank,iterations,lambda ) = (50,5,0.01)
  *
  * 更好的使用ALS
  *
  *原理：遍历所有业务范围内取值情况，找到最优模型
  * 模型评价:预测值与实际值误差最小
  *
  */
object ALSTrainner {

  def main(args: Array[String]): Unit = {
    var params = Map("spark.cores" -> "local[2]",
      "mongo.uri" -> "mongodb://192.168.110.110:27017/recom",
      "mongo.db" -> "recom"

    )

    //创建Spark环境
    val sparkConfig = new SparkConf()
      .setAppName("ALS Trainner")
      .setMaster(params("spark.cores"))
    //创建SparkSession
    val spark = SparkSession.builder().config(sparkConfig).getOrCreate()
    //加载评分数据
    val mongoConfig = MongoConfig(params("mongo.uri"),params("mongo.db"))


    import spark.implicits._

    val ratingRDD = spark.read.option("uri",mongoConfig.uri)
      .option("collection",OfficeRecommender.RATINGS_COLLECTION_NAME)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]//转换为DataSet
      .rdd  //  转换为rdd
      .map(rating => Rating(rating.uid,rating.mid,rating.score))


    //输出最优参数
    adjustALSParams(ratingRDD)

    spark.close()

  }

  //输出最优参数
  def adjustALSParams(ratingRDD:RDD[Rating]):Unit ={

  //相当于双层循环
    /**
      * 第一次30 和 1
      * 第二次 30 0.1
      * 第三次 30 0.01
      */
    var result = for(rank <- Array(30,40,50,60,70); lambda <- Array(1,0.1,0.01))
      yield{ //相当于将  (rank,lambda,rmse)拼成了一个数组
        val model = ALS.train(ratingRDD,rank,5,lambda)
        // 获取模型误差

        val rmse = getRmse(model,ratingRDD)
        (rank,lambda,rmse)
      }
    //第三位排序，默认升序，取第1个元素
    println(result.sortBy(_._3).head)
  }


  def getRmse(model:MatrixFactorizationModel,ratingRDD:RDD[Rating]) ={
    //需要构造usersProducts RDD
    //预测时只预测有真实值的，而前面的地方是用户有没有看见都会推荐
    val userMovies = ratingRDD.map(item => (item.user,item.product))
    //获取预测值
    val predictRating = model.predict(userMovies)

    //创建以下结构的真实值
    val real = ratingRDD.map(item => ((item.user,item.product),item.rating))
    val predict = predictRating.map(item => ((item.user,item.product),item.rating))

   sqrt(
     real.join(predict) // RDD[((Int, Int), (Double, Double))]
       .map{
         case ((uid,mid),(real,pre)) =>
           //https://blog.csdn.net/reallocing1/article/details/56292877
           //RMSE
           //均方误差:均方根误差是均方误差的算术平方根
           val err = real - pre
           err * err
     }.mean()


   )




  }



}
