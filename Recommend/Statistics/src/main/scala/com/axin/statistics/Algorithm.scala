package com.axin.statistics


import org.apache.spark.sql.SparkSession


/**
  * Created by Axin in 2019/10/28 23:13
  * object 中的名称可以不与文件名称相同
  */

object StatisticsSystem {

  val RATE_MORE_MOVIES="RateMoreMovies"

  //统计评分最多电影

  def rateMore(spark:SparkSession)(implicit mongoConfig: MongoConfig) : Unit = {

    //操作sparkSession中的视图ratings
    var rateMoreDF = spark.sql("select mid,count(1) as count from ratings group by mid order by count desc")

    //将结果存入mongodb
    rateMoreDF.write.option("uri",mongoConfig.uri)
              .option("collection",RATE_MORE_MOVIES)
              .mode("overwrite")
              .format("com.mongodb.spark.sql")
              .save()




  }

}
