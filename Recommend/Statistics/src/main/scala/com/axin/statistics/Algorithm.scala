package com.axin.statistics


import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.{Dataset, SparkSession}


/**
  * Created by Axin in 2019/10/28 23:13
  * object 中的名称可以不与文件名称相同
  */

object StatisticsSystem {

  val RATE_MORE_MOVIES="RateMoreMovies"
  val RATE_MORE_MOVIES_RECENTLY="RateMoreMoviesRecently"

  var AVERAGE_MOVIES_SCORE ="AverageMoviesScore"
  var GENRES_TOP_MOVIES ="GenresTopMovies"




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

  /**
    * 统计近期热门电影
    * @param spark
    * @param mongoConfig
    */
  def rateMoreRecently(spark: SparkSession)(implicit mongoConfig: MongoConfig):Unit ={

    /**
      * udf作用
      * 1260759135 => "201910"
      */
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    spark.udf.register("changeDate",(x:Long)=>simpleDateFormat.format(new Date(x*1000L)).toLong)


    var yearMonthOfRatings = spark.sql("select mid,uid,score, changeDate(timestamp) as yearmonth from ratings")

    yearMonthOfRatings.createOrReplaceTempView("ymRatings")

    spark.sql("select mid,count(1) as count, yearmonth from ymRatings group by yearmonth,mid order by yearmonth desc")
      .write.option("uri",mongoConfig.uri)
      .option("collection",RATE_MORE_MOVIES_RECENTLY)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

  }


  /**
    * 按类别统计 平均分最高的Top10
    * 柯里化传参方式
    * @param spark
    * @param movies
    * @param mongoConfig
    */
  def genresTop10(spark:SparkSession)(movies:Dataset[Movie])(implicit mongoConfig: MongoConfig):Unit ={


    //定义所有电影类别
    val genres = List("Action","Adventure","Animation","Comedy","Ccrime","Documentary","Drama","Family","Fantasy","Foreign","History","Horror","Music","Mystery"
      ,"Romance","Science","Tv","Thriller","War","Western")

    var averageMovieScoreDF = spark.sql("select mid,avg(score) as avg from ratings group by mid").cache

    //统计类别中 评分最高的10部电影

    val moviesWithScoreDF = movies.join(averageMovieScoreDF,Seq("mid","mid")).select("mid","avg","genres").cache()

    var genresRDD = spark.sparkContext.makeRDD(genres)

    import spark.implicits._
    var genresTopMovies = genresRDD.cartesian(moviesWithScoreDF.rdd).filter{
      case (genres,row) => {
        row.getAs[String]("genres").toLowerCase.contains(genres.toLowerCase)
      }
    }.map{
      case (genres,row) =>{
        (genres,(row.getAs[Int]("mid"),row.getAs[Double]("avg")))
      }
    }.groupByKey()
      .map{
        case (genres,items) =>{
          //take(10)：取前10
          val recommendations = items.toList.sortWith(_._2 > _._2).take(10).map(x => Recommendation(x._1,x._2))
          GenresRecommendation(genres,recommendations)
        }

    }.toDF



    genresTopMovies
      .write.option("uri",mongoConfig.uri)
      .option("collection",GENRES_TOP_MOVIES)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    moviesWithScoreDF
      .write.option("uri",mongoConfig.uri)
      .option("collection",AVERAGE_MOVIES_SCORE)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


  }

}
