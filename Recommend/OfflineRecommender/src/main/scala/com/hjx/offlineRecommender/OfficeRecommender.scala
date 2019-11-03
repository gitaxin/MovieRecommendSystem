package com.hjx.offlineRecommender

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.sql.SparkSession
import org.jblas.DoubleMatrix



/**
  * Created by Axin in 2019/10/30 22:45
  *
  * 离线推荐算法实现
  *
  *用ALS
  */
object OfficeRecommender {


  //MongoDB中的表Collection
  //Movie在MongoDB中的Collection名称【表】
  val MOVIES_COLLECTION_NAME = "Movie"
  //Rating在MongoDB中的Collection名称【表】
  val RATINGS_COLLECTION_NAME = "Rating"
  //Tag在MongoDB中的Collection名称【表】
  val TAGS_COLLECTION_NAME = "Tag"

  val USER_RECS = "UserRecs"

  val MOVIE_RECS = "MovieRecs"

  val USER_MAX_RECOMMENDATION = 10


  def main(args: Array[String]): Unit = {

    var params = Map("spark.cores" -> "local[1]",
      "mongo.uri" -> "mongodb://192.168.110.110:27017/recom",
      "mongo.db" -> "recom"

        )

    //创建Spark环境
    val sparkConfig = new SparkConf()
          .setAppName("Office Recommender")
          .setMaster(params("spark.cores"))
         .set("spark.executor.memory","6G")
          .set("spark.driver.memory","2G")



          //.set("spark.testing.memory", "2147480000")//后面的值大于512m即可

    /**
      * .set("spark.executor.memory","6G")
      * .set("spark.driver.memory","2G")
      * 可以如上在代码中设置，也可以在submit时设置，建议不要在代码中直接设置
      */


    //创建SparkSession
     val spark = SparkSession.builder().config(sparkConfig).getOrCreate()


    //获取mongodb数据
    val mongoConfig = MongoConfig(params("mongo.uri"),params("mongo.db"))

    //此处的 spark 代表的是上面的变量spark
    import spark.implicits._


    val ratingRDD =  spark.read
        .option("uri",mongoConfig.uri)
        .option("collection",RATINGS_COLLECTION_NAME)
        .format("com.mongodb.spark.sql")
        .load()
        .as[MovieRating]//DataSet
        .rdd  //转换为rdd 类型为Rating
        .map( rating => (rating.uid,rating.mid,rating.score)).cache



    val movieRDD = spark.read
      .option("uri",mongoConfig.uri)
      .option("collection",MOVIES_COLLECTION_NAME)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]//DataSet
      .rdd
      .map(_.mid).cache()

    //训练 ALS模型
    //   ALS.train()
    /**
      * 传入4个参数
      * trainData
      * 训练数据
      * Rating对象的集合:包含：用户ID、物品ID、偏好值
      * 格式：（用户id，物品id，偏好值）
      *
      * rank
      * 特征维度：50
      *
      * iterations
      * 迭代次数：
      *
      * lambda:
      * 损失函数
      * 0.01
      */

    val trainData = ratingRDD.map(x =>{Rating(x._1,x._2,x._3)})

    val (rank,iterations,lambda ) = (10,1,0.01)
    val model = ALS.train(trainData,rank,iterations,lambda)

    //计算用户推荐矩阵
    val userRDD = ratingRDD.map(_._1).distinct().cache()
    //使用userID 和 movieID 做一个X乘（笛尔卡积）
    val userMovies = userRDD.cartesian(movieRDD)

    //将笛尔卡积作为参数传入，
    // 得到的结果是全排列组合，需要自己根据业务过滤
    //也就是给每个用户推荐的电影

    val preRatings = model.predict(userMovies)


    //转换为UserRecs类型， 写入mongodb
    val userRecs = preRatings.filter(_.rating > 0)
    .map(rating => {(rating.user,(rating.product,rating.rating))})
      .groupByKey()
      .map{
        case (uid,recs) =>{
          UserRecs(uid,recs.toList.sortWith(_._2 > _._2).take(USER_MAX_RECOMMENDATION)
          .map(x => Recommendation(x._1,x._2)))
        }
      }.toDF


    userRecs.write.option("uri",mongoConfig.uri)
      .option("collection",USER_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()



    //计算电影相似度矩阵
    //1.获取电影的特征矩阵
    val movieFeatures = model.productFeatures.map{
      //使用到jblas这个包
      case (mid,features) => (mid,new DoubleMatrix(features))
    }

    //笛卡尔积自已,类似于自连接（每个电影与其他电影都要相互交一下）
    val movieRecs = movieFeatures.cartesian(movieFeatures)// RDD[((Int, DoubleMatrix), (Int, DoubleMatrix))]
        .filter{case(a,b) => a._1 != b._1 }//自己不与自己比较
        .map{
          case (a,b) =>
            val simScore = this.consinSim(a._2,b._2)//电影相似性评分
            (a._1,(b._1,simScore))

        }.filter(_._2._2 > 0.6)
        .groupByKey()// RDD[(Int, Iterable[(Int, Double)])]
        .map{
          case (mid,items) =>
            MovieRecs(mid,items.toList.map(x=>Recommendation(x._1,x._2)))
        }.toDF

    //保存到mongodb
    movieRecs.write.option("uri",mongoConfig.uri)
      .option("collection",MOVIE_RECS)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()


    userRDD.unpersist()
    movieRDD.unpersist()
    ratingRDD.unpersist()


    spark.close()
  }

  //计算两个电影之间的余弦相似度
  def consinSim(movie1 : DoubleMatrix, movie2 : DoubleMatrix) : Double = {
    movie1.dot(movie2) / (movie1.norm2() * movie2.norm2())
  }

}
