package com.hjx.streaming

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
/**
  * Created by Axin in 2019/11/3 14:54
  *
  * 实时推荐引擎
  */



object ConnHelper extends Serializable {


  /**
    *redis配置文件
    * 注释掉以下
    * # bind 127.0.0.1 允许其他主机连接redis
    *
    * 将以下选项改为no，允许免密登录
    * protected-mode no
    *
    */
  //lazy 懒值，需要的时候再创建
  lazy val jedis = new Jedis("192.168.110.110")


  lazy val mongoClient =
    MongoClient(MongoClientURI("mongodb://192.168.110.110:27017/recom"))



}


/**
  * redis存入模拟数据
  *
  * lpush uid:1 1129:2.0 1172:4.0 1263:2.0 1287:2.0 1293:2.0 1339:3.5 1343:2.0 1371:2.5
  *
  * lrange uid:1 0 20
  *
  * kafka
  * 1|20|5.0|1564412038
  * 1|12|5.0|1564412037
  * 1|10|5.0|1564412036
  * 1|4|5.0|1564412035
  * 1|6|5.0|1564412034
  * 1|2|5.0|1564412033
  *
  *需要启动zookeeper
  * ./bin/zkServer.sh start
  *
  * ./bin/kafka-console-producer.sh --broker-list 192.168.110.110:9092 --topic recom
  *
  */
object StreamingRecommender {

  //获取最近的M次电影评分
  val MAX_USER_RATINGS_NUM = 20
  val MAX_SIM_MOVIES_NUM = 20

  //MongoDB中的表Collection
  //Movie在MongoDB中的Collection名称【表】
  val MOVIES_COLLECTION_NAME = "Movie"
  //Rating在MongoDB中的Collection名称【表】
  val RATINGS_COLLECTION_NAME = "Rating"
  //Tag在MongoDB中的Collection名称【表】
  val TAGS_COLLECTION_NAME = "Tag"

  val USER_RECS = "UserRecs"

  val MOVIE_RECS = "MovieRecs"

  val STREAM_RECS_COLLECTION_NAME = "StreamRecs"

  val USER_MAX_RECOMMENDATION = 10




  def main(args: Array[String]): Unit = {


    var params = Map("spark.cores" -> "local[2]",
      "kafka.topic"->"recom",
      "mongo.uri" -> "mongodb://192.168.110.110:27017/recom",
      "mongo.db" -> "recom"

    )


    //创建spark运行环境
    val sparkConf = new SparkConf()
      .setAppName("Streaming Recommender")
      .setMaster(params("spark.cores"))
      .set("spark.executor.memory","4g")//真正执行任务的线程


    val spark = SparkSession.builder().config(sparkConf).getOrCreate()
    val sc = spark.sparkContext
    val ssc = new StreamingContext(sc,Seconds(2))//采样时间为2秒


    import spark.implicits._
    //获取mongodb数据
    implicit val mongoConfig = MongoConfig(params("mongo.uri"),params("mongo.db"))
    //制作共享变量
    val simMoviesMatrix = spark.read.option("uri",mongoConfig.uri)
      .option("collection",MOVIE_RECS)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRecs]
      .rdd
      .map{ resc =>
        //结构Map<mid,Map<mid,score>>  => 1,<2,5><3,4><5,6>
        (resc.mid,resc.recs.map(x => (x.mid,x.r)).toMap)


      }.collectAsMap()// Map[Int, Map[Int, Double]]

    //广播：保存所有电影即和它相似的电影列表
    val simMoviesMatrixBroadCast = sc.broadcast(simMoviesMatrix)

    //触发广播
    val abc = sc.makeRDD(1 to 2)
    abc.map(x => simMoviesMatrixBroadCast.value.get(1)).count()



    val kafkaParam = Map(
      "bootstrap.servers" -> "192.168.110.110:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "recomgroup"


    )
    //连接Kafka
    //https://blog.csdn.net/Dax1n/article/details/61917718
    val kafkaStream = KafkaUtils.createDirectStream(ssc,
                                  LocationStrategies.PreferConsistent,
                                  ConsumerStrategies.Subscribe[String,String](Array(params("kafka.topic")),
                                  kafkaParam))

    //用户->flume->kafka

    //接收评分流 UID|MID|SCORE|TIMESTAMP

     val reatingStream = kafkaStream.map{
      case msg =>
        val attr = msg.value().split("\\|")
        //产生一个元组
        (attr(0).toInt,attr(1).toInt,attr(2).toDouble,attr(3).toInt)
    }


//    reatingStream：属于rdd
    reatingStream.foreachRDD{
      rdd => rdd.map{
        case (uid,mid,score,timestamp) =>
          println(">>>>>>>>>>get data from kafka<<<<<<<<<<<<<<<")

          //获取当前最近的M次评分  redis
          val userRecentlyRatings = getUserRecentlyRatings(MAX_USER_RATINGS_NUM,uid,ConnHelper.jedis)

          //获取电影 P最相似的K个电影 广播变量（在离线推荐中已经将数据存入了mongodb中，此处可以在mongodb中直接获取，但程序是分布式运行的，为了保持每个节点的数据是相同的，我们此处使用广播变量的方式）
          val simMovies = getTopSimMovies(MAX_SIM_MOVIES_NUM,mid,uid,simMoviesMatrixBroadCast.value)

          //计算待选电影的推荐优先级
          val streamRecs = computeMovieScores(simMoviesMatrixBroadCast.value,
                        userRecentlyRatings,simMovies)

          //将数据保存到mongoDB中
          saveRecsToMongoDB(uid,streamRecs)




      }.count() //RDD操作不会触发，加.count是为了触发计算
    }

    ssc.start()
    ssc.awaitTermination()

  }

  /**
    * 将数据保存到MongoDB中
    * @param uid
    * @param streamRecs
    * @return
    */
  def saveRecsToMongoDB(uid: Int, streamRecs: Array[(Int, Double)])(implicit mongoConfig:MongoConfig) : Unit= {


    val streamRecsCollect = ConnHelper.mongoClient(mongoConfig.db)(STREAM_RECS_COLLECTION_NAME)

    //删掉Mongodb中上次存入的数据
    streamRecsCollect.findAndRemove(MongoDBObject("uid" -> uid))

    //mkString：将List转换成String，使用"|"分隔
    streamRecsCollect.insert(MongoDBObject("uid"->uid,"recs"->streamRecs.map(x => x._1+":"+x._2).mkString("|")))
    println(">>>>>>>>>>save to mongodb<<<<<<<<<<<<<<<")



  }



  /**
    * 计算待选电影的推荐分数
    *
    * @param simMovies  电影相似度矩阵
    * @param userRecentlyRatings 用户最近K次评分
    * @param topSimMovies 当前电影最相似的K的电影
    */
  def computeMovieScores(simMovies:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]]
                        ,userRecentlyRatings:Array[(Int,Double)]
                        ,topSimMovies:Array[Int]) : Array[(Int,Double)] = {


    //保存每一个待选电影和最近评分的每一个电影的权重得分
    val score = ArrayBuffer[(Int,Double)]()
    //保存每一个电影的增强因子数
    val increMap = mutable.HashMap[Int,Int]()

    //保存每一个电影的减弱因子数
    val decreMap = mutable.HashMap[Int,Int]()

    //双层循环
    for(topSimMovie <- topSimMovies; userRecentlyRating <- userRecentlyRatings){

      //计算电影的相似度
      val simScore = getMoviesSimScore(simMovies,userRecentlyRating._1,topSimMovie)

      if(simScore > 0.6) {
        score += ((topSimMovie,simScore * userRecentlyRating._2))

        if(userRecentlyRating._2 > 3){
          //增强因子起作用
          increMap(topSimMovie) = increMap.getOrDefault(topSimMovie,0) + 1
        }else{
          //减弱因子起作用
          decreMap(topSimMovie) = decreMap.getOrDefault(topSimMovie,0) + 1

        }

      }

    }

    score.groupBy(_._1).map{
      case (mid,sims) =>
        (mid,sims.map(_._2).sum / sims.length + log(increMap(mid)) - log(increMap(mid)))
    }.toArray
  }



  def log(m:Int) : Double ={
    math.log(m) / math.log(2)
  }


  /**
    * 获取电影之间相似度
    * @param simMovies
    * @param userRatingMovie
    * @param topSimMovie
    * @return
    */
  def getMoviesSimScore(simMovies:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]],
                        userRatingMovie:Int,topSimMovie:Int): Double ={
    simMovies.get(topSimMovie) match {
      case Some(sim)=> sim.get(userRatingMovie) match {
        case Some(score) => score
        case None => 0.0
      }
      case None => 0.0
    }


  }







  /**
    * 获取当前最近的M次电影评分
    * @param num  评分的个数
    * @param uid  用户ID
    * @param jedis
    */
  def getUserRecentlyRatings(num:Int,uid:Int,jedis:Jedis) : Array[(Int,Double)] = {

    //redis中保存的数据格式："uid:1"  => "（100:5.0, 200:4.9, 232:4.7）"
    //从用户队列中取出num个评分
    //jedis.lrange 是Java中的方法，如果需要使用Map方法，则需要
    //导入import scala.collection.JavaConversions._

    jedis.lrange("uid:" + uid.toString,0,num).map{
      item =>
        val attr = item.split("\\:")
        (attr(0).trim.toInt,attr(1).trim.toDouble)//元组类型
    }.toArray

  }



  def getTopSimMovies(num: Int,mid:Int,uid:Int,
                      simMovies:scala.collection.Map[Int,scala.collection.immutable.Map[Int,Double]])(implicit mongoConfig:MongoConfig): Array[Int] ={

    //从广播变量的电影相似度矩阵中获取当前电影所有的相似电影
    val allSimMovies = simMovies.get(mid).get.toArray

    //获取用户已经观看过的电影  mongo中获取 ，类似于redis

    val ratingExist = ConnHelper.mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME)
        .find(MongoDBObject("uid" -> uid))//相当于where uid = uid
        .toArray.map{
          item =>
          item.get("mid").toString.toInt
        //目的就是在ratings表中获取到指定用户下所有的电影
    }

    //过滤掉已经评分过(已经看过)的电影，并排序输出
    allSimMovies.filter(x => ratingExist.contains(x._1))
        .sortWith(_._2 > _._2)
        .take(num)
        .map(x => x._1)

  }
}
