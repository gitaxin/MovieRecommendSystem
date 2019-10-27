package com.axin.data



import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient

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

  //ES 中的TYPE名称【表】
  val ES_TAG_TYPE_NAME = "Movie"

  //拆分ES地址的正常表达式
  //.r：表示将字符串指定为一个正则表达式
  //192.168.110.110:9300
  val ES_HOST_PORT_REGEX = "(.+):(\\d+)".r

  def main(args: Array[String]): Unit = {

    val DATAFILE_MOVIES = "E:\\Workspace_IDEA\\MovieRecommendSystem\\Recommend\\DataLoader\\src\\data\\movies.csv"
    val DATAFILE_RATINGS = "E:\\Workspace_IDEA\\MovieRecommendSystem\\Recommend\\DataLoader\\src\\data\\ratings.csv"
    val DATAFILE_TAGS ="E:\\Workspace_IDEA\\MovieRecommendSystem\\Recommend\\DataLoader\\src\\data\\tags.csv"

    //创建全局配置
    var params = Map[String,Any]()
    params += "spark.cores" -> "local[2]"
    params += "mongo.uri" -> "mongodb://192.168.110.110:27017/recom"
    params += "mongo.db" -> "recom"
    //ES对外端口
    params += "es.httpHosts" -> "192.168.110.110:9200"
    //ES集群中内部通信接口
    params += "es.transportHosts" -> "192.168.110.110:9300"
    params += "es.index" -> "recom"
    params += "es.cluster.name" -> "my-application"



    //声明Spark 环境
    var config = new SparkConf().setAppName("DataLoader")
                              .setMaster(params("spark.cores").asInstanceOf[String])

    //定义MongoDB隐式的配置对象
    implicit val mongoConfig = new MongoConfig(params("mongo.uri").asInstanceOf[String],
                          params("mongo.db").asInstanceOf[String])

    //定义ElasticSearch隐式的配置对象
    implicit val esConfig = new ESConfig(params("es.httpHosts").asInstanceOf[String],
                                params("es.transportHosts").asInstanceOf[String],
                                params("es.index").asInstanceOf[String],
                                  params("es.cluster.name").asInstanceOf[String])

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

    val tagDF = tagRDD.map(f = line => {
      val x = line.split(",")
      Tag(x(0).trim().toInt, x(1).trim().toInt
        , x(2).trim(), x(3).trim().toInt)
    }).toDF()

    //将数据保存到Mongodb
    storeDataInMongo(movieDF,ratingDF,tagDF)

    //缓存(对于当前业务，加不加缓存其实影响不大)
    movieDF.cache()
    tagDF.cache()


    //将tagDF对mid做聚合操作，将tag拼接
    //引入内置数据库
    import org.apache.spark.sql.functions._

    //agg拼接
    val tagCollectDF = tagDF.groupBy($"mid").agg(concat_ws("|",collect_set($"tag")).as("tags"))




    //将tags合并到movie表，产生新的movie数据集
    val esMovieDF = movieDF.join(tagCollectDF,Seq("mid","mid"),"left")
                          .select("mid","name","descri","timelong","issue",
                                  "shoot","language","genres","actors","directors","tags")


    //将tags保存到ES
    storeDataInES(esMovieDF)

    //删除缓存
    movieDF.unpersist()
    tagDF.unpersist()

    spark.close()

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
    ratingDF.write.option("uri",mongoConfig.uri)
              .option("collection",RATINGS_COLLECTION_NAME)
              .mode("overwrite")
              .format("com.mongodb.spark.sql")
              .save()

    //将Tag数据集写入到MongoDB
    tagDF.write.option("uri",mongoConfig.uri)
            .option("collection",TAGS_COLLECTION_NAME)
            .mode("overwrite")
            .format("com.mongodb.spark.sql")
            .save()


    //创建索引 1:代表升序 -1：代表降序
    mongoClient(mongoConfig.db)(MOVIES_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(RATINGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(TAGS_COLLECTION_NAME).createIndex(MongoDBObject("uid" -> 1))

    //关闭mongodb连接
    mongoClient.close()
  }


  /**
    * 保存数据到ES
    * @param esMovieDF
    * @param esConfig
    */
  private def storeDataInES(esMovieDF: DataFrame)(implicit esConfig:ESConfig): Unit = {

    //需要操作的Index 名称
    val indexName = esConfig.index
    //连接ES配置
    val settings = Settings.builder().put("cluster.name",esConfig.clusterName).build()

    //连接ES客户端
    val esClient = new PreBuiltTransportClient(settings)

    //params += "es.transportHosts" -> "192.168.110.110:9300,192.168.110.111:9300,192.168.110.112:9300"
    esConfig.transportHosts.split(",").foreach{
        //模式匹配
        case ES_HOST_PORT_REGEX(host:String,port:String) =>
          esClient.addTransportAddress(new InetSocketTransportAddress(
                                      InetAddress.getByName(host),port.toInt
          ))
    }

    //判断如果Index是否存在，如果则存则删除
    if(esClient.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists){
      //删除index
      esClient.admin().indices().delete(new DeleteIndexRequest(indexName)).actionGet()
    }

    //创建index
    esClient.admin().indices().create(new CreateIndexRequest(indexName)).actionGet()

    val movieOptions = Map("es.nodes" -> esConfig.httpHosts
              ,"es.http.timeout" -> "100m"
                ,"es.mapping.id" -> "mid")

    val movieTypeName = s"$indexName/$ES_TAG_TYPE_NAME"
    //保存数据
    esMovieDF.write.options(movieOptions)
                  .mode("overwrite")
                  .format("org.elasticsearch.spark.sql")
                  .save(movieTypeName)


    /**
      * LINUX 命令可以查询ES中的数据
      * curl -XPOST '192.168.110.110:9200/recom/_search?pretty' -d '{"query":{"match_all":{}}}'
      * URL查询ES中的数据
      * http://192.168.110.110:9200/recom/Movie/{id}
      * 例如:http://192.168.110.110:9200/recom/Movie/1
      *
      */




  }
}
