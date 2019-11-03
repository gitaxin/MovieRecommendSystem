package com.hjx.offlineRecommender

/**
  * Created by Axin in 2019/10/27 12:21  
  */

/**
  * movies.csv :电影基本信息
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
  */
case class Movie(val mid:Int,val name:String,val descri:String,val timelong:String
                 ,val issue:String,val shoot:String,val language:String,val genres:String
                ,val actors:String,val directors:String)


/**
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
  */

case class MovieRating(val uid:Int,val mid:Int,val score:Double,val timestamp:Int)


/**
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
  */

case class Tag(val uid:Int,val mid:Int,val tag:String,val timestamp:Int)


/**
  *MongoDB配置对象
  */

case class MongoConfig(val uri:String,val db:String)

/**
  * ElasticSearch 配置对象
  */

case class ESConfig(val httpHosts:String,val transportHosts:String,val index:String,val clusterName:String)

case class Recommendation(mid:Int,r:Double)

//Seq:序列，相当于一个List
case class GenresRecommendation(genres:String,recs:Seq[Recommendation])

//用户推荐
case class UserRecs(uid:Int,recs:Seq[Recommendation])

//电影相似度
case class MovieRecs(mid:Int,recs:Seq[Recommendation])