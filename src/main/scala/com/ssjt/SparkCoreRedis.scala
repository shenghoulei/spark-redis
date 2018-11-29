package com.ssjt

import org.apache.spark.{SparkConf, SparkContext}
import com.redislabs.provider.redis._

object SparkCoreRedis {
	def main(args: Array[String]): Unit = {
		val sc = getSparkContext
		writeRedis(sc)
	}

	/**
	  * 获取SparkContext
	  *
	  * @return
	  */
	def getSparkContext: SparkContext = {
		//这里直接使用yarn-cluster模式
		val conf = new SparkConf().setMaster("local[5]").setAppName("sparkRedisTest")
		conf.set("redis.host", "10.10.100.16") //host,随便一个节点，自动发现
		conf.set("redis.port", "6379") //端口号，不填默认为6379
		//conf.set("redis.auth","null")  //用户权限配置
		conf.set("redis.db","0")  //数据库设置
		conf.set("redis.timeout","2000")  //设置连接超时时间
		new SparkContext(conf)
	}

	/**
	  * 读取Redis的数据并处理
	  *
	  * @param sc SparkContext
	  */
	def readRedis(sc: SparkContext): Unit = {
		val redis = sc.fromRedisKV("name*")
		redis.foreach(println)
	}

	/**
	  * 写出数据到Redis
	  *
	  * @param sc
	  */
	def writeRedis(sc: SparkContext): Unit = {
		val list = List(("high","111"), ("abc","222"), ("together","333"))
		val rdd = sc.makeRDD(list, 2)
		sc.toRedisKV(rdd, 2000)
	}

}
