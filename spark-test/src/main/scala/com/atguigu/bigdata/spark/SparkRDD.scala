package com.atguigu.bigdata.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SparkRDD {
	def main(args: Array[String]): Unit = {
		val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
		val sc = new SparkContext(conf)
		
		val listRDD: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))
		
		listRDD.collect().foreach(println)
		listRDD.saveAsTextFile("ooutput")
	}
	
}
