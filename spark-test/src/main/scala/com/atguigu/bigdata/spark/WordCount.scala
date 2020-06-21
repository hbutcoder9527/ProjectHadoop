package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object WordCount {
	def main(args: Array[String]): Unit = {
		val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
		val sc = new SparkContext(conf)
		
		val lines: RDD[String] = sc.textFile("F:\\Program Files\\CODE\\ProjectHadoop\\spark-test\\input\\word.txt")
		
		val words: RDD[String] = lines.flatMap(_.split(" "))
		
		val wordToOne: RDD[(String, Int)] = words.map((_, 1))
		
		val wordtoSum: RDD[(String,Int)] = wordToOne.reduceByKey(_+_)
		val result: Array[(String,Int)] = wordtoSum.collect()
		
		result.foreach(println)
	}
}
