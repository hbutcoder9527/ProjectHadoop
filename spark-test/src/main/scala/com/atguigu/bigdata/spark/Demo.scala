package com.atguigu.bigdata.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo {
	def main(args: Array[String]): Unit = {
		//1.编写配置文件
		val conf = new SparkConf().setMaster("local[*]").setAppName("wordcount")
		//2.获取上下文对象（driver）
		val context = new SparkContext(conf)
		//3.获取源数据
		val lines: RDD[String] = context.textFile("F:/Program Files/CODE/" +
			"ProjectHadoop/spark-test/input/agent.log")
		//4.获取省份和点击次数
		
		val priAndAdvAndOne: RDD[(String, Int)] = lines.map(
			{
				line => {
					val datas: Array[String] = line.split(" ")
					((datas(1) + "-" + datas(4)), 1)
				}
			}
		)
		
		//5.分类聚合
		val priAndAdvAndSum: RDD[(String, Int)] = priAndAdvAndOne.reduceByKey(_ + _)
		
		//6.分离省份和广告
		
		val priAndSum: RDD[(String, (String, Int))] = priAndAdvAndSum.map({
			case (pri, value) => {
				val priNew: Array[String] = pri.split("-")
				(priNew(0), (priNew(1), value))
			}
		}
		)
		//7.相同省份数据放在一起
		val priAndKey: RDD[(String, Iterable[(String, Int)])] = priAndSum.groupByKey()
		
		//8.排序
		val anser: RDD[(String, List[(String, Int)])] = priAndKey.mapValues(
			values => values.toList.sortWith(
				_._2 > _._2
			).take(3)
		)
		anser.collect().foreach(println)
		
		context.stop()
	}
}
