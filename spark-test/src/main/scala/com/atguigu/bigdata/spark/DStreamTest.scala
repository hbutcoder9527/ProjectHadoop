package com.atguigu.bigdata.spark

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DStreamTest {
	def main(args: Array[String]): Unit = {
		//1.配置文件
		val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DStreamTest")
		
		//2.创建离散流对象
		val ssc = new StreamingContext(conf, Seconds(5))
		//3.创建DStream
		
		val lines: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
		val oriData: DStream[String] = lines.window(Seconds(10),Seconds(5))
		//4.拆分输入的数据
		
		val oriDS: DStream[String] = oriData.flatMap(_.split(" "))
		
		//5.改变结构，便于做聚合
		
		val tunDS: DStream[(String, Int)] = oriDS.map((_, 1))
		
		//6.计数
		
		val count: DStream[(String, Int)] = tunDS.reduceByKey(_ + _)
		
		//7.显示结果
		
		count.print
		
		//8.启动流
		
		ssc.start()
		
		//9.等待计数结束，停止程序
		
		ssc.awaitTermination
	}
}
