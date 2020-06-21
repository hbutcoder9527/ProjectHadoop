package com.atguigu.bigdata.spark

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object updateStateTest {
	def main(args: Array[String]): Unit = {
		//1.配置文件
		val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("updateStateTest")
		
		//2.创建离散流对象
		val ssc = new StreamingContext(conf, Seconds(5))
		
		//3.获取数据源
		
		val oriData: ReceiverInputDStream[String] = ssc.socketTextStream("hadoop102", 9999)
		
		//4.处理数据
		val unitData: DStream[String] = oriData.flatMap(_.split(" "))
		
		//5.改变数据格式，便于处理
		
		val turbData: DStream[(String, Int)] = unitData.map((_, 1))
		
		//6.开始统计数据：将没五秒一次的数据装到一个序列中去，处理后存入buffer缓存，以便下次使用。
		//但buffer是在内存当中，为了不丢失数据，需要设置checkpoint
		
		val result: DStream[(String, Int)] = turbData.updateStateByKey(
			(seq: Seq[Int], buffer: Option[Int]) => {
				val total: Int = buffer.getOrElse(0) + seq.sum
				Option[Int](total)
			}
		)
		//7.设置检查点
		ssc.checkpoint("cp")
		result.print
		//8.启动数据采集器
		ssc.start()
		//9.等待计数结束，停止程序
		ssc.awaitTermination()
	}
}
