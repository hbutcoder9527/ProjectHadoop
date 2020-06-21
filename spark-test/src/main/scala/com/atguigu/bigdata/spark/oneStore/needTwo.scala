package com.atguigu.bigdata.spark.oneStore

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

object needTwo {
	def main(args: Array[String]): Unit = {
		 //1.配置文件
		val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("fclife")
		//2.创造driver对象
		val sc = new SparkContext(conf)
		//3.读取数据
		val oriRDD: RDD[String] = sc.textFile("F:\\Program Files\\CODE\\ProjectHadoop\\" +
			"spark-test\\input\\user_visit_action.txt")
		//4.封装数据进入对象
		val infos: RDD[UserVisitAction] = oriRDD.map(
			data => {
				val lines: mutable.ArrayOps[String] = data.split("_")
				UserVisitAction(
					lines(0),
					lines(1).toLong,
					lines(2),
					lines(3).toLong,
					lines(4),
					lines(5),
					lines(6).toLong,
					lines(7).toLong,
					lines(8),
					lines(9),
					lines(10),
					lines(11),
					lines(12).toLong
				)
			}
		)
		
		val accumulatir = new myAccumulatir
		
		sc.register(accumulatir)
		
		infos.foreach(
			data => accumulatir.add(data)
		)
		
		val datas: mutable.HashMap[(String, String), Long] = accumulatir.value
		val groupData: Map[String, mutable.HashMap[(String, String), Long]] = datas.groupBy(data => data._1._1)
		
		val gateData: immutable.Iterable[CategoryCountInfo] = groupData.map {
			case (cid, map) => {
				CategoryCountInfo(cid, map.getOrElse((cid, "click"), 0L), map.getOrElse((cid, "order"), 0L), map.getOrElse((cid, "pay"), 0L))
			}
		}
		
		//聚合
		val result: List[CategoryCountInfo] = gateData.toList.sortWith(
			(left, right) => {
				if (left.clickCount > right.clickCount) {
					true
				} else if (left.clickCount == right.clickCount) {
					if (left.orderCount > right.orderCount) {
						true
					} else if (left.orderCount == right.orderCount) {
						left.payCount > right.payCount
					} else {
						false
					}
				} else {
					false
				}
			}
		).take(10)
		
		result.foreach(println)
	}
}

class myAccumulatir extends AccumulatorV2[UserVisitAction,mutable.HashMap[(String,String),Long]]{
	private var map = new mutable.HashMap[(String,String),Long]
	override def isZero: Boolean = {
		map.isEmpty
	}
	
	override def copy(): AccumulatorV2[UserVisitAction, mutable.HashMap[(String, String), Long]] = {
		new myAccumulatir
	}
	
	override def reset(): Unit = {
		map.clear()
	}
	// 增加数据
	override def add(v: UserVisitAction): Unit = {
		if(v.click_category_id != -1) {
			val key = (v.click_category_id.toString,"click")
			map(key) = map.getOrElse(key,0L) + 1L
		}else if(v.order_category_ids != "null"){
			val datas: Array[String] = v.order_category_ids.split(",")
			datas.foreach(
				id => {
					val key = (id,"order")
					map(key) = map.getOrElse(key,0L) + 1L
				}
			)
			
		}else{
			val datas: Array[String] = v.pay_category_ids.split(",")
			datas.foreach(
				id => {
					val key = (id,"pay")
					map(key) = map.getOrElse(key,0L) + 1L
				}
			)
		}
	}
	
	override def merge(other: AccumulatorV2[UserVisitAction, mutable.HashMap[(String, String), Long]]): Unit = {
		val map1 = map
		val map2 = other.value
		
		map = map1.foldLeft(map2)(
			(innerMap,kv) => {
				innerMap(kv._1) = innerMap.getOrElse(kv._1,0L) + kv._2
				innerMap
			}
		)
	}
	//获取累加器的值
	override def value: mutable.HashMap[(String, String), Long] = {
		map
	}
}

//case class UserVisitAction(date: String,
//                           user_id: Long,
//                           session_id: String,
//                           page_id: Long,
//                           action_time: String,
//                           search_keyword: String,
//                           click_category_id: Long,
//                           click_product_id: Long,
//                           order_category_ids: String,
//                           order_product_ids: String,
//                           pay_category_ids: String,
//                           pay_product_ids: String,
//                           city_id: Long)
//
//case class CategoryCountInfo(var categoryId: String,
//                             var clickCount: Long,
//                             var orderCount: Long,
//                             var payCount: Long)