package com.atguigu.bigdata.spark.oneStore

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

object needOne {
	def main(args: Array[String]): Unit = {
		//1.配置文件
		val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("fclife")
		//2.获取driver对象
		val sc = new SparkContext(conf)
		//3.获取原始数据
		val lines: RDD[String] = sc.textFile("F:\\Program Files\\CODE\\ProjectHadoop\\spark-test\\input\\user_visit_action.txt")
		
		//4.拆分数据,并封装数据进入对象
		val objRDD: RDD[UserVisitAction] = lines.map(data => {
			val info: Array[String] = data.split("_")
			UserVisitAction(
				info(0),
				info(1).toLong,
				info(2),
				info(3).toLong,
				info(4),
				info(5),
				info(6).toLong,
				info(7).toLong,
				info(8),
				info(9),
				info(10),
				info(11),
				info(12).toLong
			)
		})
		//5.取出有用数据，并扁平化下单分类和支付分类
		val flatMapRDD: RDD[CategoryCountInfo] = objRDD.flatMap(
			datas => {
				datas match {
					case UserVisitAction(date: String,
					user_id: Long,
					session_id: String,
					page_id: Long,
					action_time: String,
					search_keyword: String,
					click_category_id: Long,
					click_product_id: Long,
					order_category_ids: String,
					order_product_ids: String,
					pay_category_ids: String,
					pay_product_ids: String,
					city_id: Long
					) if (click_category_id != -1) => List(CategoryCountInfo(click_category_id.toString, 1, 0, 0))
					case UserVisitAction(date: String,
					user_id: Long,
					session_id: String,
					page_id: Long,
					action_time: String,
					search_keyword: String,
					click_category_id: Long,
					click_product_id: Long,
					order_category_ids: String,
					order_product_ids: String,
					pay_category_ids: String,
					pay_product_ids: String,
					city_id: Long
					) if (order_category_ids != "null") => {
						val idData: mutable.ArrayOps[String] = order_category_ids.split(",")
						idData
						val infoes = new ListBuffer[CategoryCountInfo]()
						idData.foreach(
							data => {
								infoes.append(CategoryCountInfo(data, 0, 1, 0))
							}
						)
						infoes
					}
					
					case UserVisitAction(date: String,
					user_id: Long,
					session_id: String,
					page_id: Long,
					action_time: String,
					search_keyword: String,
					click_category_id: Long,
					click_product_id: Long,
					order_category_ids: String,
					order_product_ids: String,
					pay_category_ids: String,
					pay_product_ids: String,
					city_id: Long
					) if (pay_category_ids != "null") => {
						val idData: mutable.ArrayOps[String] = pay_category_ids.split(",")
						val infoes = new ListBuffer[CategoryCountInfo]()
						idData.foreach(
							data => {
								infoes.append(CategoryCountInfo(data, 0, 0, 1))
							}
						)
						infoes
					}
					case _ => Nil
				}
			}
		)
		val infoRDD: RDD[(String, Iterable[CategoryCountInfo])] = flatMapRDD.groupBy(data => data.categoryId)
		
		val finallRDD: RDD[CategoryCountInfo] = infoRDD.map(
			data => {
				data._2.reduce((x, y) => {
					x.clickCount = x.clickCount + y.clickCount
					x.orderCount = x.orderCount + y.orderCount
					x.payCount = x.payCount + y.payCount
					x
				})
			}
		)
		val result: Array[CategoryCountInfo] = finallRDD.sortBy(
			data => (data.clickCount, data.orderCount, data.payCount),
			false
		).take(10)
		result.foreach(println)
	}
}

case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long)

case class CategoryCountInfo(var categoryId: String,
                             var clickCount: Long,
                             var orderCount: Long,
                             var payCount: Long)