package cn._51doit.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object TopDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DistinctDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val nums: RDD[Int] = sc.parallelize(List(1,8,3,6, 2,9,5,4,7), 2)

    val r: Array[Int] = nums.top(2)

    //将数据在每一个分区内求topN， 然后将数据收集到在Driver端进行全局topN
    println(r.toBuffer)

  }
}
