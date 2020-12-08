package cn._51doit.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CogroupDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DistinctDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("tom", 1), ("tom", 2), ("jerry", 3), ("kitty", 2)))
    val rdd2 = sc.parallelize(List(("jerry", 2), ("tom", 1), ("shuke", 2)))
    val rdd3 = rdd1.cogroup(rdd2)




    Thread.sleep(10000000)



  }
}
