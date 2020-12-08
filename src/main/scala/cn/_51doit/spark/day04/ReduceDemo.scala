package cn._51doit.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ReduceDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JoinDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(1,2,3,  4,5,6), 2)

    val r: Int = rdd1.reduce(_ + _)

    rdd1.sum()



  }
}
