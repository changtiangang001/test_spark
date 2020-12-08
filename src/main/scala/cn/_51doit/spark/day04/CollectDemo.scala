package cn._51doit.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CollectDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JoinDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(1,2,3,  4,5,6), 2)

    val rdd2: RDD[Int] = rdd1.map(_ * 10)


    val res: Array[Int] = rdd2.collect()




  }
}
