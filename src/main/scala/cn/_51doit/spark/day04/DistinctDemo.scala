package cn._51doit.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object DistinctDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DistinctDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val nums: RDD[Int] = sc.parallelize(List(1, 2, 1, 3, 4, 1, 5, 3, 3, 5, 5, 4), 4)

    val disticned: RDD[Int] = nums.distinct(8)

    //使用reduceByKey实时distinct的功能
    //val res: RDD[Int] = nums.map((_, null)).reduceByKey((x, _) => x).map(_._1)

    //val res: Array[Int] = disticned.collect()
    println(disticned.collect().toBuffer)

    Thread.sleep(10000000)



  }
}
