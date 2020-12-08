package cn._51doit.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object CountDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DistinctDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val nums: RDD[Int] = sc.parallelize(List(1,2,3,4, 5,6,7,8,9), 2)

    //val r = nums.count()

    //将计算好的每个分区的条数返回到Driver
    val arr: Array[Long] = sc.runJob(nums, (it: Iterator[Int]) => {
      var count = 0L
      while (it.hasNext) {
        count += 1L
        it.next()
      }
      count
    })

    //在Driver端进行sum
    val r = arr.sum

    println(r)

  }
}
