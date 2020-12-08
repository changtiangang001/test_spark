package cn._51doit.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IntersectionDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DistinctDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(1,2,3,4,5), 2)
    val rdd2 = sc.parallelize(List(4,5,6,7,8), 2)

    //val rdd3 = rdd1.intersection(rdd2)

    //使用cogroup实现类似intersection的功能
    val rdd3 = rdd1.map((_, null))
    val rdd4 = rdd2.map((_, null))

    val grouped: RDD[(Int, (Iterable[Null], Iterable[Null]))] = rdd3.cogroup(rdd4)

    val rdd5 = grouped.filter(t => (
      t._2._1.nonEmpty && t._2._2.nonEmpty
      )) //.map(_._1)
      .keys

    val res = rdd5.collect()
    println(res.toBuffer)

    Thread.sleep(10000000)



  }
}
