package cn._51doit.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MakeRDDDemo {

  def main(args: Array[String]): Unit = {

    //创建SparkContext
    val conf = new SparkConf().setAppName("WordCount")
    //SparkContext是用来创建最原始的RDD的
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4), 2)

    sc.makeRDD(Array(1,2,3,4), 2)

  }
}
