package cn._51doit.spark.day02

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MapFilterDemo {

  def main(args: Array[String]): Unit = {

    //创建SparkContext
    val conf = new SparkConf().setAppName("WordCount")
    //SparkContext是用来创建最原始的RDD的
    val sc: SparkContext = new SparkContext(conf)

    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4), 2)
    val f1 = (x: Int) => x % 2 == 0 //在Driver端定义的
    val f2 = (a: Int) => a * 10
    val rdd2: RDD[Int] = rdd1.filter(f1)
    val rdd3: RDD[Int] = rdd2.map(f2)


    rdd2.map(x => (x , 1)).groupByKey()

    val res: Array[Int] = rdd3.collect()


  }
}
