package cn._51doit.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FlatMapDemo {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("FlatMapDemo").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //在Driver端定义的
    val arr: Array[String] = Array("spark hadoop flink spark", "spark hadoop flinl", "spark hadoop hadoop")

    val lines: RDD[String] = sc.parallelize(arr)

    val flat: RDD[String] = lines.flatMap(x => x.split(" "))

    val r = flat.collect()

    println(r.toBuffer)
  }
}
