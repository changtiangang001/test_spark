package cn._51doit.spark.day03

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object AggregateByKeyDemo {

  def main(args: Array[String]): Unit = {

    //创建SparkContext
    val conf = new SparkConf().setAppName("WordCount").setMaster("local[*]")
    //SparkContext是用来创建最原始的RDD的
    val sc: SparkContext = new SparkContext(conf)

    val words: RDD[String] = sc.parallelize(
      List(
        "spark", "hadoop", "hive", "spark",
        "spark", "flink", "spark", "hbase",
        "kafka", "kafka", "kafka", "kafka",
        "hadoop", "flink", "hive", "flink"
      ), 4)

    val wordAndOne: RDD[(String, Int)] = words.map((_, 1))

    //wordAndOne.reduceByKey(_+_)
    //val result = wordAndOne.foldByKey(100)(_ + _)

    //val result = wordAndOne.aggregateByKey(0)(_ + _, _ + _)

    //result.saveAsTextFile("aggregate-out")

    val wordAndCount = sc.parallelize(List(
      ("spark", 5), ("hadoop", 2), ("spark", 3), ("hive", 4),

      ("spark", 12), ("hadoop", 4), ("spark", 1), ("hive", 2)
    ), 2)

    val result = wordAndCount.aggregateByKey(10)(Math.max(_, _), _+_)

    result.saveAsTextFile("aggregate3-out")

    sc.stop()

  }
}
