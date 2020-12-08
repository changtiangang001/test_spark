package cn._51doit.spark.day03

import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.rdd.{RDD, ShuffledRDD}

import scala.collection.mutable.ArrayBuffer

object GroupByKeyDemo {

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

    val grouped: RDD[(String, Iterable[Int])] = wordAndOne.groupByKey()
    //grouped.saveAsTextFile("group-out")

    //ShuffledRDD实现跟groupByKey效果一样功能
    val shuffledRDD: ShuffledRDD[String, Int, ArrayBuffer[Int]] = new ShuffledRDD[String, Int, ArrayBuffer[Int]](
      wordAndOne,
      new HashPartitioner(wordAndOne.partitions.length)
    )
    //前两个函数都是在map side执行的
    //创建一个Combiner：就是讲每一个组内的第一个Value方法到ArrayBuffer
    val createCombiner = (x: Int) => ArrayBuffer(x)
    //将组内的其他value一次遍历追加到同一个ArrayBuffer
    val mergeValue = (ab: ArrayBuffer[Int], e: Int) => ab += e
    //第三个函数是在全局合并的执行的，是在shuffleRead之后
    val mergeCombiners = (ab1: ArrayBuffer[Int], ab2: ArrayBuffer[Int]) => ab1 ++= ab2

    shuffledRDD.setAggregator(new Aggregator[String, Int, ArrayBuffer[Int]](
      createCombiner,
      mergeValue,
      mergeCombiners
    ))
    //分组不能在mapside合并
    shuffledRDD.setMapSideCombine(false)

    shuffledRDD.saveAsTextFile("shuffle-out")
    //Thread.sleep(100000000)

    sc.stop()

  }
}
