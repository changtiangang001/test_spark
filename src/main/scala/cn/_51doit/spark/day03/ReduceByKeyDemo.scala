package cn._51doit.spark.day03

import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.{Aggregator, HashPartitioner, SparkConf, SparkContext}

object ReduceByKeyDemo {

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

    //val reduced: RDD[(String, Int)] = wordAndOne.reduceByKey(_+_)

    //现在使用groupByKey实现reduceByKey的功能，哪一个更高效
    //val grouped = wordAndOne.groupByKey()
    //val reduced = grouped.mapValues(_.sum)

    //使用combineByKey实现reduceByKey的功能
//    val f1 = (x: Int) => x
//    val f2 = (m: Int, n: Int) => m + n
//    val f3 = (a: Int, b: Int) => a + b
//    val reduced = wordAndOne.combineByKey(f1, f2, f3)
//    reduced.saveAsTextFile("reduce3-out")

    //使用自己new ShuffledRDD实现类似reduceByKey的功能

    val shuffledRDD: ShuffledRDD[String, Int, Int] = new ShuffledRDD[String, Int, Int](
      wordAndOne,
      new HashPartitioner(wordAndOne.partitions.length)
    )
    shuffledRDD.setMapSideCombine(true)
    val f1 = (x: Int) => x
    val f2 = (x: Int, y: Int) => x + y
    val f3 = (z: Int, k: Int) => z + k
    shuffledRDD.setAggregator(new Aggregator[String, Int, Int](
       f1, f2, f3
    ))
    shuffledRDD.saveAsTextFile("shuffle-reduce")
    sc.stop()

  }
}
