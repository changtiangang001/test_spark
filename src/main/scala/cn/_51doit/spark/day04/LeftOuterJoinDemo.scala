package cn._51doit.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object LeftOuterJoinDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JoinDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("spark",1), ("hadoop", 1), ("spark", 2), ("hive", 2),("flink", 2)), 2)

    val rdd2 = sc.parallelize(List(("spark", 3), ("hive", 3), ("hadoop", 4)), 2)

    //val rdd3 = rdd1.join(rdd2)

    //val rdd: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
    //使用cogroup实现类似leftOuterJoin的功能
    val rdd3: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    val rdd4: RDD[(String, (Int, Option[Int]))] = rdd3.flatMapValues(t => {
      if (t._2.isEmpty) {
        t._1.map((_, None))
      } else {
        for (x <- t._1.iterator; y <- t._2.iterator) yield (x, Some(y))
      }
    })

    rdd4.saveAsTextFile("out-leftjoin")

  }
}
