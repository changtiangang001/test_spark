package cn._51doit.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object RightOuterJoinDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JoinDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("spark",1), ("hadoop", 1), ("spark", 2), ("hive", 2),("flink", 2)), 2)

    val rdd2 = sc.parallelize(List(("spark", 3), ("hbase", 3), ("hadoop", 4)), 2)

    //val rdd: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
    //使用cogroup实现类似rightOuterJoin的功能
    val rdd3: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    val rdd4: RDD[(String, (Option[Int], Int))] = rdd3.flatMapValues(t => {
      if (t._1.isEmpty) {
        t._2.map((None, _))
      } else {
        for (x <- t._1.iterator; y <- t._2.iterator) yield (Some(x), y)
      }
    })
    rdd4.saveAsTextFile("out-rightjoin")

  }
}
