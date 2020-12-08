package cn._51doit.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object FullOuterJoinDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("FullOuterJoinDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(("spark",1), ("hadoop", 1), ("spark", 2), ("hive", 2),("flink", 2)), 2)

    val rdd2 = sc.parallelize(List(("spark", 3), ("hbase", 3), ("hadoop", 4)), 2)

    //val rdd: RDD[(String, (Option[Int], Option[Int])] = rdd1.fullOuterJoin(rdd2)
    //使用cogroup实现类似rightOuterJoin的功能
    val rdd3: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

    val rdd4: RDD[(String, (Option[Int], Option[Int]))] = rdd3.flatMapValues{
      case (i1, Seq()) => i1.iterator.map(x => (Some(x), None))
      case (Seq(), i2) => i2.iterator.map(y => (None, Some(y)))
      case (i1, i2) => for(a <- i1.iterator; b <- i2.iterator) yield (Some(a), Some(b))
    }
    rdd4.saveAsTextFile("out-fulljoin")

  }
}
