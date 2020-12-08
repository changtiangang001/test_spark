package cn._51doit.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object FavTeacherTopN7 {

  def main(args: Array[String]): Unit = {

    val isLocal = args(0).toBoolean

    val conf = new SparkConf().setAppName(this.getClass.getCanonicalName)

    if (isLocal) {
      conf.setMaster("local[*]")
    }

    val sc = new SparkContext(conf)

    //指定以后从哪里读取数据创建RDD
    val lines = sc.textFile(args(1))

    val topN = args(2).toInt

    //对数据进行整理
    val reduced: RDD[((String, String), Int)] = lines.map(line => {
      val fields = line.split("/")
      val url = fields(2)
      val teacher = fields(3)
      val subject = url.split("[.]")(0)
      ((subject, teacher), 1)
    }).reduceByKey(_ + _)

    //计算学科的数量
    val subjects: Array[String] = reduced.map(_._1._1).distinct().collect()

    val mapped: RDD[((String, String, Int), Null)] = reduced.map(t => {
      //把参与排序的字段放到key中了，value是null
      ((t._1._1, t._1._2, t._2), null)
    })

    //自定义一个分区器,初始化分区
    val partitioner = new SubjectPartitioner2(subjects)

    //调用一个算子，可以重新分区，并且在每一个分区内排序
    implicit val orderingRules: Ordering[(String, String, Int)] = new Ordering[(String, String, Int)] {
      override def compare(x: (String, String, Int), y: (String, String, Int)): Int = {
        -(x._3 - y._3)
      }
    }
    //使用自定义的分区器进行分区（shuffle），然后在每一个分区内进行排序
    val result = mapped.repartitionAndSortWithinPartitions(partitioner).map(_._1)

    val r = result.collect()

    println(r.toBuffer)

    sc.stop()
  }
}

class SubjectPartitioner2(val subjects: Array[String]) extends Partitioner {

  //在主构造器中定义分区规则
  val nameToNum = new mutable.HashMap[String, Int]()
  var i = 0
  for (sub <- subjects) {
    nameToNum(sub) = i
    i += 1
  }


  override def numPartitions: Int = subjects.length

  //在Executor的Task中，shuffle Write之前会调用
  override def getPartition(key: Any): Int = {
    val tp = key.asInstanceOf[(String, String, Int)]
    val subject = tp._1
    nameToNum(subject) //根据学科名称获取对应的分区编号
  }
}

