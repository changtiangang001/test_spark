package cn._51doit.spark.day05

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object FavTeacherTopN3 {

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

    //初始化分区
    val subjectPartitioner = new SubjectPartitioner(subjects)

    //按照学科进行分区
    val partitioned: RDD[((String, String), Int)] = reduced.partitionBy(subjectPartitioner)

    val result = partitioned.mapPartitions(it => {
      it.toList.sortBy(-_._2).take(topN).iterator
    })

    println(result.collect().toBuffer)

    sc.stop()

  }
}

class SubjectPartitioner(val subjects: Array[String]) extends Partitioner {

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
    val tuple = key.asInstanceOf[(String, String)]
    val subject = tuple._1
    nameToNum(subject) //根据学科名称获取对应的分区编号
  }
}