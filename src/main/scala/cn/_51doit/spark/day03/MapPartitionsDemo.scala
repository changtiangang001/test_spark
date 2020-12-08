package cn._51doit.spark.day03

import org.apache.hadoop.mapreduce.v2.proto.MRProtos.TaskAttemptCompletionEventProto
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext, TaskContext}

object MapPartitionsDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("MapPartitionsDemo").setMaster("local[*]")

    val sc = new SparkContext(conf)

    //创建RDD
    val nums: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), 3)

    //调用map方法:每处理一条数据就调用一次传入到map方法中的函数
    val result: RDD[(Int, Int)] = nums.map(x => {
      //创建一个连接

      println("*************************************")
      val index = TaskContext.getPartitionId()
      (index, x * 100)
      //使用连接
      //关闭连接
      //返回处理后的数据
    })

    //调用mapPartitions方法，可以将数据以分区为单位取出来，一个分区就是一个迭代器,你出入的函数一个分区调用一次
    val result2 = nums.mapPartitions(it => {
      //一个分区取出一个连接
      println("#################################")
      val index = TaskContext.getPartitionId()
      val nIt: Iterator[(Int, Int)] = it.map(x => {
        //每一条数据会处理一次，反复使用事先创建好的连接
        (index, x * 100)
      })
      //关闭连接
      nIt //返回新的迭代器
    }, false)

//    val result3 = nums.mapPartitions(it => {
//      println("#################################")
//      val index = TaskContext.getPartitionId()
//      it.filter(x => {
//        x % 2 == 0
//      })
//
//    })


    val result4: RDD[String] = nums.mapPartitionsWithIndex((index, it) => {
      it.map(e => {
        s"partitionIndex: $index , element: $e"
      })
    })


    val words: RDD[String] = sc.parallelize(Array("hadoop", "spark", "flink", "hadoop"), 6)

    val result5 = words.mapPartitionsWithIndex((i, it) => {
      it.map(e => {
        s"partitionIndex: $i, ele: $e"
      })
    })

    val arr = result5.collect()
    println(arr.toBuffer)




    //words.saveAsTextFile("map4-out")


    sc.stop()
  }
}
