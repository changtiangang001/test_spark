package cn._51doit.spark.day04

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object ForeachPartitionDemo {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JoinDemo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val rdd1 = sc.parallelize(List(1,2,3,  4,5,6), 2)

    rdd1.foreach(t => {
      //建立数据库连接或从连接池中取出一个连接

      //将数据写入到数据库

      //关闭连接或将连接还回到连接池
    })

    rdd1.foreachPartition(it => {
      //建立数据库连接或从连接池中取出一个连接

      //对迭代器中的多条数据进行操作
      it.foreach(t => {
        //使用事先创建好的连接，写入到数据库（反复使用连接）
      })
      //关闭连接或将连接还回到连接池

    })


    //rdd1.saveAsTextFile()

  }
}
