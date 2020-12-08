package cn._51doit.spark.utils

object TestWordPartitionNum {

  def main(args: Array[String]): Unit = {

    val r = MyModUtils.nonNegativeMod("hive".hashCode, 4)

    println(r)

  }

}
