package cn._51doit.spark.day09

object TestObj {

  def main(args: Array[String]): Unit = {

    val f = () => {
      IpRulesLoader.getAllRules
    }

    println(f)

  }
}
