package cn._51doit.spark.day08

object OrderingContext {

  //隐式的参数（隐式object）
  implicit object OrderingPerson extends Ordering[Person] {

    override def compare(x: Person, y: Person): Int = {
      if(x.fv == y.fv) {
        x.age - y.age
      } else {
        java.lang.Double.compare(y.fv, x.fv)
      }
    }
  }

  implicit val orderPerson: Ordering[Person] = new Ordering[Person] {

    override def compare(x: Person, y: Person): Int = {
      if(x.fv == y.fv) {
        x.age - y.age
      } else {
        java.lang.Double.compare(y.fv, x.fv)
      }
    }
  }
}
