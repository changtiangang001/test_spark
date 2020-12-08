package cn._51doit.spark.day08

case class Man(name: String, var age: Int, var fv: Double) extends Ordered[Man] {

  override def compare(o: Man): Int = {
    if(this.fv == o.fv) {
      this.age - o.age
    } else {
      //- (this.fv - o.fv).toInt
      - java.lang.Double.compare(this.fv, o.fv)
    }
  }


  override def toString = s"Man($name, $age, $fv)"
}
