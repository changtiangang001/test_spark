package cn._51doit.spark.day08

class Boy(val name: String, var age: Int, var fv: Double) extends Comparable[Boy] with Serializable {

  override def compareTo(o: Boy): Int = {
    if(this.fv == o.fv) {
      this.age - o.age
    } else {
      //- (this.fv - o.fv).toInt
      - java.lang.Double.compare(this.fv, o.fv)
    }
  }


  override def toString = s"Boy($name, $age, $fv)"
}
