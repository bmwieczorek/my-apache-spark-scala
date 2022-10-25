package myimplicit

import scala.collection.mutable.ArrayBuffer

object MyMain {

  def main(args: Array[String]): Unit = {
    val myspark = MySpark.build()

//   import myspark.myimplicits._
    import myspark.myimplicits.convertToDF2
    import myspark.myimplicits.intEncoder
    val ints = Seq(1, 2, 3)
    println(ints.toDF)

    import myspark.myimplicits.stringEncoder
    val strings = Seq("a", "b", "c")
    println(strings.toDF)
  }


}
