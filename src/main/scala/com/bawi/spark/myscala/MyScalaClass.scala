package com.bawi.spark.myscala

class MyScalaClass {
  val myField: String = "abc"
  var age: Int = _
  def getSequence(index: Int): Seq[String] = {
      Seq("abc" + index)
  }

}
