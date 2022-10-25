package com.bawi.spark

object MyImplicit2 {

  case class MyIntOp(i: Int) {
    def __X_X__(j: Int): String = s"$i(__X_X__)$j"
  }

  implicit def foo(i: Int): MyIntOp = MyIntOp(i)

  def main(args: Array[String]): Unit = {
    val str = 1 __X_X__ 2
    val str2 = 1.__X_X__(2)
    val str3 = (1).__X_X__(2)
    println(str)
    println(str2)
    println(str3)
  }
}
