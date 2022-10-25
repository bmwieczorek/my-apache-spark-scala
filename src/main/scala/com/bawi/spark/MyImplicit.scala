package com.bawi.spark

object MyImplicit {
  case class MyIntOp(i: Int) {
    def __X_X__(j: Int): String = s"$i(__X_X__)$j"
  }

  case class Quantity(value: Int)

  implicit val disc: Double = 0.5

  implicit def quantityToValue(q: Quantity): Double = q.value

  implicit def foo(i: Int): MyIntOp = MyIntOp(i)

  def main(args: Array[String]): Unit = {
    println(calculate(Quantity(2)))
    println(1 __X_X__ 2)
    println((1).__X_X__(2))
  }

  private def calculate(qty: Quantity)(implicit discount: Double): Double = {
    qty * discount
  }
}
