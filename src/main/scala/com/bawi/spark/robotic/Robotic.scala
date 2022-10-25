package com.bawi.spark.robotic
import scala.collection.immutable

object Robotic {
  def main(args: Array[String]): Unit = {
    val ranges: immutable.Seq[Range] = Vector.fill(5)(10 until 15)
//    println(ranges)
    val flatten: immutable.Seq[Int] = ranges.flatten
//    println(flatten)
    val los = Vector.fill(4)(repeatElementsInList((5 until 7 + 1), 14 - 10 + 1))
//    println(los)
//    println(los.flatten)

    val hour = repeatElementsInList((0 until 4), ((2 + 1) * (3 + 1)))
    println(hour)

  }

  def repeatElementsInList(list: Range, times: Int): IndexedSeq[Int] = {
    list.map(x => Vector.fill(times)(x)).flatten
  }





}
