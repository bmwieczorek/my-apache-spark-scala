package com.bawi.spark

abstract class AbstractIterator2 {
  type T
  def hasNext: Boolean
  def next(): T
}

class StringIterator2(string: String) extends AbstractIterator2 {
  type T = Char
  var i = 0;

  override def hasNext: Boolean = i < string.length

  override def next(): Char = {
    val c = string.charAt(i)
    i += 1
    c
  }
}

trait RichIterator2 extends AbstractIterator2 {
  def foreach(f: T => Unit): Unit = {
    while (hasNext) f(next())
  }
}

object MyScalaMixin2 {
  def main(args: Array[String]): Unit = {
    class RichStringIterator2(string: String) extends StringIterator2(string) with RichIterator2
    new RichStringIterator2("hello").foreach(println)
  }
}
