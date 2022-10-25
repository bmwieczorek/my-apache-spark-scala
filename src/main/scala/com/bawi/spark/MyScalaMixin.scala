package com.bawi.spark

abstract class AbstractIterator[T] {
  def hasNext: Boolean
  def next(): T
}

class StringIterator(string: String) extends AbstractIterator[Char] {
  var i = 0;
  override def hasNext: Boolean = i < string.length
  override def next(): Char = {
    val c = string.charAt(i)
    i = i + 1
    c
  }
}

trait RichIteratorTrait[T] extends AbstractIterator[T] {
  def foreach(f: T => Unit): Unit = {
    while (hasNext) f(next())
  }
}

object MyScalaMixin {
  def main(args: Array[String]): Unit = {
    class RichStringIterator(string: String) extends StringIterator(string) with RichIteratorTrait[Char]
    new RichStringIterator("hello").foreach(println)
  }
}
