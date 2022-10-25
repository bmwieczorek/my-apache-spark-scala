package com.bawi.spark.myscala

object MyTestEncoder {

  object MyDataSet {
    def create[T](seq: Seq[T])(implicit encoder: MyEnc[T]): MyDataSet[T] = {
      new MyDataSet[T](seq.map(e => encoder.apply(e)))
    }
  }

  class MyDataSet[T](var seq: Seq[T]) {

    def map[OUT](function: T => OUT)(implicit encoder: MyEnc[OUT]): MyDataSet[OUT] = {
      val outs: Seq[OUT] = this.seq.map(e => encoder.apply(function.apply(e)))
      new MyDataSet[OUT](outs)
    }

    def show(): Unit = {
      this.seq.foreach(println)
    }

  }

  def main(args: Array[String]): Unit = {
//    apply("ala")
//    apply(2)
    MyDataSet.create(Seq("a", "bb", "ccc")).map(s => s.length).map(i => i * i).show()
  }

  class MyEnc[T] {
    def apply(value: T): T = {
      println("encoder for type: " + value.getClass + " and value: " + value)
      value
    }
  }

  implicit val intEncoder: MyEnc[Int] = {
    println("registered int encoder")
    new MyEnc[Int]
  }

  implicit val stringEncoder: MyEnc[String] = {
    println("registered string encoder")
    new MyEnc[String]
  }

  def apply[IN](in: IN)(implicit inEncoder: MyEnc[IN]): Unit = {
    inEncoder.apply(in)
  }
}
