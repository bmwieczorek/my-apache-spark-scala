package myimplicit

case class MyRow(value: Any) {
  override def toString: String = s"Row[$value]"
}

trait MyEncoder[T] {
  def toMyRow(t: T): MyRow
}

class MySpark {

  object myimplicits {

    implicit val intEncoder: MyEncoder[Int] = new MyEncoder[Int] {
      override def toMyRow(t: Int): MyRow = MyRow(s"int($t)")
    }

    implicit val stringEncoder: MyEncoder[String] = new MyEncoder[String] {
      override def toMyRow(t: String): MyRow = MyRow(s"string($t)")
    }

//    implicit def convertToDF[T](seq: Seq[T])(implicit e: MyEncoder[T]): MyDatasetHolder = {
//      new MyDatasetHolder(createDataset(seq))
//    }

    implicit def convertToDF2[T: MyEncoder](seq: Seq[T]) = {
      new MyDatasetHolder(createDataset2(seq))
    }
  }
  private def encoderFor[T](implicit e: MyEncoder[T]): MyEncoder[T] = e

  private def encoderFor2[T: MyEncoder]: MyEncoder[T] = implicitly[MyEncoder[T]] match {
    case e: MyEncoder[T] => e
    case _ => sys.error(s"Only myencoders are supported today")
  }

  private def createDataset[T](seq: Seq[T])(implicit e: MyEncoder[T]): Seq[MyRow] = {
    val encoder: MyEncoder[T] = encoderFor[T]
    seq.map(encoder.toMyRow)
  }

  private def createDataset2[T: MyEncoder](seq: Seq[T]): Seq[MyRow] = {
    val encoder: MyEncoder[T] = encoderFor2[T]
    seq.map(encoder.toMyRow)
  }
}

object MySpark {
  def build(): MySpark = new MySpark
}
