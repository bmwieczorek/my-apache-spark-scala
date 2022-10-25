package myimplicit

class MyDatasetHolder(seq: Seq[MyRow]) {

  def toDF = seq.mkString(",")

}
