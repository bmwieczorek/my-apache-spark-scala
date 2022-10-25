package myimplicit

object MyMainFunction {
  case class A(age: Int)


  def runCommand(str: String)(a: A): Unit = {
    println(str + a)
  }

  def main(args: Array[String]): Unit = {
    runCommand
    {
      "abc"
    }
    {
      A(7)
    }
    runCommand("abc")(A(7))
  }
}
