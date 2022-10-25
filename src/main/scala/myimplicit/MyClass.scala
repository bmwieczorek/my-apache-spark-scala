package myimplicit

class MyClass(name: String) {
  println("Hello")

  def this() {
    this("dummy")
    println("hello")
  }

}
