package com.bawi.spark

import scala.util.Random

object MyLazyTest {

  def main(args: Array[String]): Unit = {
//    lazy val expensiveConnection = getExpensiveConnection()
    val expensiveConnection = getExpensiveConnection()
    for (_ <- 1 to 20) {
      if (Random.nextInt(10) == 7) {
        println("expensive operation")
        expensiveConnection.getProperties
        println(expensiveConnection)
      }
      else
        println("light operation")
    }
    println("finished")
  }

  class MyExpensiveConnection() {
    def getProperties: String = "properties"

    override def toString = {
      println("Println [MyExpensiveConnection]")
      "[MyExpensiveConnection]"
    }
  }

  def getExpensiveConnection(): MyExpensiveConnection = {
    println("getting expensive connection")
    new MyExpensiveConnection
  }

}
