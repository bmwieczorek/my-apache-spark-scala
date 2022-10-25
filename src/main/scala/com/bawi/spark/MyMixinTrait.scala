package com.bawi.spark

case class Scenario(name: String)

trait ScenarioProvider {
  def scenario: Scenario = {
    println("in scenario")
    Scenario("aaa")
  }
  println("ScenarioProvider")

}

trait Executor {
  def execute(scenario: Scenario): Unit = {
    println(scenario.name)
  }
}

abstract class AppBase {
  val appBaseField: String = {
    println("appBaseField")
    "appBaseField"
  }
  println("appBase")

  def scenario: Scenario
  def execute(scenario: Scenario): Unit
  def start(): Unit = {
    println("started")
    execute(scenario)
    println("finished")
  }
}

object MyMixinTrait {
  def main(args: Array[String]): Unit = {
    class App extends AppBase with Executor with ScenarioProvider
    new App().start()
  }
}
