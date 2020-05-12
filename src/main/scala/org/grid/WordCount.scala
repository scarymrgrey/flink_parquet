package org.grid

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    time {
      // set up the execution environment
      val env = ExecutionEnvironment.getExecutionEnvironment

      // get input data
      val text = env.readTextFile("src/main/resources/dummy.txt")
      val counts = text
        .flatMap {
        _.toLowerCase.split(" ")
      }
        .mapPartition { in => in map { (_, 1) } }

        .groupBy(0)

        .sum(1)
        .sortPartition(1, Order.ASCENDING)

      counts.print()
    }
  }

  def time[R](block: => R): R = {
    val t0 = System.currentTimeMillis()
    val result = block // call-by-name
    val t1 = System.currentTimeMillis()
    println("Elapsed time: " + (t1 - t0) / 1000 + " sec")
    result
  }
}
