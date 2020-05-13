package org.grid

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    time {
      // set up the execution environment
      val env = ExecutionEnvironment.getExecutionEnvironment

      // get input data
      val text = env.readTextFile("https://redshift-downloads.s3.amazonaws.com/TPC-DS/100GB/call_center/call_center.dat.0000.gz")
      val counts = text.count()

      counts
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
