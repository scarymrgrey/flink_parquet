package org.grid

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.scala._
import org.apache.flink.hadoopcompatibility.HadoopInputs
import org.apache.hadoop.io.ArrayWritable
import org.apache.parquet.avro.AvroParquetInputFormat
import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.api.scala.{ExecutionEnvironment}

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.{FileInputFormat}
import org.apache.flink.api.scala._
import org.apache.parquet.avro.AvroParquetInputFormat


object Job2 {

  case class post(title: String)

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment

    val job = Job.getInstance()
    val dIf = new HadoopInputFormat[Void, ArrayWritable](new AvroParquetInputFormat(), classOf[Void], classOf[ArrayWritable], job)
    val value = "src/main/resources/stackoverflow_posts_parq"
    FileInputFormat.addInputPath(job, new Path(value))
    val orcSrc = env.createInput(dIf)
    orcSrc.print()

    env.execute("Flink Scala API Skeleton")
  }
}
