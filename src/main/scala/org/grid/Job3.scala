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

import org.apache.flink.api.java.hadoop.mapreduce.HadoopInputFormat
import org.apache.flink.api.scala.{ExecutionEnvironment, _}
import org.apache.flink.hadoopcompatibility.HadoopInputs
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.ArrayWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.parquet.avro.AvroParquetInputFormat

/**
 * Skeleton for a Flink Job.
 *
 * For a full example of a Flink Job, see the WordCountJob.scala file in the
 * same package/directory or have a look at the website.
 *
 * You can also generate a .jar file that you can submit on your Flink
 * cluster. Just type
 * {{{
 *   sbt clean assembly
 * }}}
 * in the projects root directory. You will find the jar in
 * target/scala-2.11/Flink\ Project-assembly-0.1-SNAPSHOT.jar
 *
 */

object Job3 {
  case class post(title: String)
  def main(args: Array[String]): Unit = {
    // set up the execution environment

    val env = ExecutionEnvironment.getExecutionEnvironment

    val job = Job.getInstance()
    val parquetPath: String = "/path_to_file_dir/parquet_0000"

//    val hadoopInputFormat: HadoopInputFormat[Void, ArrayWritable] = HadoopInputs.readHadoopFile(new MapredParquetInputFormat(), classOf[Void], classOf[ArrayWritable], parquetPath)
//    val stream: DataStream[(Void, ArrayWritable)] = streamExecutionEnvironment.createInput(hadoopInputFormat).map { record =>
//      // process the record here ...
//    }
    env.execute("Flink Scala API Skeleton")
  }
}
