/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucla.cs.bwaspark

import org.apache.spark.SparkContext._
import org.apache.spark.{ Logging, SparkContext }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro._
import org.bdgenomics.utils.cli._
import org.kohsuke.args4j.{ Argument, Option => Args4jOption }

object CloudScaleBWAMEM extends BDGCommandCompanion {
  val commandName = "cloud-scale-bwamem"
  val commandDescription = "Aligns reads to a reference genome using Spark and BWA-MEM"

  def apply(cmdLine: Array[String]) = {
    new CloudScaleBWAMEM(Args4j[CloudScaleBWAMEMArgs](cmdLine))
  }
}

class CloudScaleBWAMEMArgs extends Args4jBase with ParquetArgs {
  @Argument(required = true, metaVar = "INDEX", usage = "The path to the BWA indices.", index = 0)
  var fastaInputPath: String = null

  @Argument(required = true, metaVar = "INPUT", usage = "The reads to align.", index = 1)
  var fastqHDFSInputPath: String = null

  @Argument(required = true, metaVar = "OUTPUT", usage = "The output path to save reads to.", index = 2)
  var outputPath: String = null

  @Args4jOption(required = false, name = "-paired", usage = "Run alignment with paired end reads.")
  var isPairEnd: Boolean = false

  @Args4jOption(required = false, name = "-pswBatchSize", usage = "Number of reads to process in a Smith-Waterman batch. Default is 10. Setting to <=1 disables batching.")
  var swSubBatchSize: Int = 10

  @Args4jOption(required = false, name = "-pswJniLibPath", usage = "Path to the optional Smith-Waterman JNI library.")
  var pswJniLibPath: String = null

  @Args4jOption(required = false, name = "-useLocalRef", usage = "Load the reference locally, bypassing the broadcast.")
  var localRef: Boolean = false

  @Args4jOption(required = false, name = "-headerLine", usage = "SAM header lines to add. Defaults to empty.")
  var headerLine: String = ""

  @Args4jOption(required = false, name = "-swExtBatchSize", usage = "Number of reads to process in a Smith-Waterman extend batch. Default is 1,024. Setting to <=1 disables batching.")
  var swExtBatchSize: Int = 10

  @Args4jOption(required = false, name = "-fpgaThreshold", usage = "Threshold for using FPGA. Default is -1; setting to a negative number disables. ")
  var fpgaSWExtThreshold: Int = -1

  @Args4jOption(required = false, name = "-swExtendJniLibPath", usage = "Path to the optional Smith-Waterman Extend JNI library.")
  var jniSWExtendLibPath: String = null
}

class CloudScaleBWAMEM(protected val args: CloudScaleBWAMEMArgs) extends BDGSparkCommand[CloudScaleBWAMEMArgs] {
  val companion = CloudScaleBWAMEM

  def run(sc: SparkContext) {
    FastMap.memMain(sc, args)
  }
}
