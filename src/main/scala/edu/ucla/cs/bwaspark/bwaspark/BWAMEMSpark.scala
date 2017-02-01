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

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

import scala.collection.mutable.MutableList

import edu.ucla.cs.bwaspark.datatype._
import edu.ucla.cs.bwaspark.worker1.BWAMemWorker1._
import edu.ucla.cs.bwaspark.worker2.BWAMemWorker2._
import edu.ucla.cs.bwaspark.FastMap.memMain
import edu.ucla.cs.bwaspark.commandline._
import edu.ucla.cs.bwaspark.dnaseq._

import org.bdgenomics.adam.rdd.ADAMContext._

object BWAMEMSpark {
  private def bwamemCmdLineParser(argsList: List[String]): BWAMEMCommand = {
    type OptionMap = Map[Symbol, Any]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      list match {
        case Nil => map
        case "-bfn" :: value :: tail =>
          nextOption(map ++ Map('batchedFolderNum -> value.toInt), tail)
        case "-bPSW" :: value :: tail =>
          nextOption(map ++ Map('isPSWBatched -> value.toInt), tail)
        case "-sbatch" :: value :: tail =>
          nextOption(map ++ Map('subBatchSize -> value.toInt), tail)
        case "-bPSWJNI" :: value :: tail =>
          nextOption(map ++ Map('isPSWJNI -> value.toInt), tail)
        case "-jniPath" :: value :: tail =>
          nextOption(map ++ Map('jniLibPath -> value), tail)
        case "-oChoice" :: value :: tail =>
          nextOption(map ++ Map('outputChoice -> value.toInt), tail)
        case "-oPath" :: value :: tail =>
          nextOption(map ++ Map('outputPath -> value), tail)
        case "-localRef" :: value :: tail =>
          nextOption(map ++ Map('localRef -> value), tail)
        case "-R" :: value :: tail =>
          nextOption(map ++ Map('headerLine -> value), tail)
        case "-isSWExtBatched" :: value :: tail =>
          nextOption(map ++ Map('isSWExtBatched -> value.toInt), tail)
        case "-bSWExtSize" :: value :: tail =>
          nextOption(map ++ Map('swExtBatchSize -> value.toInt), tail)
        case "-FPGAAccSWExt" :: value :: tail =>
          nextOption(map ++ Map('isFPGAAccSWExtend -> value.toInt), tail)
        case "-FPGASWExtThreshold" :: value :: tail =>
          nextOption(map ++ Map('FPGASWExtThreshold -> value.toInt), tail)
        case "-jniSWExtendLibPath" :: value :: tail =>
          nextOption(map ++ Map('jniSWExtendLibPath -> value.toString), tail)
        case isPairEnd :: inFASTAPath :: inFASTQPath :: Nil =>
          nextOption(map ++
            Map('isPairEnd -> isPairEnd.toInt) ++
            Map('inFASTAPath -> inFASTAPath) ++
            Map('inFASTQPath -> inFASTQPath), list.tail.tail.tail)
        case option :: tail =>
          println("[Error] Unknown option " + option)
          exit(1)
      }
    }
    val options = nextOption(Map(), argsList)
    println(options)

    val bwamemArgs = new BWAMEMCommand
    if (options.get('batchedFolderNum) != None)
      bwamemArgs.batchedFolderNum = options('batchedFolderNum).toString.toInt
    if (options.get('isPSWBatched) != None) {
      val isPSWBatched = options('isPSWBatched).toString.toInt
      if (isPSWBatched == 1)
        bwamemArgs.isPSWBatched = true
      else if (isPSWBatched == 0)
        bwamemArgs.isPSWBatched = false
      else {
        println("[Error] Undefined -bPSW argument" + isPSWBatched)
        exit(1)
      }
    }
    if (options.get('subBatchSize) != None)
      bwamemArgs.subBatchSize = options('subBatchSize).toString.toInt
    if (options.get('isPSWJNI) != None) {
      val isPSWJNI = options('isPSWJNI).toString.toInt
      if (isPSWJNI == 1)
        bwamemArgs.isPSWJNI = true
      else if (isPSWJNI == 0)
        bwamemArgs.isPSWJNI = false
      else {
        println("[Error] Undefined -bPSWJNI argument" + isPSWJNI)
        exit(1)
      }
    }
    if (options.get('jniLibPath) != None)
      bwamemArgs.jniLibPath = options('jniLibPath).toString
    if (options.get('outputChoice) != None) {
      val outputChoice = options('outputChoice).toString.toInt
      if (outputChoice > 3) {
        println("[Error] Undefined -oChoice argument" + outputChoice)
        exit(1)
      }
      bwamemArgs.outputChoice = outputChoice
    }
    if (options.get('outputPath) != None)
      bwamemArgs.outputPath = options('outputPath).toString
    if (options.get('localRef) != None)
      bwamemArgs.localRef = options('localRef).toString.toInt
    if (options.get('headerLine) != None)
      bwamemArgs.headerLine = options('headerLine).toString
    if (options.get('isSWExtBatched) != None) {
      val isSWExtBatched = options('isSWExtBatched).toString.toInt
      if (isSWExtBatched == 0)
        bwamemArgs.isSWExtBatched = false
      else if (isSWExtBatched == 1)
        bwamemArgs.isSWExtBatched = true
      else {
        println("[Error] Undefined -isSWExtBatched argument" + isSWExtBatched)
        exit(1)
      }
    }
    if (options.get('swExtBatchSize) != None)
      bwamemArgs.swExtBatchSize = options('swExtBatchSize).toString.toInt
    if (options.get('isFPGAAccSWExtend) != None) {
      val isFPGAAccSWExtend = options('isFPGAAccSWExtend).toString.toInt
      if (isFPGAAccSWExtend == 0)
        bwamemArgs.isFPGAAccSWExtend = false
      else if (isFPGAAccSWExtend == 1)
        bwamemArgs.isFPGAAccSWExtend = true
      else {
        println("[Error] Undefined -FPGAAccSWExt argument" + isFPGAAccSWExtend)
        exit(1)
      }
    }
    if (options.get('FPGASWExtThreshold) != None)
      bwamemArgs.fpgaSWExtThreshold = options('FPGASWExtThreshold).toString.toInt

    val isPairEnd = options('isPairEnd).toString.toInt
    if (isPairEnd == 1)
      bwamemArgs.isPairEnd = true
    else if (isPairEnd == 0)
      bwamemArgs.isPairEnd = false
    else {
      println("[Error] Undefined isPairEnd argument" + isPairEnd)
      exit(1)
    }
    if (options.get('jniSWExtendLibPath) != None)
      bwamemArgs.jniSWExtendLibPath = options('jniSWExtendLibPath).toString
    bwamemArgs.fastaInputPath = options('inFASTAPath).toString
    bwamemArgs.fastqHDFSInputPath = options('inFASTQPath).toString

    println("CS- BWAMEM command line arguments: " + bwamemArgs.isPairEnd + " " + bwamemArgs.fastaInputPath + " " + bwamemArgs.fastqHDFSInputPath + " " +
      bwamemArgs.batchedFolderNum + " " + bwamemArgs.isPSWBatched + " " + bwamemArgs.subBatchSize + " " + bwamemArgs.isPSWJNI + " " + bwamemArgs.jniLibPath + " " +
      bwamemArgs.outputChoice + " " + bwamemArgs.outputPath)

    bwamemArgs
  }

  val usage = Usage.usage

  private def commandLineParser(arg: String): String = {
    def getCommand(cmd: String): String = {
      cmd match {
        case "cs-bwamem" => cmd
        case "help" =>
          println(usage)
          exit(1)
        case _ =>
          println("Unknown command " + cmd)
          println(usage)
          exit(1)
      }
    }

    val command = getCommand(arg)
    println("command: " + command)

    command
  }

  def main(args: Array[String]) {
    val argsList = args.toList
    val command = commandLineParser(argsList(0))
    var bwamemArgs = new BWAMEMCommand
    var sortArgs = List[String]()
    val coalesceFactor = 10

    if (command == "cs-bwamem") bwamemArgs = bwamemCmdLineParser(argsList.tail)
    else {
      println("Unknown command " + command)
      exit(1)
    }

    if (command == "cs-bwamem") {
      val conf = new SparkConf().setAppName("Cloud-Scale BWAMEM: cs-bwamem")
      val sc = new SparkContext(conf)

      memMain(sc, bwamemArgs)
      println("CS-BWAMEM Finished!!!")

      // NOTE: Some of the Spark tasks are in "GET RESULT" status and cause the pending state... 
      //       However, the data are returned. Therefore, we enforce program to exit.
      exit(1)
    }
  }
}
