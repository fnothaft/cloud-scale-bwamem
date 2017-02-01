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

package cs.ucla.edu.bwaspark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast

import cs.ucla.edu.bwaspark.datatype._
import cs.ucla.edu.bwaspark.worker1.BWAMemWorker1._
import cs.ucla.edu.bwaspark.worker1.BWAMemWorker1Batched._
import cs.ucla.edu.bwaspark.worker2.BWAMemWorker2._
import cs.ucla.edu.bwaspark.worker2.MemSamPe._
import cs.ucla.edu.bwaspark.sam.SAMHeader
import cs.ucla.edu.bwaspark.sam.SAMWriter
import cs.ucla.edu.bwaspark.sam.SAMHDFSWriter
import cs.ucla.edu.bwaspark.debug.DebugFlag._
import cs.ucla.edu.bwaspark.util.SWUtil._
import cs.ucla.edu.bwaspark.commandline._
import cs.ucla.edu.bwaspark.broadcast.ReferenceBroadcast

import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.rdd.read.AlignedReadRDD
import org.bdgenomics.adam.models.{ SequenceDictionary, RecordGroup, RecordGroupDictionary }

import htsjdk.samtools.SAMFileHeader

import java.io.FileReader
import java.io.BufferedReader
import java.text.SimpleDateFormat
import java.util.Calendar

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }
import scala.concurrent.duration._
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import java.net.URI

object FastMap {
  private val MEM_F_PE: Int = 0x2
  private val MEM_F_ALL = 0x8
  private val MEM_F_NO_MULTI = 0x10
  private val packageVersion = "cloud-scale-bwamem-0.2.2"

  /**
   *  memMain: the main function to perform read mapping
   *
   *  @param sc the spark context object
   *  @param bwamemArgs the arguments of CS-BWAMEM
   */
  def memMain(sc: SparkContext, bwamemArgs: BWAMEMCommand) {
    val fastaLocalInputPath = bwamemArgs.fastaInputPath // the local BWA index files (bns, pac, and so on)
    val fastqHDFSInputPath = bwamemArgs.fastqHDFSInputPath // the raw read file stored in HDFS
    val isPairEnd = bwamemArgs.isPairEnd // perform pair-end or single-end mapping
    val batchFolderNum = bwamemArgs.batchedFolderNum // the number of raw read folders in a batch to be processed
    val isPSWBatched = bwamemArgs.isPSWBatched // whether the pair-end Smith Waterman is performed in a batched way
    val subBatchSize = bwamemArgs.subBatchSize // the number of reads to be processed in a subbatch
    val isPSWJNI = bwamemArgs.isPSWJNI // whether the native JNI library is called for better performance
    val jniLibPath = bwamemArgs.jniLibPath // the JNI library path in the local machine
    val outputChoice = bwamemArgs.outputChoice // the output format choice
    val outputPath = bwamemArgs.outputPath // the output path in the local or distributed file system
    val readGroupString = bwamemArgs.headerLine // complete read group header line: Example: @RG\tID:foo\tSM:bar

    val samHeader = new SAMHeader
    var adamHeader = new SequenceDictionary
    val samFileHeader = new SAMFileHeader
    var seqDict: SequenceDictionary = null
    var readGroupDict: RecordGroupDictionary = null
    var readGroup: RecordGroup = null

    // get HDFS information
    val conf: Configuration = new Configuration
    val hdfs: FileSystem = FileSystem.get(new URI(fastqHDFSInputPath), conf)
    val status = hdfs.listStatus(new Path(fastqHDFSInputPath))
    val fastqInputFolderNum = status.size // the number of folders generated in the HDFS for the raw reads
    bwamemArgs.fastqInputFolderNum = fastqInputFolderNum // the number of folders generated in the HDFS for the raw reads
    println("HDFS master: " + hdfs.getUri.toString)
    println("Input HDFS folder number: " + bwamemArgs.fastqInputFolderNum)

    if (samHeader.bwaSetReadGroup(readGroupString)) {
      println("Head line: " + samHeader.readGroupLine)
      println("Read Group ID: " + samHeader.bwaReadGroupID)
    } else println("Error on reading header")
    val readGroupName = samHeader.bwaReadGroupID

    // loading index files
    println("Load Index Files")
    val bwaIdx = new BWAIdxType
    bwaIdx.load(fastaLocalInputPath, 0)

    // loading BWA MEM options
    println("Load BWA-MEM options")
    val bwaMemOpt = new MemOptType
    bwaMemOpt.load

    bwaMemOpt.flag |= MEM_F_ALL
    bwaMemOpt.flag |= MEM_F_NO_MULTI

    // write SAM header
    println("Output choice: " + outputChoice)

    samHeader.bwaGenSAMHeader(bwaIdx.bns, packageVersion, readGroupString, samFileHeader)
    seqDict = SequenceDictionary(samFileHeader)
    readGroupDict = RecordGroupDictionary.fromSAMHeader(samFileHeader)
    readGroup = readGroupDict(readGroupName)

    // pair-end read mapping
    if (isPairEnd) {
      bwaMemOpt.flag |= MEM_F_PE
      memPairEndMapping(sc, bwamemArgs, bwaMemOpt, bwaIdx, samHeader, seqDict, readGroup)
    } else {
      // single-end read mapping
      memSingleEndMapping(sc, fastaLocalInputPath, fastqHDFSInputPath, fastqInputFolderNum, batchFolderNum, bwaMemOpt, bwaIdx, outputChoice, outputPath, samHeader, seqDict, readGroup)
    }

  }

  private def arrayToFragments(reads: Array[AlignmentRecord]): Fragment = {
    val readBuffer = reads.toBuffer
    Fragment.newBuilder
      .setAlignments(readBuffer)
      .build
  }

  /**
   *  memPairEndMapping: the main function to perform pair-end read mapping
   *
   *  @param sc the spark context object
   *  @param bwamemArgs the arguments of CS-BWAMEM
   *  @param bwaMemOpt the MemOptType object
   *  @param bwaIdx the BWAIdxType object
   *  @param samHeader the SAM header file used for writing SAM output file
   *  @param seqDict (optional) the sequences (chromosome) dictionary: used for ADAM format output
   *  @param readGroup (optional) the read group: used for ADAM format output
   */
  private def memPairEndMapping(sc: SparkContext, bwamemArgs: BWAMEMCommand, bwaMemOpt: MemOptType, bwaIdx: BWAIdxType,
                                samHeader: SAMHeader, seqDict: SequenceDictionary = null, readGroup: RecordGroup = null) {
    // Get the input arguments
    val fastaLocalInputPath = bwamemArgs.fastaInputPath // the local BWA index files (bns, pac, and so on)
    val fastqHDFSInputPath = bwamemArgs.fastqHDFSInputPath // the raw read file stored in HDFS
    val fastqInputFolderNum = bwamemArgs.fastqInputFolderNum // the number of folders generated in the HDFS for the raw reads
    val batchFolderNum = bwamemArgs.batchedFolderNum // the number of raw read folders in a batch to be processed
    val isPSWBatched = bwamemArgs.isPSWBatched // whether the pair-end Smith Waterman is performed in a batched way
    val subBatchSize = bwamemArgs.subBatchSize // the number of reads to be processed in a subbatch
    val isPSWJNI = bwamemArgs.isPSWJNI // whether the native JNI library is called for better performance
    val jniLibPath = bwamemArgs.jniLibPath // the JNI library path in the local machine
    val outputChoice = bwamemArgs.outputChoice // the output format choice
    val outputPath = bwamemArgs.outputPath // the output path in the local or distributed file system
    val isSWExtBatched = bwamemArgs.isSWExtBatched // whether the SWExtend is executed in a batched way
    val swExtBatchSize = bwamemArgs.swExtBatchSize // the batch size used for used for SWExtend
    val isFPGAAccSWExtend = bwamemArgs.isFPGAAccSWExtend // whether the FPGA accelerator is used for accelerating SWExtend
    val fpgaSWExtThreshold = bwamemArgs.fpgaSWExtThreshold // the threshold of using FPGA accelerator for SWExtend
    val jniSWExtendLibPath = bwamemArgs.jniSWExtendLibPath // (optional) the JNI library path used for SWExtend FPGA acceleration

    // Initialize output writer
    val samWriter = new SAMWriter
    val samHDFSWriter = new SAMHDFSWriter(outputPath)
    // broadcast shared variables
    // If each node has its own copy of human reference genome, we can bypass the broadcast from the driver node.
    // Otherwise, we need to use Spark broadcast
    var isLocalRef = false
    if (bwamemArgs.localRef == 1)
      isLocalRef = true
    val bwaIdxGlobal = sc.broadcast(new ReferenceBroadcast(sc.broadcast(bwaIdx), isLocalRef, fastaLocalInputPath))

    val bwaMemOptGlobal = sc.broadcast(bwaMemOpt)

    // Used to avoid time consuming adamRDD.count (numProcessed += adamRDD.count)
    // Assume the number of read in one batch is the same (This is determined when uploading FASTQ to HDFS)
    val fragmentRdd = sc.loadFragments(fastqHDFSInputPath)

    // *****   PROFILING    *******
    var worker1Time: Long = 0
    var calMetricsTime: Long = 0
    var worker2Time: Long = 0
    var ioWaitingTime: Long = 0

    var numProcessed: Long = 0
    var isSAMWriteDone: Boolean = true // a done signal for writing SAM file

    var pes: Array[MemPeStat] = new Array[MemPeStat](4)
    var j = 0
    while (j < 4) {
      pes(j) = new MemPeStat
      j += 1
    }

    val recordGroupDictionary = if (fragmentRdd.recordGroups.recordGroups.isEmpty) {
      RecordGroupDictionary(Seq(readGroup))
    } else {
      fragmentRdd.recordGroups
    }

    fragmentRdd.transform(rdd => {

      // SWExtend() is not processed in a batched way (by default)
      val stageOneReads = if (!isSWExtBatched) {
        rdd.map(pairSeq => pairEndBwaMemWorker1(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bwt, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac, null, pairSeq))
      } else {
        // SWExtend() is processed in a batched way. FPGA accelerating may be applied
        def it2ArrayIt_W1(iter: Iterator[Fragment]): Iterator[Array[PairEndReadType]] = {
          val batchedDegree = swExtBatchSize
          var counter = 0
          var ret: Vector[Array[PairEndReadType]] = scala.collection.immutable.Vector.empty
          var end1 = new Array[AlignmentRecord](batchedDegree)
          var end2 = new Array[AlignmentRecord](batchedDegree)

          while (iter.hasNext) {
            val pairEnd = iter.next
            end1(counter) = pairEnd.getAlignments.get(0)
            end2(counter) = pairEnd.getAlignments.get(1)
            counter += 1
            if (counter == batchedDegree) {
              ret = ret :+ pairEndBwaMemWorker1Batched(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bwt, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac,
                null, end1, end2, batchedDegree, isFPGAAccSWExtend, fpgaSWExtThreshold, jniSWExtendLibPath)
              counter = 0
            }
          }

          if (counter != 0) {
            ret = ret :+ pairEndBwaMemWorker1Batched(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bwt, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac,
              null, end1, end2, counter, isFPGAAccSWExtend, fpgaSWExtThreshold, jniSWExtendLibPath)
          }

          ret.toArray.iterator
        }

        rdd.mapPartitions(it2ArrayIt_W1).flatMap(s => s)
      }
      stageOneReads.cache

      // MemPeStat (Reduce step)
      val peStatPrepRDD = stageOneReads.map(pairSeq => memPeStatPrep(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bns.l_pac, pairSeq))
      val peStatPrepArray = peStatPrepRDD.collect

      memPeStatCompute(bwaMemOptGlobal.value, peStatPrepArray, pes)

      // Worker2 (Map step)
      def it2ArrayIt(iter: Iterator[PairEndReadType]): Iterator[Fragment] = {
        var counter = 0
        var ret: Vector[Array[AlignmentRecord]] = scala.collection.immutable.Vector.empty
        var subBatch = new Array[PairEndReadType](subBatchSize)
        while (iter.hasNext) {
          subBatch(counter) = iter.next
          counter = counter + 1
          if (counter == subBatchSize) {
            ret = ret :+ pairEndBwaMemWorker2PSWBatchedADAMRet(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac, 0, pes, subBatch, subBatchSize, isPSWJNI, jniLibPath, samHeader, seqDict, readGroup)
            counter = 0
          }
        }
        if (counter != 0)
          ret = ret :+ pairEndBwaMemWorker2PSWBatchedADAMRet(bwaMemOptGlobal.value, bwaIdxGlobal.value.value.bns, bwaIdxGlobal.value.value.pac, 0, pes, subBatch, counter, isPSWJNI, jniLibPath, samHeader, seqDict, readGroup)
        ret.toArray.iterator.map(arrayToFragments)
      }

      val adamObjRdd = stageOneReads.mapPartitions(it2ArrayIt)
      stageOneReads.unpersist()
      adamObjRdd
    }).copy(sequences = seqDict,
      recordGroups = recordGroupDictionary)
      .saveAsParquet(outputPath)

  }

  /**
   *  memSingleEndMapping: the main function to perform single-end read mapping
   *
   *  @param sc the spark context object
   *  @param fastaLocalInputPath the local BWA index files (bns, pac, and so on)
   *  @param fastqHDFSInputPath the raw read file stored in HDFS
   *  @param fastqInputFolderNum the number of folders generated in the HDFS for the raw reads
   *  @param batchFolderNum the number of raw read folders in a batch to be processed
   *  @param bwaMemOpt the MemOptType object
   *  @param bwaIdx the BWAIdxType object
   *  @param outputChoice the output format choice
   *  @param outputPath the output path in the local or distributed file system
   *  @param samHeader the SAM header file used for writing SAM output file
   *  @param seqDict (optional) the sequences (chromosome) dictionary: used for ADAM format output
   *  @param readGroup (optional) the read group: used for ADAM format output
   */
  private def memSingleEndMapping(sc: SparkContext, fastaLocalInputPath: String, fastqHDFSInputPath: String, fastqInputFolderNum: Int, batchFolderNum: Int,
                                  bwaMemOpt: MemOptType, bwaIdx: BWAIdxType, outputChoice: Int, outputPath: String, samHeader: SAMHeader,
                                  seqDict: SequenceDictionary = null, readGroup: RecordGroup = null) {
    // broadcast shared variables
    //val bwaIdxGlobal = sc.broadcast(bwaIdx, fastaLocalInputPath)  // read from local disks!!!
    val bwaIdxGlobal = sc.broadcast(bwaIdx) // broadcast
    val bwaMemOptGlobal = sc.broadcast(bwaMemOpt)

    var numProcessed: Long = 0

    // Used to avoid time consuming adamRDD.count (numProcessed += adamRDD.count)
    // Assume the number of read in one batch is the same (This is determined when uploading FASTQ to HDFS)
    val fragmentRdd = sc.loadFragments(fastqHDFSInputPath)

    val recordGroupDictionary = if (fragmentRdd.recordGroups.recordGroups.isEmpty) {
      RecordGroupDictionary(Seq(readGroup))
    } else {
      fragmentRdd.recordGroups
    }

    val adamRDD = fragmentRdd.transform(rdd => {
      rdd.flatMap(seq => {
        seq.getAlignments.map(read => {
          bwaMemWorker1(bwaMemOptGlobal.value,
            bwaIdxGlobal.value.bwt,
            bwaIdxGlobal.value.bns,
            bwaIdxGlobal.value.pac,
            null,
            read)
        })
      }).zipWithUniqueId
        .flatMap(p => {
          val (read, id) = p
          singleEndBwaMemWorker2ADAMOut(bwaMemOptGlobal.value,
            read.regs,
            bwaIdxGlobal.value.bns,
            bwaIdxGlobal.value.pac,
            read.seq,
            id,
            samHeader,
            seqDict,
            readGroup).map(read => {
              val readBuffer = Buffer(read)
              Fragment.newBuilder
                .setAlignments(readBuffer)
                .build
            })
        })
    }).copy(sequences = seqDict,
      recordGroups = recordGroupDictionary)
      .saveAsParquet(outputPath)
  }
}
