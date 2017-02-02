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

package edu.ucla.cs.bwaspark.worker2

import scala.collection.immutable.Vector

import edu.ucla.cs.bwaspark.datatype._
import edu.ucla.cs.bwaspark.worker2.MemMarkPrimarySe._
import edu.ucla.cs.bwaspark.worker2.MemRegToADAMSAM._
import edu.ucla.cs.bwaspark.worker2.MemSamPe._
import edu.ucla.cs.bwaspark.sam.SAMHeader
import edu.ucla.cs.bwaspark.util.LocusEncode._
import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment }
import org.bdgenomics.adam.models.{ SequenceDictionary, RecordGroup }
import scala.collection.JavaConversions._
import scala.collection.mutable.Buffer

object BWAMemWorker2 {
  private val MEM_F_PE: Int = 0x2

  /**
   *  BWA-MEM Worker 2: used for single-end alignment
   *
   *  @param opt the input MemOptType object
   *  @param regs the alignment registers to be transformed
   *  @param bns the input BNTSeqType object
   *  @param pac the PAC array
   *  @param seq the read (NOTE: in the distributed version, we use AlignmentRecord data structure.)
   *  @param numProcessed the number of reads that have been proceeded
   *  @param samHeader the SAM header required to output SAM strings
   *  @return the SAM format string of the given read
   */
  def singleEndBwaMemWorker2(opt: MemOptType, regs: Array[MemAlnRegType], bns: BNTSeqType, pac: Array[Byte], seq: AlignmentRecord, numProcessed: Long, samHeader: SAMHeader): String = {
    var regsOut: Array[MemAlnRegType] = null
    if (regs != null)
      regsOut = memMarkPrimarySe(opt, regs, numProcessed)

    val seqStr = seq.getSequence
    val seqTrans: Array[Byte] = seqStr.toCharArray.map(ele => locusEncode(ele))

    memRegToSAMSe(opt, bns, pac, seq, seqTrans, regsOut, 0, null, samHeader)
  }

  /**
   *  BWA-MEM Worker 2: used for single-end alignment ADAM format output
   *
   *  @param opt the input MemOptType object
   *  @param regs the alignment registers to be transformed
   *  @param bns the input BNTSeqType object
   *  @param pac the PAC array
   *  @param seq the read (NOTE: in the distributed version, we use AlignmentRecord data structure.)
   *  @param numProcessed the number of reads that have been proceeded
   *  @param samHeader the SAM header required to output SAM strings
   *  @param seqDict the sequences (chromosome) dictionary: used for ADAM format output
   *  @param readGroup the read group: used for ADAM format output
   *  @return the ADAM format object array of the given read
   */
  def singleEndBwaMemWorker2ADAMOut(opt: MemOptType,
                                    regs: Array[MemAlnRegType],
                                    bns: BNTSeqType,
                                    pac: Array[Byte],
                                    seq: AlignmentRecord,
                                    numProcessed: Long,
                                    samHeader: SAMHeader,
                                    seqDict: SequenceDictionary,
                                    readGroup: RecordGroup): Array[AlignmentRecord] = {
    var regsOut: Array[MemAlnRegType] = null
    if (regs != null)
      regsOut = memMarkPrimarySe(opt, regs, numProcessed)

    val seqStr = seq.getSequence
    val seqTrans: Array[Byte] = seqStr.toCharArray.map(ele => locusEncode(ele))

    memRegToADAMSe(opt, bns, pac, seq, seqTrans, regsOut, 0, null, samHeader, seqDict, readGroup).toArray
  }

  /**
   *  BWA-MEM Worker 2: used for pair-end alignment (batched processing + JNI with native libraries)
   *  In addition, the ADAM format output will be write back to the distributed file system
   *
   *  @param opt the input MemOptType object
   *  @param bns the input BNSSeqType object
   *  @param pac the PAC array
   *  @param numProcessed the number of reads that have been proceeded
   *  @param pes the pair-end statistics array
   *  @param pairEndReadArray the PairEndReadType object array. Each element has both the read and the alignments information
   *  @param subBatchSize the batch size of the number of reads to be sent to JNI library for native execution
   *  @param isPSWJNI the Boolean flag to mark whether the JNI native library is going to be used
   *  @param jniLibPath the JNI library path
   *  @param samHeader the SAM header required to output SAM strings
   *  @param seqDict the sequences (chromosome) dictionary: used for ADAM format output
   *  @param readGroup the read group: used for ADAM format output
   *  @return the ADAM format object array of the given read
   */
  def pairEndBwaMemWorker2PSWBatchedADAMRet(opt: MemOptType,
                                            bns: BNTSeqType,
                                            pac: Array[Byte],
                                            numProcessed: Long,
                                            pes: Array[MemPeStat],
                                            pairEndReadArray: Array[PairEndReadType],
                                            subBatchSize: Int,
                                            isPSWJNI: Boolean,
                                            jniLibPath: String,
                                            samHeader: SAMHeader,
                                            seqDict: SequenceDictionary,
                                            readGroup: RecordGroup): Array[AlignmentRecord] = {
    var alnRegVecPairs: Array[Array[Array[MemAlnRegType]]] = new Array[Array[Array[MemAlnRegType]]](subBatchSize)
    var seqsPairs: Array[Fragment] = new Array[Fragment](subBatchSize)

    var i = 0
    while (i < subBatchSize) {
      alnRegVecPairs(i) = new Array[Array[MemAlnRegType]](2)

      seqsPairs(i) = Fragment.newBuilder
        .setAlignments(Buffer(pairEndReadArray(i).seq0, pairEndReadArray(i).seq1))
        .build
      alnRegVecPairs(i)(0) = pairEndReadArray(i).regs0
      alnRegVecPairs(i)(1) = pairEndReadArray(i).regs1
      i += 1
    }

    if (isPSWJNI) {
      System.load(jniLibPath)
      memADAMPeGroupJNI(opt, bns, pac, pes, subBatchSize, numProcessed, seqsPairs, alnRegVecPairs, samHeader, seqDict, readGroup)
    } else {
      memADAMPeGroup(opt, bns, pac, pes, subBatchSize, numProcessed, seqsPairs, alnRegVecPairs, samHeader, seqDict, readGroup)
    }
  }
}

