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

package edu.ucla.cs.bwaspark.worker1

import edu.ucla.cs.bwaspark.datatype._
import edu.ucla.cs.bwaspark.util.LocusEncode._
import edu.ucla.cs.bwaspark.worker1.MemChain._
import edu.ucla.cs.bwaspark.worker1.MemChainFilter._
import edu.ucla.cs.bwaspark.worker1.MemChainToAlignBatched._
import edu.ucla.cs.bwaspark.worker1.MemSortAndDedup._
import java.util.TreeSet
import java.util.Comparator
import org.bdgenomics.formats.avro.{ AlignmentRecord, Fragment }
import org.bdgenomics.utils.misc.Logging
import scala.collection.mutable.MutableList

//this standalone object defines the main job of BWA MEM:
//1)for each read, generate all the possible seed chains
//2)using SW algorithm to extend each chain to all possible aligns
object BWAMemWorker1Batched extends Logging {

  /**
   *  Perform BWAMEM worker1 function for single-end alignment
   *  Alignment is processed in a batched way
   *
   *  @param opt the MemOptType object, BWAMEM options
   *  @param bwt BWT and Suffix Array
   *  @param bns .ann, .amb files
   *  @param pac .pac file (PAC array: uint8_t)
   *  @param pes pes array for worker2
   *  @param seqArray a batch of reads
   *  @param numOfReads #reads in the batch
   *
   *  Return: a batch of reads with alignments
   */
  def bwaMemWorker1Batched(opt: MemOptType, //BWA MEM options
                           bwt: BWTType, //BWT and Suffix Array
                           bns: BNTSeqType, //.ann, .amb files
                           pac: Array[Byte], //.pac file uint8_t
                           pes: Array[MemPeStat], //pes array
                           seqArray: Array[AlignmentRecord], //the batched reads
                           numOfReads: Int, //the number of the batched reads
                           runOnFPGA: Boolean, //if run on FPGA
                           threshold: Int //the batch threshold to run on FPGA
                           ): Array[ReadType] = { //all possible alignments for all the reads  

    val readArray = new Array[Array[Byte]](numOfReads)
    val lenArray = new Array[Int](numOfReads)
    var i = 0
    while (i < numOfReads) {
      readArray(i) = seqArray(i).getSequence
        .toCharArray
        .map(locus => locusEncode(locus))
      lenArray(i) = seqArray(i).getSequence.length
      i = i + 1
    }

    //first & second step: chaining and filtering
    val chainsFilteredArray = new Array[Array[MemChainType]](numOfReads)
    i = 0;
    while (i < numOfReads) {
      chainsFilteredArray(i) = memChainFilter(opt, generateChains(opt, bwt, bns.l_pac, lenArray(i), readArray(i)))
      i = i + 1;
    }

    val readRetArray = new Array[ReadType](numOfReads)
    i = 0;
    while (i < numOfReads) {
      readRetArray(i) = new ReadType
      readRetArray(i).seq = seqArray(i)
      i = i + 1
    }

    val preResultsOfSW = new Array[Array[SWPreResultType]](numOfReads)
    val numOfSeedsArray = new Array[Int](numOfReads)
    val regArrays = new Array[MemAlnRegArrayType](numOfReads)
    i = 0;
    while (i < numOfReads) {
      if (chainsFilteredArray(i) == null) {
        preResultsOfSW(i) = null
        numOfSeedsArray(i) = 0
        regArrays(i) = null
      } else {
        preResultsOfSW(i) = new Array[SWPreResultType](chainsFilteredArray(i).length)
        var j = 0;
        while (j < chainsFilteredArray(i).length) {
          preResultsOfSW(i)(j) = calPreResultsOfSW(opt, bns.l_pac, pac, lenArray(i), readArray(i), chainsFilteredArray(i)(j))
          j = j + 1
        }
        numOfSeedsArray(i) = 0
        chainsFilteredArray(i).foreach(chain => {
          numOfSeedsArray(i) += chain.seeds.length
        })
        log.debug("Finished the calculation of pre-results of Smith-Waterman\nThe number of reads in this pack is: %d".format(numOfReads))
        regArrays(i) = new MemAlnRegArrayType
        regArrays(i).maxLength = numOfSeedsArray(i)
        regArrays(i).regs = new Array[MemAlnRegType](numOfSeedsArray(i))
      }
      i = i + 1;
    }
    log.debug("Finished the pre-processing part")

    memChainToAlnBatched(opt, bns.l_pac, pac, lenArray, readArray, numOfReads, preResultsOfSW, chainsFilteredArray, regArrays, runOnFPGA, threshold)
    log.debug("Finished the batched-processing part")
    regArrays.foreach(ele => { if (ele != null) ele.regs = ele.regs.filter(r => (r != null)) })
    regArrays.foreach(ele => { if (ele != null) ele.maxLength = ele.regs.length })
    i = 0;
    while (i < numOfReads) {
      if (regArrays(i) == null) readRetArray(i).regs = null
      else readRetArray(i).regs = memSortAndDedup(regArrays(i), opt.maskLevelRedun).regs
      i = i + 1
    }
    readRetArray
  }

  /**
   *  Perform BWAMEM worker1 function for pair-end alignment
   *
   *  @param opt the MemOptType object, BWAMEM options
   *  @param bwt BWT and Suffix Array
   *  @param bns .ann, .amb files
   *  @param pac .pac file (PAC array: uint8_t)
   *  @param pes pes array for worker2
   *  @param pairSeqs a read with both ends
   *
   *  Return: a read with alignments on both ends
   */
  def pairEndBwaMemWorker1Batched(opt: MemOptType, //BWA MEM options
                                  bwt: BWTType, //BWT and Suffix Array
                                  bns: BNTSeqType, //.ann, .amb files
                                  pac: Array[Byte], //.pac file uint8_t
                                  pes: Array[MemPeStat], //pes array
                                  seqArray0: Array[AlignmentRecord], //the first batch
                                  seqArray1: Array[AlignmentRecord], //the second batch
                                  numOfReads: Int, //the number of reads in each batch
                                  runOnFPGA: Boolean, //if run on FPGA
                                  threshold: Int, //the batch threshold to run on FPGA
                                  jniSWExtendLibPath: String = null // SWExtend Library Path
                                  ): Array[PairEndReadType] = { //all possible alignment  
    if (jniSWExtendLibPath != null && runOnFPGA)
      System.load(jniSWExtendLibPath)

    val readArray0 = bwaMemWorker1Batched(opt, bwt, bns, pac, pes, seqArray0, numOfReads, runOnFPGA, threshold)
    val readArray1 = bwaMemWorker1Batched(opt, bwt, bns, pac, pes, seqArray1, numOfReads, runOnFPGA, threshold)
    var pairEndReadArray = new Array[PairEndReadType](numOfReads)
    var i = 0
    while (i < numOfReads) {
      pairEndReadArray(i) = new PairEndReadType
      pairEndReadArray(i).seq0 = readArray0(i).seq
      pairEndReadArray(i).regs0 = readArray0(i).regs
      pairEndReadArray(i).seq1 = readArray1(i).seq
      pairEndReadArray(i).regs1 = readArray1(i).regs
      i = i + 1
    }

    pairEndReadArray // return
  }
}
