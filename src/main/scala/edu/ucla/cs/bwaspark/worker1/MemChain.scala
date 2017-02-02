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
import edu.ucla.cs.bwaspark.worker1.SAPos2RefPos._
import java.util.{ Comparator, TreeSet }
import scala.collection.mutable.MutableList
import scala.math._

//standalone object for generating all MEM chains for each read
object MemChain {

  //get the next start point for forward and backward extension

  def smemNext(itr: SMemItrType, splitLen: Int, splitWidth: Int, startWidth: Int): Array[BWTIntvType] = {

    //if the start point has exceeded the length
    //or it has gone back to negative number
    //return null 
    if (itr.start >= itr.len || itr.start < 0) null
    else {
      //skip ambiguous bases
      while (itr.start < itr.len && itr.query(itr.start) > 3) itr.start += 1

      //if all the bases left are N bases, return null
      if (itr.start == itr.len) null

      else {
        //skipping all the N bases, the point is actually the real start point
        var oriStart = itr.start

        //now, call the bwtSMem1 to generate the next start point

        //create a BWTSMem object to call the function
        val smemObj = new BWTSMem

        itr.start = smemObj.bwtSMem1(itr.bwt, itr.len, itr.query, oriStart, startWidth, itr.matches, itr.tmpVec0, itr.tmpVec1)

        assert(itr.matches.length > 0) //in theory, there is at least one match

        //looking for the longest match
        var maxBWTIntv = itr.matches.maxBy(ele => (ele.endPoint - ele.startPoint))
        var maxLength = maxBWTIntv.endPoint - maxBWTIntv.startPoint
        var middlePointOfMax = (maxBWTIntv.endPoint + maxBWTIntv.startPoint) / 2

        //if the longest SMEM is unique and long
        if (splitLen > 0 && splitLen <= maxLength && maxBWTIntv.s <= splitWidth) {

          //re-do the seeding process starting from the middle of the longest MEM
          val tmp = smemObj.bwtSMem1(itr.bwt, itr.len, itr.query, middlePointOfMax, (maxBWTIntv.s + 1).toInt, itr.sub, itr.tmpVec0, itr.tmpVec1)

          //only some seeds in the sub array can still be there
          //1)length of the seed should be no less than maxLength/2
          //2)endPoint should exceed original start point
          itr.sub = itr.sub.filter(ele => ((ele.endPoint - ele.startPoint) >= maxLength / 2) &&
            ele.endPoint > oriStart)

          //merge itr.matches and itr.sub and sort by start point (end point if start point equals)
          itr.matches = (itr.matches.++(itr.sub)).sortWith((a, b) => (if (a.startPoint < b.startPoint) true else if (a.startPoint > b.startPoint) false else a.endPoint > b.endPoint))
        }
        var res: Array[BWTIntvType] = itr.matches.toArray

        res
      }
    }
  }

  //generate a chain tree for each read

  def generateChainTree(opt: MemOptType, l_pac: Long, smemItr: SMemItrType): TreeSet[MemChainType] = {

    //calculate splitLen
    val splitLen = min((opt.minSeedLen * opt.splitFactor + 0.499).toInt, smemItr.len)

    //!!!be careful!!!
    //we temporarily assign startWidth as 1 other than 2
    //but it need be fixed
    //val startWidth = { if (opt.flag & MEM_F_NO_EXACT) 2 else 1 }
    val startWidth = 1

    //the mutable list is for storing all the bi-intervals generated from specific point
    var bwtIntvOnPoint: Array[BWTIntvType] = null

    //the return value: mutable TreeSet which maintains all the chains
    var chainTree: TreeSet[MemChainType] = null

    //if bi-intervals responding to specific point are generated
    //go through each seed, either
    //1) merge it to existing chain
    //2) generate new chain from it

    bwtIntvOnPoint = smemNext(smemItr, splitLen, opt.splitWidth, startWidth)

    var idx = 0;
    while (bwtIntvOnPoint != null) {
      //traverse all the seeds
      for (i <- 0 until bwtIntvOnPoint.length) {

        //end - start = length of the seed
        val seedLen = bwtIntvOnPoint(i).endPoint - bwtIntvOnPoint(i).startPoint

        //ignore the seed if it it too short or too repetitive
        if (seedLen < opt.minSeedLen || bwtIntvOnPoint(i).s > opt.maxOcc) {
          //do nothing
        } //the statements in the else clause are the main part
        //it will traverse all the possible positions in the suffix array(reference)
        else {
          //traverse each aligned position
          for (j <- 0 until bwtIntvOnPoint(i).s.toInt) {

            //prepare for generating a new seed
            var rBeg = suffixArrayPos2ReferencePos(smemItr.bwt, bwtIntvOnPoint(i).k + j)
            var qBeg = bwtIntvOnPoint(i).startPoint
            var len = seedLen

            //handle edge cases
            if (rBeg < l_pac && l_pac < rBeg + len) {
              //do nothing if a seed crossing the boundary
            } else {
              //generate a seed for this position
              val newSeed = new MemSeedType(rBeg, qBeg, len)

              //find the closest chain in the existing chain tree
              //the closest chain should satisfy
              //1)chain.pos <= seed.rbegin
              //2)the chain.pos is the largest one of all chains that satisfy 1)

              def findClosestChain(chainTree: TreeSet[MemChainType], refPoint: Long): MemChainType = {
                //if the tree is empty, return null
                if (chainTree == null) null
                else {
                  //create a temporary chain for finding the lower chain
                  //because lower.pos < tmpChain.pos
                  //having refPoint + 1 to handle if lower.pos == tmpChain.pos
                  val tmpChain = new MemChainType(refPoint + 1, null)
                  val res = chainTree.lower(tmpChain)
                  val tmp = chainTree.higher(tmpChain)
                  res
                }
              }

              val targetChain = findClosestChain(chainTree, newSeed.rBeg)

              //test if the seed can be merged into some existing chain
              //return true/false
              //if return true, actually also DID the merging task

              //define tryMergeSeedToChain to test if a seed can be merged with some chain in the chain tree
              def tryMergeSeedToChain(opt: MemOptType, l_pac: Long, chain: MemChainType, seed: MemSeedType): Boolean = {

                //get query begin and end, reference begin and end
                //!!!to clarify!!!: the order of seeds in a chain
                //qBeg sorting? or rBeg sorting?
                val qBegChain = chain.seeds.head.qBeg
                val rBegChain = chain.seeds.head.rBeg
                val qEndChain = chain.seeds.last.qBeg + chain.seeds.last.len
                val rEndChain = chain.seeds.last.rBeg + chain.seeds.last.len

                //if the seed is fully contained by the chain, return true
                if (qBegChain <= seed.qBeg && qEndChain >= seed.qBeg + seed.len &&
                  rBegChain <= seed.rBeg && rEndChain >= seed.rBeg + seed.len)
                  true
                //if not in the same strand (crossing l_pac boundary), return false
                else if ((rBegChain < l_pac || chain.seeds.last.rBeg < l_pac) &&
                  seed.rBeg >= l_pac)
                  false
                else {
                  //follow the conditions judged in original BWA test_and_merge function
                  val x: Int = seed.qBeg - chain.seeds.last.qBeg // always non-negtive???
                  val y: Int = (seed.rBeg - chain.seeds.last.rBeg).toInt

                  if (y >= 0 &&
                    x - y <= opt.w &&
                    y - x <= opt.w &&
                    x - chain.seeds.last.len.toInt < opt.maxChainGap &&
                    y - chain.seeds.last.len.toInt < opt.maxChainGap) {
                    //all the conditions are satisfied? growing the chain
                    chain.seeds += seed
                    true //return true
                  } else false
                }
              }

              val isMergable = if (targetChain == null) false else tryMergeSeedToChain(opt, l_pac, targetChain, newSeed)

              //add the seed as a new chain if not mergable
              if (!isMergable) {
                val newSeedList = MutableList[MemSeedType](newSeed)
                val newChain = new MemChainType(rBeg, newSeedList)
                //Push the new chain to the chain tree
                //1)if the chainTree is empty
                if (chainTree == null) {
                  //using java style to new a TreeSet[MemChainType]
                  chainTree = new TreeSet[MemChainType](new Comparator[MemChainType]() {
                    def compare(a: MemChainType, b: MemChainType): Int = {
                      if (a.pos > b.pos) 1
                      else if (a.pos < b.pos) -1
                      else 0
                    }
                  })
                  //insert the chain to the tree
                  chainTree.add(newChain)
                } //2)if the chainTree is not empty, directly add it
                else chainTree.add(newChain)
              }
            }
          }
        }
      }
      bwtIntvOnPoint = smemNext(smemItr, splitLen, opt.splitWidth, startWidth)
    }

    //finally, return the tree

    chainTree
  }

  def traverseChainTree(chainTree: TreeSet[MemChainType]): Array[MemChainType] = {
    //if the tree is empty, return null
    if (chainTree == null) null

    //else, it's gonna be a simple map() task
    else {
      val itr = chainTree.iterator
      val chains = new Array[MemChainType](chainTree.size).map(ele => itr.next)
      chains.foreach(ele => {
        ele.seedsRefArray = ele.seeds.toArray
      })
      chains
    }
  }

  //generate chains for each read
  def generateChains(opt: MemOptType, bwt: BWTType, l_pac: Long, len: Int, seq: Array[Byte]): Array[MemChainType] = {

    //if the query is shorter than the seed length, no match, return null
    if (len < opt.minSeedLen) {
      null
    } //the else part the real meaty part for this function
    else {
      //generate a SMemItrType object for smemNext
      val smemItr = new SMemItrType(bwt,
        seq,
        0, //the first startpoint for smemNext is 0
        len,
        new MutableList[BWTIntvType](), //matches array
        new MutableList[BWTIntvType](), //sub array
        new MutableList[BWTIntvType](), //temporary array 0
        new MutableList[BWTIntvType]()) //temporary array 1

      //generate a tree for all chains
      val chainTree = generateChainTree(opt, l_pac, smemItr)
      traverseChainTree(chainTree)
    }
  }
}
