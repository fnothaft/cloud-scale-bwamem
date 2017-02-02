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
import java.io.{ FileReader, BufferedReader }
import scala.collection.mutable.MutableList
import scala.util.control.Breaks._

class BWTSMem {

  //constant value in the orignal algo
  val OCC_INTV_SHIFT = 7 //bwt.h
  val OCC_INTERVAL = (1 << OCC_INTV_SHIFT) //bwt.h, assume 0x80
  val OCC_INTV_MASK = OCC_INTERVAL - 1

  //parameter/local variable in the original bwt_smem1
  var m_ik: BWTIntvType = _
  var m_ok: Array[BWTIntvType] = new Array[BWTIntvType](4)
  //m_ok.foreach(_ => new BWTIntvType(0, 0, 0, 0, 0))
  m_ok(0) = new BWTIntvType(0, 0, 0, 0, 0)
  m_ok(1) = new BWTIntvType(0, 0, 0, 0, 0)
  m_ok(2) = new BWTIntvType(0, 0, 0, 0, 0)
  m_ok(3) = new BWTIntvType(0, 0, 0, 0, 0)
  var mBWT: BWTType = _

  //local variable in the original bwt_extend
  var m_tk: Array[Long] = new Array[Long](4)
  var m_tl: Array[Long] = new Array[Long](4)

  //def bwtOccIntv(k: Long) : Long = {
  //  (k >>> 7) << 4
  //}

  //def occAux4(b: Int) : Long = {
  //  mBWT.cntTable(b & 0xff) + mBWT.cntTable((b >>> 8) & 0xff) + mBWT.cntTable((b >>> 16) & 0xff) + mBWT.cntTable(b >>> 24)
  //}

  def bwt_occ4(k: Long): Array[Long] = {
    var cnt: Array[Long] = Array(0, 0, 0, 0);
    if (k == -1) cnt

    else {
      var _k = k
      if (k >= mBWT.primary) _k -= 1
      var index = (_k >>> 7) << 4 // Inline of bwtOccIntv(_k)
      var tmp_idx = index.toInt
      //assert(tmp_idx >= 0) //assertion enabled?
      //mimic memcpy in the orignal program, test needed!!!
      var i = 0
      while (i < 4) {
        cnt(i) = mBWT.bwt(tmp_idx) << 32 | mBWT.bwt(tmp_idx + 1)
        tmp_idx += 2
        i += 1
      }
      index += 8
      var end_idx = index + ((_k >>> 4) - ((_k & ~OCC_INTV_MASK) >>> 4))
      var x: Long = 0
      var b: Int = 0
      //println("index: " + index + ", end_idx: " + end_idx + ", diff: " + (end_idx - index))
      var indexInt = index.toInt
      var endIdxInt = end_idx.toInt
      while (indexInt < endIdxInt) {
        b = mBWT.bwt(indexInt)
        x += mBWT.cntTable(b & 0xff) + mBWT.cntTable((b >>> 8) & 0xff) + mBWT.cntTable((b >>> 16) & 0xff) + mBWT.cntTable(b >>> 24) // Inline occAux4(mBWT.bwt(index.toInt))
        indexInt += 1
      }

      var tmp: Int = mBWT.bwt(indexInt) & ~((1 << ((~_k & 15) << 1)) - 1)
      x += mBWT.cntTable(tmp & 0xff) + mBWT.cntTable((tmp >>> 8) & 0xff) + mBWT.cntTable((tmp >>> 16) & 0xff) + mBWT.cntTable(tmp >>> 24) - (~_k & 15) // Inline occAux4(tmp)
      cnt(0) += x & 0xff
      cnt(1) += (x >>> 8) & 0xff
      cnt(2) += (x >>> 16) & 0xff
      cnt(3) += (x >>> 24)
      cnt
    }
  }

  def bwt_2occ4(k: Long, l: Long) {
    var _k = k
    var _l = l
    if (k >= mBWT.primary) _k -= 1
    if (l >= mBWT.primary) _l -= 1
    if (_l >>> OCC_INTV_SHIFT != _k >>> OCC_INTV_SHIFT || k == -1 || l == -1) {
      m_tk = bwt_occ4(k)
      m_tl = bwt_occ4(l)
    } else {
      if (k >= mBWT.primary) _k = k - 1
      if (l >= mBWT.primary) _l = l - 1
      var index = (_k >>> 7) << 4 // Inline of bwtOccIntv(_k)
      var tmp_idx = index.toInt
      //assert(tmp_idx >= 0) //assertion enabled?
      //mimic memcpy in the orignal program, test needed!!!
      var i = 0
      while (i < 4) {
        m_tk(i) = mBWT.bwt(tmp_idx) << 32 | mBWT.bwt(tmp_idx + 1)
        tmp_idx += 2
        i += 1
      }
      index += 8 //sizeof(bwtint_t) in the original algo
      var endk_idx = index + ((_k >>> 4) - ((_k & ~OCC_INTV_MASK) >>> 4))
      var endl_idx = index + ((_l >>> 4) - ((_l & ~OCC_INTV_MASK) >>> 4))
      var x: Long = 0
      var y: Long = 0
      var b: Int = 0
      var indexInt = index.toInt
      var endkIdxInt = endk_idx.toInt
      while (indexInt < endkIdxInt) {
        b = mBWT.bwt(indexInt)
        x += mBWT.cntTable(b & 0xff) + mBWT.cntTable((b >>> 8) & 0xff) + mBWT.cntTable((b >>> 16) & 0xff) + mBWT.cntTable(b >>> 24) // Inline occAux4(mBWT.bwt(index.toInt))
        indexInt += 1
      }
      y = x
      var tmp: Int = 0
      tmp = mBWT.bwt(indexInt) & ~((1 << ((~_k & 15) << 1)) - 1)
      x += mBWT.cntTable(tmp & 0xff) + mBWT.cntTable((tmp >>> 8) & 0xff) + mBWT.cntTable((tmp >>> 16) & 0xff) + mBWT.cntTable(tmp >>> 24) - (~_k & 15) // Inline occAux4(tmp)
      var endlIdxInt = endl_idx.toInt
      while (indexInt < endlIdxInt) {
        b = mBWT.bwt(indexInt)
        y += mBWT.cntTable(b & 0xff) + mBWT.cntTable((b >>> 8) & 0xff) + mBWT.cntTable((b >>> 16) & 0xff) + mBWT.cntTable(b >>> 24) // Inline occAux4(mBWT.bwt(index.toInt))
        indexInt += 1
      }
      tmp = mBWT.bwt(indexInt) & ~((1 << ((~_l & 15) << 1)) - 1)
      y += mBWT.cntTable(tmp & 0xff) + mBWT.cntTable((tmp >>> 8) & 0xff) + mBWT.cntTable((tmp >>> 16) & 0xff) + mBWT.cntTable(tmp >>> 24) - (~_l & 15) // Inline occAux4(tmp) 
      m_tk.copyToArray(m_tl)
      m_tk(0) += x & 0xff
      m_tk(1) += (x >>> 8) & 0xff
      m_tk(2) += (x >>> 16) & 0xff
      m_tk(3) += (x >>> 24)

      m_tl(0) += y & 0xff
      m_tl(1) += (y >>> 8) & 0xff
      m_tl(2) += (y >>> 16) & 0xff
      m_tl(3) += (y >>> 24)
    }
  }

  def bwtExtend(is_back: Boolean) {
    var cond: Boolean = false
    if (is_back) {
      bwt_2occ4(m_ik.k - 1, m_ik.k - 1 + m_ik.s)
      var i = 0
      while (i < 4) {
        m_ok(i).k = mBWT.L2(i) + 1 + m_tk(i)
        m_ok(i).s = m_tl(i) - m_tk(i)
        i += 1
      }
      cond = ((m_ik.k <= mBWT.primary) && (m_ik.k + m_ik.s - 1 >= mBWT.primary))
      m_ok(3).l = m_ik.l
      if (cond) m_ok(3).l += 1
      m_ok(2).l = m_ok(3).l + m_ok(3).s
      m_ok(1).l = m_ok(2).l + m_ok(2).s
      m_ok(0).l = m_ok(1).l + m_ok(1).s
    } else {
      bwt_2occ4(m_ik.l - 1, m_ik.l - 1 + m_ik.s)
      var i = 0
      while (i < 4) {
        m_ok(i).l = (mBWT.L2(i) + 1 + m_tk(i))
        m_ok(i).s = m_tl(i) - m_tk(i)
        i += 1
      }
      cond = ((m_ik.l <= mBWT.primary) && (m_ik.l + m_ik.s - 1 >= mBWT.primary))
      m_ok(3).k = m_ik.k
      if (cond) m_ok(3).k += 1
      m_ok(2).k = m_ok(3).k + m_ok(3).s
      m_ok(1).k = m_ok(2).k + m_ok(2).s
      m_ok(0).k = m_ok(1).k + m_ok(1).s
    }
  }

  //len: q's length
  def bwtSMem1(bwt: BWTType, len: Int, q: Array[Byte], x: Int, min_intv: Int, mem: MutableList[BWTIntvType], tmpvec_0: MutableList[BWTIntvType], tmpvec_1: MutableList[BWTIntvType]): Int = {
    mem.clear

    //if (q(x) > 3)
    //return x + 1

    // MODIFIED by Yu-Ting Chen
    if (q(x) > 3)
      x + 1
    else {
      var min_intv_copy: Int = 0
      if (min_intv < 1)
        min_intv_copy = 1
      else
        min_intv_copy = min_intv

      var prev = tmpvec_0
      var curr = tmpvec_1
      var swap: MutableList[BWTIntvType] = new MutableList[BWTIntvType]()
      mBWT = bwt
      // start pos for ik = 0?
      m_ik = new BWTIntvType(0, x + 1, bwt.L2(q(x)) + 1, bwt.L2(3 - q(x)) + 1, bwt.L2(q(x) + 1) - bwt.L2(q(x)))
      var c: Int = 0
      var breaked: Boolean = false

      var i: Int = x + 1
      while (i < len && breaked == false) { //forward search
        if (q(i) < 4) { //an A/C/G/T base
          c = 3 - q(i)
          bwtExtend(false) //bwt_forward_extend
          if (m_ok(c).s != m_ik.s) {
            var m_ik_copy = new BWTIntvType(m_ik.startPoint, m_ik.endPoint, m_ik.k, m_ik.l, m_ik.s)
            curr.+=:(m_ik_copy)
            if (m_ok(c).s < min_intv_copy) {
              breaked = true
            }
          }
          if (breaked == false) {
            m_ik.copy(m_ok(c))
            m_ik.endPoint = i + 1
          }
        } else {
          var m_ik_copy = new BWTIntvType(m_ik.startPoint, m_ik.endPoint, m_ik.k, m_ik.l, m_ik.s)
          curr.+=:(m_ik_copy) //prepend the item to the list -- no need to reverse later
          breaked = true
        }
        i += 1
      }

      if (breaked == false) {
        var m_ik_copy = new BWTIntvType(m_ik.startPoint, m_ik.endPoint, m_ik.k, m_ik.l, m_ik.s)
        curr.+=:(m_ik_copy)
      }

      //var ret : Int = curr(0).endPoint
      var ret: Int = curr.head.endPoint
      swap = curr
      curr = prev
      prev = swap

      i = x - 1

      breaked = false
      while (i >= -1 && breaked == false) { //backward extension
        if (i < 0)
          c = -1
        else {
          if (q(i) < 4) c = q(i) else c = -1
        }

        curr.clear
        var j = 0
        while (j < prev.length) {
          m_ik = prev(j)
          bwtExtend(true) //bwt_extend on p
          if (c < 0 || m_ok(c).s < min_intv_copy) {
            if (curr.isEmpty) {
              //if (mem.isEmpty || i + 1 < mem.last.startPoint) {
              if (mem.isEmpty || i + 1 < mem.head.startPoint) {
                var m_ik_copy: BWTIntvType = new BWTIntvType(0, 0, 0, 0, 0)
                m_ik_copy.copy(m_ik)
                m_ik_copy.startPoint = i + 1
                mem.+=:(m_ik_copy)
              }
            }
          } else if (curr.isEmpty || m_ok(c).s != curr.last.s) {
            m_ok(c).startPoint = m_ik.startPoint
            m_ok(c).endPoint = m_ik.endPoint
            var m_ok_copy = new BWTIntvType(m_ok(c).startPoint, m_ok(c).endPoint, m_ok(c).k, m_ok(c).l, m_ok(c).s)
            curr += m_ok_copy
          }

          j += 1
        }
        if (curr.isEmpty) breaked = true
        else {
          swap = curr
          curr = prev
          prev = swap
          i -= 1
        }
      }

      ret
    }
  }

  //below are for testing purpose
  var test_len: Int = 0
  var test_q: Array[Byte] = _
  var test_x: Int = 0
  var test_min_intv = 0

  def readTestData(fileName: String) {
    val reader = new BufferedReader(new FileReader(fileName))

    var line = reader.readLine
    test_len = line.toInt
    println(test_len)

    line = reader.readLine
    println(line)
    test_q = line.getBytes
    test_q = test_q.map(s => (s - 48).toByte)

    line = reader.readLine
    test_x = line.toInt
    println(test_x)

    line = reader.readLine
    test_min_intv = line.toInt
    println(test_min_intv)
  }
}
