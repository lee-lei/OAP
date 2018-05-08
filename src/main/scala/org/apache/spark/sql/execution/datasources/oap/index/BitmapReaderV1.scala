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

package org.apache.spark.sql.execution.datasources.oap.index

import java.io.DataInput
import java.io.EOFException

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.roaringbitmap.FastAggregation
import org.roaringbitmap.RoaringBitmap

import org.apache.spark.sql.execution.datasources.oap.filecache.{BitmapFiber, FiberCache, FiberCacheManager}
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.types.StructType

private[oap] class BitmapReaderV1(
    fin: FSDataInputStream,
    intervalArray: ArrayBuffer[RangeInterval],
    internalLimit: Int,
    keySchema: StructType,
    idxPath: Path,
    conf: Configuration)
    extends BitmapReader(intervalArray, keySchema, idxPath, conf) with Iterator[Int] {

  @transient private var bmRowIdIterator: Iterator[Integer] = _
  private var empty: Boolean = _

  override def hasNext: Boolean = !empty && bmRowIdIterator.hasNext
  override def next(): Int = bmRowIdIterator.next()
  override def toString: String = "BitmapReaderV1"

  private def getDesiredBitmap(fc: FiberCache): RoaringBitmap = {
    val stream = new BitmapDataInputStream(fc)
    val entry = new RoaringBitmap()
    // Below is directly reading from byte array rather than deserializing into java object.
    entry.deserialize(stream)
    entry
  }

  private def getDesiredBitmapArray(): ArrayBuffer[RoaringBitmap] = {
    val keySeq = readBmUniqueKeyList(bmUniqueKeyListCache)
    intervalArray.flatMap{
      case range if !range.isNullPredicate =>
        val (startIdx, endIdx) = getKeyIdx(keySeq, range)
        if (startIdx == -1 || endIdx == -1) {
          // range not fond in cur bitmap, return empty for performance consideration
          Seq.empty
        } else {
          (startIdx until (endIdx + 1)).map(idx => {
            val curIdxOffset = getIdxOffset(bmOffsetListCache, 0L, idx)
            val entrySize = getIdxOffset(bmOffsetListCache, 0L, idx + 1) - curIdxOffset
            val entryFiber = BitmapFiber(() => loadBmSection(fin, curIdxOffset, entrySize),
              idxPath.toString, BitmapIndexSectionId.entryListSection, idx)
            val entryCache = FiberCacheManager.get(entryFiber, conf)
            val entry = getDesiredBitmap(entryCache)
            entryCache.release
            entry
          })
        }
      case range if range.isNullPredicate =>
        val nullListFiber = BitmapFiber(
          () => loadBmSection(fin, bmNullEntryOffset, bmNullEntrySize),
          idxPath.toString, BitmapIndexSectionId.entryNullSection, 0)
        val nullListCache = FiberCacheManager.get(nullListFiber, conf)
        if (nullListCache.size != 0) {
          val entry = getDesiredBitmap(nullListCache)
          nullListCache.release
          Seq(entry)
        } else {
          Seq.empty
        }
    }
  }

  def getRowIdIterator(): Unit = {
    try {
      getDesiredSegments(fin)
      val bitmapArray = getDesiredBitmapArray
      if (bitmapArray.nonEmpty) {
        if (internalLimit > 0) {
          // Get N items from each index.
          bmRowIdIterator =
            bitmapArray.flatMap(bm => bm.iterator.asScala.take(internalLimit)).iterator
        } else {
          bmRowIdIterator =
            bitmapArray.reduceLeft(FastAggregation.or(_, _)).iterator.asScala
        }
        empty = false
      } else {
        empty = true
      }
    } finally {
      clearCache
    }
  }
}

// Below class is used to directly decode bitmap from FiberCache(either offheap/onheap memory).
private[oap] class BitmapDataInputStream(bitsStream: FiberCache) extends DataInput {

 private val bitsSize: Int = bitsStream.size.toInt
 // The current position to read from FiberCache.
 private var pos: Int = 0

 // The reading byte order is big endian.
 override def readShort(): Short = {
   val curPos = pos
   pos += 2
   (((bitsStream.getByte(curPos) & 0xFF) << 8) |
     ((bitsStream.getByte(curPos + 1) & 0xFF)) & 0xFFFF).toShort
 }

 override def readInt(): Int = {
   val curPos = pos
   pos += 4
   ((bitsStream.getByte(curPos) & 0xFF) << 24) |
     ((bitsStream.getByte(curPos + 1) & 0xFF) << 16) |
     ((bitsStream.getByte(curPos + 2) & 0xFF) << 8) |
     (bitsStream.getByte(curPos + 3) & 0xFF)
 }

 override def readLong(): Long = {
   val curPos = pos
   pos += 8
   ((bitsStream.getByte(curPos) & 0xFF).toLong << 56) |
     ((bitsStream.getByte(curPos + 1).toLong & 0xFF) << 48) |
     ((bitsStream.getByte(curPos + 2).toLong & 0xFF) << 40) |
     ((bitsStream.getByte(curPos + 3).toLong & 0xFF) << 32) |
     ((bitsStream.getByte(curPos + 4).toLong & 0xFF) << 24) |
     ((bitsStream.getByte(curPos + 5).toLong & 0xFF) << 16) |
     ((bitsStream.getByte(curPos + 6).toLong & 0xFF) << 8) |
     (bitsStream.getByte(curPos + 7).toLong & 0xFF)
 }

 override def readFully(readBuffer: Array[Byte], offset: Int, length: Int): Unit = {
   if (length < 0) {
     throw new IndexOutOfBoundsException("read length is inlegal for bitmap index.\n")
   }
   var curPos = pos
   pos += length
   if (pos > bitsSize - 1) {
     throw new EOFException("read is ending of file for bitmap index.\n")
   }
   (offset until (offset + length)).foreach(idx => {
     readBuffer(idx) = bitsStream.getByte(curPos)
     curPos += 1
   })
 }

 override def skipBytes(n: Int): Int = {
   pos += n
   n
 }

 // Below are not needed by roaring bitmap, just implement them for DataInput interface.
 override def readBoolean(): Boolean = {
   val curPos = pos
   pos += 1
   if (bitsStream.getByte(curPos).toInt != 0) true else false
 }

 override def readByte(): Byte = {
   val curPos = pos
   pos += 1
   bitsStream.getByte(curPos)
 }

 override def readUnsignedByte(): Int = {
   readByte().toInt
 }

 override def readUnsignedShort(): Int = {
   readShort().toInt
 }

 override def readChar(): Char = {
   readShort().toChar
 }

 override def readDouble(): Double = {
   readLong().toDouble
 }

 override def readFloat(): Float = {
   readInt().toFloat
 }

 override def readFully(readBuffer: Array[Byte]): Unit = {
   readFully(readBuffer, 0, readBuffer.length)
 }

 override def readLine(): String = {
   throw new UnsupportedOperationException("Bitmap doesn't need this." +
     "It's inlegal to use it in bitmap!!!")
 }

 override def readUTF(): String = {
   throw new UnsupportedOperationException("Bitmap doesn't need this." +
     "It' inlegal to use it in bitmap!!!")
 }
}
