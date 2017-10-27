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

import java.io.{ByteArrayInputStream, ObjectInputStream}
import java.nio.ByteBuffer

import scala.collection.mutable
import scala.collection.immutable
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.roaringbitmap.buffer.ImmutableRoaringBitmap
import org.roaringbitmap.buffer.MutableRoaringBitmap
import sun.nio.ch.DirectBuffer

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap._
import org.apache.spark.sql.execution.datasources.oap.filecache._
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.unsafe.Platform
import org.apache.spark.util.collection.BitSet
import org.apache.spark.util.io.ChunkedByteBuffer

private[oap] case class BitMapScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {

  override def canBeOptimizedByStatistics: Boolean = true

  @transient var internalItr: Iterator[Integer] = Iterator[Integer]()
  var empty: Boolean = _
  var internalBitSet: MutableRoaringBitmap = _
  var indexFiber: IndexFiber = _
  var indexData: CacheResult = _

  override def hasNext: Boolean = {
    if (!empty && internalItr.hasNext) {
      true
    } else {
      if (indexData != null) {
        if (indexData.cached) FiberCacheManager.releaseLock(indexFiber)
        else indexData.buffer.dispose()
      }
      false
    }
  }

  override def next(): Long = internalItr.next().toLong

  override def initialize(dataPath: Path, conf: Configuration): IndexScanner = {
    assert(keySchema ne null)
    this.ordering = GenerateOrdering.create(keySchema)
    val indexPath = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)
    /* TODO: 1. Here the index file may be read twice if it can't be cached successfully due to
     *          incorrect configurations. The first is to read fully in getOrElseUpdate. The second
     *          is following partial load. If the file size is not big enough, the partially load
     *          is not always more effifient than fully load. Will resolve it soon.
     *       2. Use microbenchmarks or benchmarks to decide to when to walk the partial load path.
     *       3. Fine cache the bitmap index file(key list, offset list, bitmap entries).
     */
    val indexFile = IndexFile(indexPath)
    indexFiber = IndexFiber(indexFile)
    indexData = FiberCacheManager.getOrElseUpdate(indexFiber, conf)
    indexData.cached match {
      case true => fullyLoadFromCache(indexData.buffer)
      case false => partiallyLoadFromFile(indexPath, conf)
    }

    this
  }

  def getBitmapIdx(keyList: immutable.List[InternalRow], range: RangeInterval): (Int, Int) = {
    val startIdx = if (range.start == IndexScanner.DUMMY_KEY_START) {
      // diff from which startIdx not found, so here startIdx = -2
      -2
    } else {
      // find first key which >= start key, can't find return -1
      if (range.startInclude) {
        keyList.indexWhere(ordering.compare(range.start, _) <= 0)
      } else {
        keyList.indexWhere(ordering.compare(range.start, _) < 0)
      }
    }
    val endIdx = if (range.end == IndexScanner.DUMMY_KEY_END) {
      keyList.size
    } else {
      // find last key which <= end key, can't find return -1
      if (range.endInclude) {
        keyList.lastIndexWhere(ordering.compare(_, range.end) <= 0)
      } else {
        keyList.lastIndexWhere(ordering.compare(_, range.end) < 0)
      }
    }
    (startIdx, endIdx)
  }

  def getDesiredBitmaps(byteArray: Array[Byte], position: Int,
    startIdx: Int, endIdx: Int): mutable.ListBuffer[ImmutableRoaringBitmap] = {
    val partialBitmapList = new mutable.ListBuffer[ImmutableRoaringBitmap]()
    val rawBb = ByteBuffer.wrap(byteArray)
    var curPosition = position
    rawBb.position(curPosition)
    (startIdx until endIdx).map( idx => {
      val bm = new ImmutableRoaringBitmap(rawBb)
      partialBitmapList.append(bm)
      curPosition += bm.serializedSizeInBytes
      rawBb.position(curPosition)
    })
    partialBitmapList
  }

  def getDesiredRowIdIterator(bitmapArray: mutable.ArrayBuffer[ImmutableRoaringBitmap]): Unit = {
    if (bitmapArray.nonEmpty) {
      if (limitScanEnabled()) {
        // Get N items from each index.
        internalItr = bitmapArray.flatMap(bm =>
          bm.iterator.asScala.take(getLimitScanNum())).iterator
      } else {
        internalBitSet = new MutableRoaringBitmap()
        bitmapArray.foreach(bm => internalBitSet.or(bm))
        internalItr = internalBitSet.iterator.asScala.toIterator
      }
      empty = false
    } else {
      empty = true
    }
  }

  def partiallyLoadFromFile(indexPath: Path, conf: Configuration): Unit = {
    val fs = indexPath.getFileSystem(conf)
    val fin = fs.open(indexPath)
    val indexFileSize = fs.getFileStatus(indexPath).getLen

    // The bitmap total size is 4 bytes at the beginning of the footer of bitmap index file.
    // Please refer to the footer description in BitmapIndexRecordWriter.
    val bmTotalSizeOffset = indexFileSize - (3 * 8 + 4)
    var bmTotalSizeBuffer = new Array[Byte](4)
    fin.read(bmTotalSizeOffset, bmTotalSizeBuffer, 0, 4)
    val bmTotalSize = Platform.getInt(bmTotalSizeBuffer, Platform.BYTE_ARRAY_OFFSET)

    var sortedKeyListSizeBuffer = new Array[Byte](4)
    val sortedKeyListSizeOffset = IndexFile.indexFileHeaderLength
    fin.read(sortedKeyListSizeOffset, sortedKeyListSizeBuffer, 0, 4)
    val sortedKeyListSize = Platform.getInt(sortedKeyListSizeBuffer, Platform.BYTE_ARRAY_OFFSET)
    // TODO: seems not supported yet on my local dev machine(hadoop is 2.7.3).
    // fin.setReadahead(sortedKeyListSize)

    var sortedKeyListBuffer = new Array[Byte](sortedKeyListSize)
    val sortedKeyListByteArrayStart = sortedKeyListSizeOffset + 4
    fin.read(sortedKeyListByteArrayStart, sortedKeyListBuffer, 0, sortedKeyListSize)
    val inputStream = new ByteArrayInputStream(sortedKeyListBuffer)
    val in = new ObjectInputStream(inputStream)
    val sortedKeyList = in.readObject().asInstanceOf[immutable.List[InternalRow]]
    val bmOffsetOffset = sortedKeyListByteArrayStart + sortedKeyListSize + bmTotalSize

    val bitmapArray = intervalArray.flatMap(range => {
      val (startIdx, endIdx) = getBitmapIdx(sortedKeyList, range)
      if (startIdx == -1 || endIdx == -1) {
        // range not fond in cur bitmap, return empty for performance consideration
        Array.empty[ImmutableRoaringBitmap]
      } else {
        var startIdxOffsetBuffer = new Array[Byte](4)
        val startIdxOffset = bmOffsetOffset + startIdx * 4
        fin.read(startIdxOffset, startIdxOffsetBuffer, 0, 4)
        val bmStartIdxOffset = Platform.getInt(startIdxOffsetBuffer, Platform.BYTE_ARRAY_OFFSET)
        val endIdxOffset = bmOffsetOffset + (endIdx + 1) * 4
        var endIdxOffsetBuffer = new Array[Byte](4)
        fin.read(endIdxOffset, endIdxOffsetBuffer, 0, 4)
        val bmEndIdxOffset = Platform.getInt(endIdxOffsetBuffer, Platform.BYTE_ARRAY_OFFSET)
        val bmLoadSize = bmEndIdxOffset - bmStartIdxOffset
        // fin.setReadahead(bmLoadSize)
        var bmSizeBuffer = new Array[Byte](bmLoadSize)
        fin.read(bmStartIdxOffset, bmSizeBuffer, 0, bmLoadSize)

        getDesiredBitmaps(bmSizeBuffer, 0, startIdx, (endIdx + 1))
      }
    })
    fin.close()

    getDesiredRowIdIterator(bitmapArray)
  }

  // This will fully load from the cache.
  def fullyLoadFromCache(indexData: ChunkedByteBuffer): Unit = {
    // Please refer to the layout details about bitmap index file in BitmapIndexRecordWriter.
    val (baseObj, sortedKeyListOffset): (Object, Long) = indexData.chunks.head match {
      case buf: DirectBuffer => (null, buf.address() + IndexFile.indexFileHeaderLength)
      case _ => (indexData.toArray, Platform.BYTE_ARRAY_OFFSET + IndexFile.indexFileHeaderLength)
    }

    // The bitmap total size is 4 bytes at the beginning of the footer of bitmap index file.
    // Please refer to the footer description in BitmapIndexRecordWriter.
    val bmTotalSizeIdx = indexData.size - (3 * 8 + 4)
    val bmTotalSizeOffset = indexData.chunks.head match {
      case buf: DirectBuffer => buf.address() + bmTotalSizeIdx
      case _ => Platform.BYTE_ARRAY_OFFSET + bmTotalSizeIdx
    }
    val bmTotalSize = Platform.getInt(baseObj, bmTotalSizeOffset)

    // Deserialize sortedKeyList[InternalRow] from index file.
    // Get the byte number first.
    val sortedKeyListObjLength = Platform.getInt(baseObj, sortedKeyListOffset)
    val sortedKeyListByteArrayStart = sortedKeyListOffset + 4
    val byteArray = (0 until sortedKeyListObjLength).map(i => {
      Platform.getByte(baseObj, sortedKeyListByteArrayStart + i)
    }).toArray
    val inputStream = new ByteArrayInputStream(byteArray)
    val in = new ObjectInputStream(inputStream)
    val sortedKeyList = in.readObject().asInstanceOf[immutable.List[InternalRow]]

    val bmOffset = sortedKeyListByteArrayStart + sortedKeyListObjLength
    val bmOffsetOffset = bmOffset + bmTotalSize

    val bitmapArray = intervalArray.flatMap(range => {
      val (startIdx, endIdx) = getBitmapIdx(sortedKeyList, range)
      if (startIdx == -1 || endIdx == -1) {
        // range not fond in cur bitmap, return empty for performance consideration
        Array.empty[ImmutableRoaringBitmap]
      } else {
        val rawByteArray = indexData.toArray
        val bmStartIdxOffset = Platform.getInt(baseObj, bmOffsetOffset + startIdx * 4)
        getDesiredBitmaps(rawByteArray, bmStartIdxOffset, startIdx, (endIdx + 1))
      }
    })

    getDesiredRowIdIterator(bitmapArray)
  }

  override def toString: String = "BitMapScanner"
}
