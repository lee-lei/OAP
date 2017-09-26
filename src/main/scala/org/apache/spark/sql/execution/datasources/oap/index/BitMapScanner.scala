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

import scala.collection.mutable
import scala.collection.immutable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
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

  @transient var internalItr: Iterator[Int] = Iterator[Int]()
  var empty: Boolean = _
  var internalBitSet: BitSet = _
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
    val path = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)
    val indexFile = IndexFile(path)
    indexFiber = IndexFiber(indexFile)
    indexData = FiberCacheManager.getOrElseUpdate(indexFiber, conf)
    open(indexData.buffer, indexFile.version(conf))

    this
  }

  def open(indexData: ChunkedByteBuffer, version: Int = IndexFile.INDEX_VERSION): Unit = {
    this.ordering = GenerateOrdering.create(keySchema)
    // deserialize sortedKeyList[InternalRow] from index file
    val (sortedKeyListObj, sortedKeyListOffset): (Object, Long) = indexData.chunks.head match {
      case buf: DirectBuffer => (null, buf.address() + IndexFile.indexFileHeaderLength)
      case _ => (indexData.toArray, Platform.BYTE_ARRAY_OFFSET + IndexFile.indexFileHeaderLength)
    }
    // get the byte number first
    val sortedKeyListObjLength = Platform.getInt(sortedKeyListObj, sortedKeyListOffset)
    val byteArrayStart = sortedKeyListOffset + 4
    val byteArray = (0 until sortedKeyListObjLength).map(i => {
      Platform.getByte(sortedKeyListObj, byteArrayStart + i)
    }).toArray
    val inputStream = new ByteArrayInputStream(byteArray)
    val in = new ObjectInputStream(inputStream)
    val sortedKeyList = in.readObject().asInstanceOf[immutable.List[InternalRow]]

    // deserialize listBitMap from index file
    val (bitmapObj, bitmapOffset): (Object, Long) = indexData.chunks.head match {
      case buf: DirectBuffer => (null, buf.address() + IndexFile.indexFileHeaderLength +
        4 + sortedKeyListObjLength)
      case _ => (indexData.toArray, Platform.BYTE_ARRAY_OFFSET + IndexFile.indexFileHeaderLength +
        4 + sortedKeyListObjLength)
    }
    val bitmapObjLength = Platform.getInt(bitmapObj, bitmapOffset)
    val bitmapByteArrayStart = bitmapOffset + 4
    val bitmapByteArray = (0 until bitmapObjLength).map(i => {
      Platform.getByte(bitmapObj, bitmapByteArrayStart + i)
    }).toArray
    val bitmapInputStream = new ByteArrayInputStream(bitmapByteArray)
    val bitmapIn = new ObjectInputStream(bitmapInputStream)
    val listBitMap = bitmapIn.readObject().asInstanceOf[mutable.ListBuffer[BitSet]]

    val bitMapArray = intervalArray.flatMap(range => {
      val startIdx = if (range.start == IndexScanner.DUMMY_KEY_START) {
        // diff from which startIdx not found, so here startIdx = -2
        -2
      } else {
        // find first key which >= start key, can't find return -1
        if (range.startInclude) {
          sortedKeyList.indexWhere(ordering.compare(range.start, _) <= 0)
        } else {
          sortedKeyList.indexWhere(ordering.compare(range.start, _) < 0)
        }
      }
      val endIdx = if (range.end == IndexScanner.DUMMY_KEY_END) {
        sortedKeyList.size
      } else {
        // find last key which <= end key, can't find return -1
        if (range.endInclude) {
          sortedKeyList.lastIndexWhere(ordering.compare(_, range.end) <= 0)
        } else {
          sortedKeyList.lastIndexWhere(ordering.compare(_, range.end) < 0)
        }
      }

      if (startIdx == -1 || endIdx == -1) {
        // range not fond in cur bitmap, return empty for performance consideration
        Array.empty[BitSet]
      } else {
        listBitMap.slice(startIdx, endIdx + 1)
      }
    })

    if (bitMapArray.nonEmpty) {
      if (limitScanEnabled()) {
        // Get N items from each index.
        internalItr = bitMapArray.flatMap(bitSet =>
          bitSet.iterator.take(getLimitScanNum())).iterator
      } else {
        internalBitSet = bitMapArray.reduceLeft(_ | _)
        internalItr = internalBitSet.iterator
      }
      empty = false
    } else {
      empty = true
    }
  }

  override def toString: String = "BitMapScanner"
}
