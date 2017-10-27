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

import java.io.{ByteArrayOutputStream, DataOutputStream, ObjectOutputStream, OutputStream}

import scala.collection.mutable
import scala.collection.JavaConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.{RecordWriter, TaskAttemptContext}
import org.roaringbitmap.buffer.MutableRoaringBitmap

import org.apache.spark.sql.catalyst.expressions.codegen.GenerateOrdering
import org.apache.spark.sql.catalyst.expressions.FromUnsafeProjection
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.StatisticsManager
import org.apache.spark.sql.types.StructType

private[index] class BitmapIndexRecordWriter(
    configuration: Configuration,
    writer: OutputStream,
    keySchema: StructType) extends RecordWriter[Void, InternalRow] {

  @transient private lazy val genericProjector = FromUnsafeProjection(keySchema)

  private val rowMapBitmap = new mutable.HashMap[InternalRow, MutableRoaringBitmap]()
  private var recordCount: Int = 0

  override def write(key: Void, value: InternalRow): Unit = {
    val v = genericProjector(value).copy()
    if (!rowMapBitmap.contains(v)) {
      val bm = new MutableRoaringBitmap()
      bm.add(recordCount)
      rowMapBitmap.put(v, bm)
    } else {
      rowMapBitmap.get(v).get.add(recordCount)
    }
    recordCount += 1
  }

  override def close(context: TaskAttemptContext): Unit = {
    flushToFile(writer)
    writer.close()
  }

  private def flushToFile(out: OutputStream): Unit = {

    val statisticsManager = new StatisticsManager
    statisticsManager.initialize(BitMapIndexType, keySchema, configuration)

    /* The general layout for bitmap index file is below.
     * header (8 bytes)
     * sorted key list size (4 bytes)
     * sorted key list (sorted bitmap index column)
     * bitmap entries (each entry size is varied due to different BitSet implementations)
     * offset array for the above each bitmap entry (each offset element is fixed 4 bytes)
     * total bitmap size (4 bytes)
     * bitmap index footer (not changed than before)
     * TODO: 1. Use BitSet scala version to replace roaring bitmap.
     *       2. Optimize roaring bitmap usage to further reduce index file size.
     *       3. Explore an approach to partial load key set during bitmap scanning.
     */
    val header = writeHead(writer, IndexFile.INDEX_VERSION)

    val ordering = GenerateOrdering.create(keySchema)
    val sortedKeyList = rowMapBitmap.keySet.toList.sorted(ordering)
    // Serialize sortedKeyList and get length
    val writeSortedKeyListBuf = new ByteArrayOutputStream()
    val sortedKeyListOut = new ObjectOutputStream(writeSortedKeyListBuf)
    sortedKeyListOut.writeObject(sortedKeyList)
    sortedKeyListOut.flush()
    val sortedKeyListObjLen = writeSortedKeyListBuf.size()

    // Write sortedKeyList byteArray length and byteArray
    IndexUtils.writeInt(writer, sortedKeyListObjLen)
    writer.write(writeSortedKeyListBuf.toByteArray)
    sortedKeyListOut.close()

    val bmOffset = header + 4 + sortedKeyListObjLen
    val bmOffsetListBuffer = new mutable.ListBuffer[Int]()
    // Get the total bm size, and write each bitmap entries one by one.
    var totalBitmapSize = 0
    sortedKeyList.foreach(sortedKey => {
      bmOffsetListBuffer.append(bmOffset + totalBitmapSize)
      val bm = rowMapBitmap.get(sortedKey).get
      bm.runOptimize()
      val bmWriteBitMapBuf = new ByteArrayOutputStream()
      val bmBitMapOut = new DataOutputStream(bmWriteBitMapBuf)
      bm.serialize(bmBitMapOut)
      bmBitMapOut.flush()
      totalBitmapSize += bmWriteBitMapBuf.size
      writer.write(bmWriteBitMapBuf.toByteArray)
      bmBitMapOut.close()
    })
    bmOffsetListBuffer.append(bmOffset + totalBitmapSize)

    // write offset for each bitmap entry to fast partially load bitmap entries during scanning.
    bmOffsetListBuffer.foreach(offsetIdx =>
      IndexUtils.writeInt(writer, offsetIdx))
    val bmOffsetTotalSize = 4 * bmOffsetListBuffer.size

    // The index end is also the starting position of stats file.
    val indexEnd = bmOffset + totalBitmapSize + bmOffsetTotalSize

    statisticsManager.write(writer)

    // The bitmap total size is four bytes at the beginning of the footer.
    IndexUtils.writeInt(writer, totalBitmapSize)
    IndexUtils.writeLong(writer, indexEnd)
    IndexUtils.writeLong(writer, indexEnd)
    IndexUtils.writeLong(writer, indexEnd)
  }

  private def writeHead(writer: OutputStream, version: Int): Int = {
    writer.write("OAPIDX".getBytes("UTF-8"))
    assert(version <= 65535)
    val data = Array((version >> 8).toByte, (version & 0xFF).toByte)
    writer.write(data)
    IndexFile.indexFileHeaderLength
  }
}
