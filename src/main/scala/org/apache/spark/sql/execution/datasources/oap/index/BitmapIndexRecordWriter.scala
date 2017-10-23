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

  private val rowMapRb = new mutable.HashMap[InternalRow, MutableRoaringBitmap]()
  private var recordCount: Int = 0

  override def write(key: Void, value: InternalRow): Unit = {
    val v = genericProjector(value).copy()
    if (!rowMapRb.contains(v)) {
      val rb = new MutableRoaringBitmap()
      rb.add(recordCount)
      rowMapRb.put(v, rb)
    } else {
      rowMapRb.get(v).get.add(recordCount)
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

    /* The general layout for bitmap index file is below:
     * header (8 bytes)
     * sorted key list size (4 bytes)
     * sorted key list
     * total bitmap size (4 bytes)
     * roaring bitmaps (each entry size is varied due to run length encoding and compression)
     * offset array for the above each bitmap entry (each element is fixed 4 bytes)
     * TODO: 1. Save the total bitmap size into somewhere (e.g. StatisticsManager) to avoid
     *          go through bitmap entries twice to improve bitmap index writing efficiency.
     *       2. Optimize roaring bitmap usage to further reduce index file size.
     *       3. Explore an approach to partial load key set during bitmap scanning.
     */
    val ordering = GenerateOrdering.create(keySchema)
    val sortedKeyList = rowMapRb.keySet.toList.sorted(ordering)
    val header = writeHead(writer, IndexFile.INDEX_VERSION)
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
    // The second 4 is for the reserved 4 bytes for total rb size.
    val rbOffset = header + 4 + sortedKeyListObjLen + 4
    val rbOffsetListBuffer = new mutable.ListBuffer[Int]()
    // Get the total rb size.
    var totalRbSize = 0
    sortedKeyList.foreach(sortedKey => {
      rbOffsetListBuffer.append(rbOffset + totalRbSize)
      val rb = rowMapRb.get(sortedKey).get
      rb.runOptimize()
      val rbWriteBitMapBuf = new ByteArrayOutputStream()
      val rbBitMapOut = new DataOutputStream(rbWriteBitMapBuf)
      rb.serialize(rbBitMapOut)
      rbBitMapOut.flush()
      totalRbSize += rbWriteBitMapBuf.size
      rbBitMapOut.close()
    })
    rbOffsetListBuffer.append(rbOffset + totalRbSize)
    IndexUtils.writeInt(writer, totalRbSize)
    sortedKeyList.foreach(sortedKey => {
      val rb = rowMapRb.get(sortedKey).get
      rb.runOptimize()
      val rbWriteBitMapBuf = new ByteArrayOutputStream()
      val rbBitMapOut = new DataOutputStream(rbWriteBitMapBuf)
      rb.serialize(rbBitMapOut)
      rbBitMapOut.flush()
      writer.write(rbWriteBitMapBuf.toByteArray)
      rbBitMapOut.close()
    })
    // Save the offset for each rb entry to fast partially load bitmap entries during scanning.
    rbOffsetListBuffer.foreach(offsetIdx =>
      IndexUtils.writeInt(writer, offsetIdx))
    val rbOffsetTotalSize = 4 * rbOffsetListBuffer.size
    val indexEnd = rbOffset + totalRbSize + rbOffsetTotalSize
    var offset: Long = indexEnd

    statisticsManager.write(writer)

    // write index file footer
    IndexUtils.writeLong(writer, indexEnd) // statistics start pos
    IndexUtils.writeLong(writer, offset) // index file end offset
    IndexUtils.writeLong(writer, indexEnd) // dataEnd
  }

  private def writeHead(writer: OutputStream, version: Int): Int = {
    writer.write("OAPIDX".getBytes("UTF-8"))
    assert(version <= 65535)
    val data = Array((version >> 8).toByte, (version & 0xFF).toByte)
    writer.write(data)
    IndexFile.indexFileHeaderLength
  }
}
