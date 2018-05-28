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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}

import org.apache.spark.sql.execution.datasources.OapException
import org.apache.spark.sql.execution.datasources.oap.IndexMeta
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.statistics.StatsAnalysisResult

private[oap] case class BitMapScanner(idxMeta: IndexMeta) extends IndexScanner(idxMeta) {

  override def canBeOptimizedByStatistics: Boolean = true

  private var _totalRows: Long = 0
  @transient private var bmRowIdIterator: Iterator[Int] = _
  override def hasNext: Boolean = bmRowIdIterator.hasNext
  override def next(): Int = bmRowIdIterator.next
  override def totalRows(): Long = _totalRows

  // TODO: If the index file is not changed, bypass the repetitive initialization for queries.
  override def initialize(dataPath: Path, conf: Configuration): IndexScanner = {
    assert(keySchema ne null)
    // Currently OAP index type supports the column with one single field.
    assert(keySchema.fields.length == 1)
    val idxPath = IndexUtils.indexFileFromDataFile(dataPath, meta.name, meta.time)

    val fin = idxPath.getFileSystem(conf).open(idxPath)
    val indexVersion = readHead(fin, 0)
    val reader =
      indexVersion match {
        case 1 =>
          val reader = new BitmapReaderV1(
            fin, intervalArray, internalLimit, keySchema, idxPath, conf)
          reader.getRowIdIterator
          bmRowIdIterator = reader
          reader
        case 2 =>
          val reader = new BitmapReaderV2(
            fin, intervalArray, internalLimit, keySchema, idxPath, conf)
          reader.getRowIdIterator
          bmRowIdIterator = reader
          reader
        case _ =>
          throw new OapException("not supported bitmap index version. " + indexVersion)
      }
    _totalRows = reader.totalRows
    reader.close(fin)
    this
  }

  override protected def analyzeStatistics(
      idxPath: Path,
      conf: Configuration): StatsAnalysisResult = {
    val reader = BitmapReader(intervalArray, keySchema, idxPath, conf)
    reader.analyzeStatistics
  }

  override def toString: String = "BitMapScanner"

  private def readHead(fin: FSDataInputStream, offset: Int): Int = {
    val magicBytes = new Array[Byte](IndexFile.VERSION_LENGTH)
    fin.readFully(offset, magicBytes)
    IndexUtils.deserializeVersion(magicBytes).get
  }
}
