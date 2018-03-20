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

package org.apache.spark.sql.execution.datasources.oap.utils

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.datasources.oap.filecache.{BitmapFiber, MemoryManager, FiberCache, FiberCacheManager, WrappedFiberCache}
import org.apache.spark.sql.execution.datasources.oap.index.{BitmapIndexSectionId, IndexUtils}
import org.apache.spark.sql.execution.datasources.oap.io.IndexFile
import org.apache.spark.sql.execution.datasources.oap.OapFileFormat
import org.apache.spark.sql.test.oap.SharedOapContext
import org.apache.spark.util.Utils

// Below data are used to test the functionality of directly getting
// row IDs from fiber caches without roarting bitmap involved.
class BitmapUtilsSuite extends QueryTest with SharedOapContext with BeforeAndAfterEach {

  import testImplicits._
  private val BITMAP_FOOTER_SIZE = 6 * 8
  private var dir: File = _
  private val dataForRunChunk: Seq[(Int, String)] =
    (1 to 20000).map{i => (i / 100, s"this is test $i")}
  private val dataForArrayChunk: Seq[(Int, String)] =
    (1 to 20000).map{i => (i, s"this is test $i")}
  private val dataForBitmapChunk: Seq[(Int, String)] =
    (1 to 20000).map{i => (i % 2, s"this is test $i")}
  private val dataCombination =
    dataForBitmapChunk ++ dataForArrayChunk ++ dataForRunChunk
  private val dataArray =
    Array(dataForRunChunk, dataForArrayChunk, dataForBitmapChunk, dataCombination)

  override def beforeEach(): Unit = {
    dir = Utils.createTempDir()
    val path = dir.getAbsolutePath
    sql(s"""CREATE TEMPORARY VIEW oap_test (a INT, b STRING)
            | USING oap
            | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test")
    dir.delete()
  }

  // Below are just borrowed from BitMapScanner.scala in order to easily load bitmap index files.
  private def loadBmFooter(
      fin: FSDataInputStream,
      bmFooterOffset: Int): FiberCache =
    MemoryManager.putToIndexFiberCache(fin, bmFooterOffset, BITMAP_FOOTER_SIZE)

  private def loadBmEntry(
      fin: FSDataInputStream,
      offset: Int,
      size: Int): FiberCache =
    MemoryManager.putToIndexFiberCache(fin, offset, size)

  private def loadBmOffsetList(
      fin: FSDataInputStream,
      bmOffsetListOffset: Int,
      bmOffsetListTotalSize: Int): FiberCache =
    MemoryManager.putToIndexFiberCache(fin, bmOffsetListOffset, bmOffsetListTotalSize)

  private def getIdxOffset(
      fiberCache: FiberCache,
      baseOffset: Long,
      idx: Int): Int = {
    val idxOffset = baseOffset + idx * 4
    fiberCache.getInt(idxOffset)
  }

  private def getMetaDataAndFiberCaches(
      fin: FSDataInputStream,
      idxPath: Path,
      conf: Configuration): (Int, WrappedFiberCache, WrappedFiberCache) = {
    val idxFileSize = idxPath.getFileSystem(conf).getFileStatus(idxPath).getLen
    val bmFooterOffset = idxFileSize.toInt - BITMAP_FOOTER_SIZE
    val bmFooterFiber = BitmapFiber(
      () => loadBmFooter(fin, bmFooterOffset),
      idxPath.toString, BitmapIndexSectionId.footerSection, 0)
    val bmFooterCache = WrappedFiberCache(FiberCacheManager.get(bmFooterFiber, conf))
    val bmUniqueKeyListTotalSize = bmFooterCache.fc.getInt(IndexUtils.INT_SIZE)
    val keyCount = bmFooterCache.fc.getInt(IndexUtils.INT_SIZE * 2)
    val bmEntryListTotalSize = bmFooterCache.fc.getInt(IndexUtils.INT_SIZE * 3)
    val bmOffsetListTotalSize = bmFooterCache.fc.getInt(IndexUtils.INT_SIZE * 4)
    val bmNullEntrySize = bmFooterCache.fc.getInt(IndexUtils.INT_SIZE * 6)
    val bmUniqueKeyListOffset = IndexFile.VERSION_LENGTH
    val bmEntryListOffset = bmUniqueKeyListOffset + bmUniqueKeyListTotalSize
    val bmOffsetListOffset = bmEntryListOffset + bmEntryListTotalSize + bmNullEntrySize
    val bmOffsetListFiber = BitmapFiber(
      () => loadBmOffsetList(fin, bmOffsetListOffset, bmOffsetListTotalSize),
      idxPath.toString, BitmapIndexSectionId.entryOffsetsSection, 0)
    val bmOffsetListCache = WrappedFiberCache(FiberCacheManager.get(bmOffsetListFiber, conf))
    (keyCount, bmOffsetListCache, bmFooterCache)
  }

  test("test the functionality of directly traversing single fiber cache without roaring bitmap") {
    dataArray.foreach(dataIdx => {
      dataIdx.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table oap_test select * from t")
      sql("create oindex index_bm on oap_test (a) USING BITMAP")
      dir.listFiles.foreach(fileName => {
        if (fileName.toString.endsWith(OapFileFormat.OAP_INDEX_EXTENSION)) {
          val idxPath = new Path(fileName.toString)
          val conf = new Configuration()
          val fin = idxPath.getFileSystem(conf).open(idxPath)
          val (keyCount, bmOffsetListCache, bmFooterCache) =
            getMetaDataAndFiberCaches(fin, idxPath, conf)
          var maxRowIdInPartition = 0
          (0 until keyCount).map( idx => {
            val curIdxOffset = getIdxOffset(bmOffsetListCache.fc, 0L, idx)
            val entrySize = getIdxOffset(bmOffsetListCache.fc, 0L, idx + 1) - curIdxOffset
            val entryFiber = BitmapFiber(
              () => loadBmEntry(fin, curIdxOffset, entrySize), idxPath.toString,
            BitmapIndexSectionId.entryListSection, idx)
            val wfc = new OapBitmapWrappedFiberCache(FiberCacheManager.get(entryFiber, conf))
            wfc.init
            val rowIdIterator = ChunksInSingleFiberCacheIterator(wfc).init
            // The row id is starting from 0.
            val rowIdSeq = Seq(0 to rowIdIterator.max)
            rowIdIterator.foreach(rowId => assert(rowIdSeq.contains(rowId) == true))
            wfc.release
          })
          bmFooterCache.release
          bmOffsetListCache.release
          fin.close
        }
      })
      sql("drop oindex index_bm on oap_test")
    })
  }

  test("test the functionality of directly bitwise OR case for multi fiber caches") {
    dataArray.foreach(dataIdx => {
      dataIdx.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table oap_test select * from t")
      sql("create oindex index_bm on oap_test (a) USING BITMAP")
      dir.listFiles.foreach(fileName => {
        if (fileName.toString.endsWith(OapFileFormat.OAP_INDEX_EXTENSION)) {
          val idxPath = new Path(fileName.toString)
          val conf = new Configuration()
          val fin = idxPath.getFileSystem(conf).open(idxPath)
          val (keyCount, bmOffsetListCache, bmFooterCache) =
            getMetaDataAndFiberCaches(fin, idxPath, conf)
          val wfcSeq = (0 until keyCount).map(idx => {
            val curIdxOffset = getIdxOffset(bmOffsetListCache.fc, 0L, idx)
            val entrySize = getIdxOffset(bmOffsetListCache.fc, 0L, idx + 1) - curIdxOffset
            val entryFiber = BitmapFiber(
              () => loadBmEntry(fin, curIdxOffset, entrySize), idxPath.toString,
              BitmapIndexSectionId.entryListSection, idx)
            new OapBitmapWrappedFiberCache(FiberCacheManager.get(entryFiber, conf))
          })
          val chunkList = OapBitmapFastAggregation.or(wfcSeq)
          var rowIdIterator = ChunksInMultiFiberCachesIterator(chunkList).init
          // The row id is starting from 0.
          val rowIdSeq = Seq(0 to rowIdIterator.max)
          rowIdIterator.foreach(rowId => assert(rowIdSeq.contains(rowId) == true))
          wfcSeq.foreach(wfc => wfc.release)
          bmFooterCache.release
          bmOffsetListCache.release
          fin.close
        }
      })
      sql("drop oindex index_bm on oap_test")
    })
  }
}
