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

import java.io.{ByteArrayOutputStream, DataOutputStream, File, FileOutputStream}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FSDataInputStream, Path}
import org.roaringbitmap.RoaringBitmap
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

  private val seqForRunChunk = (1 to 9).toSeq
  private val seqForArrayChunk = Seq(11, 13, 15, 17, 19)
  private val seqForBitmapChunk = (20 to 10000).filter(_ % 2 == 1)
  private val seqCombination =
    seqForBitmapChunk ++ seqForArrayChunk ++ seqForRunChunk

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

  test("test ChunksInSingleFiberCacheIterator to get the expected row IDs") {
    val seqArray =
      Array(seqForRunChunk, seqForArrayChunk, seqForBitmapChunk, seqCombination)
    seqArray.foreach(seqIdx => {
      val subDir = Utils.createTempDir()
      val rb = new RoaringBitmap()
      seqIdx.foreach(rb.add)
      val rbFile = subDir.getAbsolutePath + "rb.bin"
      rb.runOptimize()
      val rbFos = new FileOutputStream(rbFile)
      val rbBos = new ByteArrayOutputStream()
      val rbDos = new DataOutputStream(rbBos)
      rb.serialize(rbDos)
      rbBos.writeTo(rbFos)
      rbBos.close()
      rbDos.close()
      rbFos.close()
      val rbPath = new Path(rbFile)
      val conf = new Configuration()
      val fin = rbPath.getFileSystem(conf).open(rbPath)
      val fileSize = rbPath.getFileSystem(conf).getFileStatus(rbPath).getLen
      val rbFiber = BitmapFiber(() => loadBmSection(fin, 0, fileSize.toInt), rbPath.toString, 0, 0)
      val wrappedFiberCache = new OapBitmapWrappedFiberCache(FiberCacheManager.get(rbFiber, conf))
      wrappedFiberCache.init
      var actualRowIdSeq = Seq.empty[Int]
      ChunksInSingleFiberCacheIterator(wrappedFiberCache).init.foreach(rowId =>
        actualRowIdSeq :+= rowId)
      actualRowIdSeq.foreach(rowId => assert(seqIdx.contains(rowId) == true))
      seqIdx.foreach(rowId => assert(actualRowIdSeq.contains(rowId) == true))
      wrappedFiberCache.release
      fin.close
      subDir.delete
    })
  }

  test("test ChunksInMultiFiberCachesIterator to get the expected row IDs") {
    val seqArray =
      Array(seqForRunChunk, seqForArrayChunk, seqForBitmapChunk)
    val wrapperFiberCacheSeq = seqArray.map(seqIdx => {
      val rb = new RoaringBitmap()
      seqIdx.foreach(rb.add)
      val rbFile = dir.getAbsolutePath + seqIdx.head.toString
      rb.runOptimize()
      val rbFos = new FileOutputStream(rbFile)
      val rbBos = new ByteArrayOutputStream()
      val rbDos = new DataOutputStream(rbBos)
      rb.serialize(rbDos)
      rbBos.writeTo(rbFos)
      rbBos.close()
      rbDos.close()
      rbFos.close()
      val rbPath = new Path(rbFile)
      val conf = new Configuration()
      val fin = rbPath.getFileSystem(conf).open(rbPath)
      val rbFileSize = rbPath.getFileSystem(conf).getFileStatus(rbPath).getLen
      val rbFiber = BitmapFiber(() => loadBmSection(fin, 0, rbFileSize.toInt), rbPath.toString, 0, 0)
      fin.close
      new OapBitmapWrappedFiberCache(FiberCacheManager.get(rbFiber, conf))
    })
    var actualRowIdSeq = Seq.empty[Int]
    val chunkList = BitmapUtils.or(wrapperFiberCacheSeq)
    ChunksInMultiFiberCachesIterator(chunkList).init.foreach(rowId => actualRowIdSeq :+= rowId)
    actualRowIdSeq.foreach(rowId => assert(seqCombination.contains(rowId) == true))
    seqCombination.foreach(rowId => assert(actualRowIdSeq.contains(rowId) == true))
    wrapperFiberCacheSeq.foreach(wfc => wfc.release)
  }

  private def loadBmSection(fin: FSDataInputStream, offset: Int, size: Int): FiberCache =
    MemoryManager.putToIndexFiberCache(fin, offset, size)

  private def getIdxOffset(fiberCache: FiberCache, baseOffset: Long, idx: Int): Int = {
    val idxOffset = baseOffset + idx * 4
    fiberCache.getInt(idxOffset)
  }

  private def getMetaDataAndFiberCaches(
      fin: FSDataInputStream,
      idxPath: Path,
      conf: Configuration): (Int, WrappedFiberCache, WrappedFiberCache) = {
    val idxFileSize = idxPath.getFileSystem(conf).getFileStatus(idxPath).getLen
    val footerOffset = idxFileSize.toInt - BITMAP_FOOTER_SIZE
    val footerFiber = BitmapFiber(
      () => loadBmSection(fin, footerOffset, BITMAP_FOOTER_SIZE),
      idxPath.toString, BitmapIndexSectionId.footerSection, 0)
    val footerCache = WrappedFiberCache(FiberCacheManager.get(footerFiber, conf))
    val uniqueKeyListTotalSize = footerCache.fc.getInt(IndexUtils.INT_SIZE)
    val keyCount = footerCache.fc.getInt(IndexUtils.INT_SIZE * 2)
    val entryListTotalSize = footerCache.fc.getInt(IndexUtils.INT_SIZE * 3)
    val offsetListTotalSize = footerCache.fc.getInt(IndexUtils.INT_SIZE * 4)
    val nullEntrySize = footerCache.fc.getInt(IndexUtils.INT_SIZE * 6)
    val entryListOffset = IndexFile.VERSION_LENGTH + uniqueKeyListTotalSize
    val offsetListOffset = entryListOffset + entryListTotalSize + nullEntrySize
    val offsetListFiber = BitmapFiber(
      () => loadBmSection(fin, offsetListOffset, offsetListTotalSize),
      idxPath.toString, BitmapIndexSectionId.entryOffsetsSection, 0)
    val offsetListCache = WrappedFiberCache(FiberCacheManager.get(offsetListFiber, conf))
    (keyCount, offsetListCache, footerCache)
  }

  test("test how to directly get the row ID list from single fiber cache without roaring bitmap") {
    dataArray.foreach(dataIdx => {
      dataIdx.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table oap_test select * from t")
      sql("create oindex index_bm on oap_test (a) USING BITMAP")
      // For dataCombination, the total rows is 3 * 20000 = 60000.
      val expectedRowIdSeq = if (dataIdx != dataCombination) (0 until 20000).toSeq
        else (0 until 60000).toSeq
      var actualRowIdSeq = Seq.empty[Int]
      var accumulatorRowId = 0
      dir.listFiles.foreach(fileName => {
        if (fileName.toString.endsWith(OapFileFormat.OAP_INDEX_EXTENSION)) {
          var maxRowIdInPartition = 0
          val idxPath = new Path(fileName.toString)
          val conf = new Configuration()
          val fin = idxPath.getFileSystem(conf).open(idxPath)
          val (keyCount, offsetListCache, footerCache) =
            getMetaDataAndFiberCaches(fin, idxPath, conf)
          (0 until keyCount).map(idx => {
            val curIdxOffset = getIdxOffset(offsetListCache.fc, 0L, idx)
            val entrySize = getIdxOffset(offsetListCache.fc, 0L, idx + 1) - curIdxOffset
            val entryFiber = BitmapFiber(
              () => loadBmSection(fin, curIdxOffset, entrySize), idxPath.toString,
            BitmapIndexSectionId.entryListSection, idx)
            val wrappedFiberCache =
              new OapBitmapWrappedFiberCache(FiberCacheManager.get(entryFiber, conf))
            wrappedFiberCache.init
            ChunksInSingleFiberCacheIterator(wrappedFiberCache).init.foreach(rowId => {
              val realRowId = rowId + accumulatorRowId
              actualRowIdSeq :+= realRowId
              if (maxRowIdInPartition < rowId) maxRowIdInPartition = rowId
            })
            wrappedFiberCache.release
          })
          // The row Id is starting from 0.
          accumulatorRowId += maxRowIdInPartition + 1
          footerCache.release
          offsetListCache.release
          fin.close
        }
      })
      // It's not appropriate to use Set to just match if they are equal, because it can't avoid
      // the possibility that the actual row ID seq may contain some repeated same elements.
      actualRowIdSeq.foreach(rowId => assert(expectedRowIdSeq.contains(rowId) == true))
      expectedRowIdSeq.foreach(rowId => assert(actualRowIdSeq.contains(rowId) == true))
      sql("drop oindex index_bm on oap_test")
    })
  }

  test("test how to directly get the row Id list after bitwise OR among multi fiber caches") {
    dataArray.foreach(dataIdx => {
      dataIdx.toDF("key", "value").createOrReplaceTempView("t")
      sql("insert overwrite table oap_test select * from t")
      sql("create oindex index_bm on oap_test (a) USING BITMAP")
      // For dataCombination, the total rows is 3 * 20000 = 60000.
      val expectedRowIdSeq = if (dataIdx != dataCombination) (0 until 20000).toSeq
        else (0 until 60000).toSeq
      var actualRowIdSeq = Seq.empty[Int]
      var accumulatorRowId = 0
      dir.listFiles.foreach(fileName => {
        if (fileName.toString.endsWith(OapFileFormat.OAP_INDEX_EXTENSION)) {
          var maxRowIdInPartition = 0
          val idxPath = new Path(fileName.toString)
          val conf = new Configuration()
          val fin = idxPath.getFileSystem(conf).open(idxPath)
          val (keyCount, offsetListCache, footerCache) =
            getMetaDataAndFiberCaches(fin, idxPath, conf)
          val wrappedFiberCacheSeq = (0 until keyCount).map(idx => {
            val curIdxOffset = getIdxOffset(offsetListCache.fc, 0L, idx)
            val entrySize = getIdxOffset(offsetListCache.fc, 0L, idx + 1) - curIdxOffset
            val entryFiber = BitmapFiber(
              () => loadBmSection(fin, curIdxOffset, entrySize), idxPath.toString,
              BitmapIndexSectionId.entryListSection, idx)
            new OapBitmapWrappedFiberCache(FiberCacheManager.get(entryFiber, conf))
          })
          val chunkList = BitmapUtils.or(wrappedFiberCacheSeq)
          ChunksInMultiFiberCachesIterator(chunkList).init.foreach(rowId => {
              val realRowId = rowId + accumulatorRowId
              actualRowIdSeq :+= realRowId
              if (maxRowIdInPartition < rowId) maxRowIdInPartition = rowId
          })
          accumulatorRowId += maxRowIdInPartition + 1
          wrappedFiberCacheSeq.foreach(wfc => wfc.release)
          footerCache.release
          offsetListCache.release
          fin.close
        }
      })
      actualRowIdSeq.foreach(rowId => assert(expectedRowIdSeq.contains(rowId) == true))
      expectedRowIdSeq.foreach(rowId => assert(actualRowIdSeq.contains(rowId) == true))
      sql("drop oindex index_bm on oap_test")
    })
  }
}
