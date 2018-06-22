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

import java.io.File

import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.execution.datasources.oap.{DataSourceMeta, OapFileFormat}
import org.apache.spark.sql.execution.datasources.oap.statistics.StatsAnalysisResult
import org.apache.spark.sql.internal.oap.OapConf
import org.apache.spark.sql.sources._
import org.apache.spark.sql.test.TestSparkSession
import org.apache.spark.sql.test.oap.SharedOapContextBase
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.util.Utils

/*
 * By default, the spark and oap tests are using the same debug file system which
 * will re-open the file for each file read. Therefore the file read will not be
 * impacted even if the input stream is closed alrady.
 * This test is to mimic the same file system with the customer's QA environment.
 * Thus it will throw the exception if the input stream is closed before the file read.
 */

private[oap] class TestOapSessionWithDifferentFileSystem(sc: SparkContext)
    extends TestSparkSession(sc) { self =>

  def this(sparkConf: SparkConf) {
    this(new SparkContext(
      "local[2]",
      "test-oap-context",
      sparkConf.set("spark.sql.testkey", "true")
        .set("spark.hadoop.fs.file.impl", classOf[RawLocalFileSystem].getName)))
  }
}

trait SharedOapContextWithDifferentFileSystem extends SharedOapContextBase {
  protected override def createSparkSession: TestSparkSession = {
    new TestOapSessionWithDifferentFileSystem(sparkConf)
  }
}

class BitmapAnalyzeStatisticsSuite extends QueryTest with SharedOapContextWithDifferentFileSystem
    with BeforeAndAfterEach {
  import testImplicits._

  private var tempDir: File = _
  private var path: String = _

  override def beforeEach(): Unit = {
    spark.conf.set(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key, "true")
    tempDir = Utils.createTempDir()
    path = tempDir.getAbsolutePath
    sql(s"""CREATE TEMPORARY VIEW oap_test (a INT, b STRING)
            | USING oap
            | OPTIONS (path '$path')""".stripMargin)
  }

  override def afterEach(): Unit = {
    sqlContext.dropTempTable("oap_test")
    spark.conf.set(OapConf.OAP_ENABLE_EXECUTOR_INDEX_SELECTION.key, "false")
  }

  test("Bitmap Scanner analyzes the statistics.") {
    val data: Seq[(Int, String)] = (0 to 200).map {i => (i, s"this is test $i")}
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")
    sql("create oindex idxa on oap_test(a) USING BITMAP")
    val oapMetaPath = new Path(
      new File(path, OapFileFormat.OAP_META_FILE).getAbsolutePath)
    val oapMeta = DataSourceMeta.initialize(oapMetaPath, configuration)
    val ic = new IndexContext(oapMeta)
    val equalFilters: Array[Filter] = Array(Or(EqualTo("a", 14),
      EqualTo("b", "this is test 166")))
    val schema = StructType(StructField("a", IntegerType) :: Nil)
    ScannerBuilder.build(equalFilters, ic)
    assert(ic.getScanners.get.scanners.head.isInstanceOf[BitMapScanner])
    val indexScanner = ic.getScanners.get.scanners.head
    val bitmapScanner = BitMapScanner(indexScanner.meta).withKeySchema(schema)
    bitmapScanner.intervalArray = indexScanner.intervalArray
    val fileNameIterator = tempDir.listFiles()
    for (fileName <- fileNameIterator) {
      if (fileName.toString.endsWith(OapFileFormat.OAP_DATA_EXTENSION)) {
        // Below will finally test the analyzeStatistics of BitMapScanner.
        val bitmapRes = bitmapScanner.readBehavior(new Path(fileName.toString), configuration)
        assert((bitmapRes == StatsAnalysisResult.FULL_SCAN ||
          bitmapRes == StatsAnalysisResult.SKIP_INDEX ||
          bitmapRes == StatsAnalysisResult.USE_INDEX), true)
      }
    }
    sql("drop oindex idxa on oap_test")
  }

  test("Bitmap index typical equal test") {
    val data: Seq[(Int, String)] = (1 to 200).map { i => (i, s"this is test $i") }
    data.toDF("key", "value").createOrReplaceTempView("t")
    sql("insert overwrite table oap_test select * from t")
    sql("create oindex idxa on oap_test (a) USING BITMAP")
    assert(sql(s"SELECT * FROM oap_test WHERE a = 10 AND a = 11").count() == 0)
    checkAnswer(sql(s"SELECT * FROM oap_test WHERE a = 20 OR a = 21"),
      Row(20, "this is test 20") :: Row(21, "this is test 21") :: Nil)
    sql("drop oindex idxa on oap_test")
  }
}
