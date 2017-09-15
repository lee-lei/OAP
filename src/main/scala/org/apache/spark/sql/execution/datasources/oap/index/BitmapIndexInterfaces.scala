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

private[oap] abstract class BitmapIndexInterfaces {

  // TODO: Add the arguments and return types.
  def BitmapIndexOpenFile
  def BitmapIndexCloseFile

  def BitmapIndexBuild
  def BitmapIndexSearchFromOffset

  def BitmapIndexReadRows
  def BitmapIndexReadRow
  def BitmapIndexWriteRows
  def BitmapIndexWriteRow

  def BitmapIndexGetFromFibers
  def BitmapIndexGetFromFiber
  def BitmapIndexSaveToFibers
  def BitmapIndexSaveToFiber
  // If the index is updated or refreshed, the fibers will be flushed accordingly.
  def BitmapIndexFlushFibers
  def BitmapIndexFlushFiber
  // The sorted keyset can be cached either in fibers(DRAM) or NVRAM if no rooms in fiber caches.
  def BitmapIndexGetSortedKeySet
  // The sorted keyset can be updated into fibers or NVRAM once the index is updated or refreshed.
  def BitmapIndexUpdateSortedKeySet
  def BitmapIndexSaveSortedKeySet

  def BitmapIndexCompress
  def BitmapIndexDecompress

  def BitmapIndexEncode
  def BitmapIndexDecode

  def BitmapIndexConsistencyCheck
  def BitmapIndexDump
}
