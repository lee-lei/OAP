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

package org.apache.spark.sql.execution.datasources.oap.orc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.orc.storage.ql.exec.vector.ColumnVector;

import org.apache.spark.sql.execution.datasources.orc.OrcColumnVector;
import org.apache.spark.sql.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.StructField;

/**
 * The OapIndexOrcColumnarBatchReader class has rowIds in order to scan the data
 * from the predefined row ids.
 */
public class IndexedOrcColumnarBatchReader extends OrcColumnarBatchReader {

  // Below three fields are added by Oap index.
  private int[] rowIds;

  private int curRowIndex;

  private int rowLength;

  public IndexedOrcColumnarBatchReader(boolean useOffHeap, boolean copyToSpark, int[] rowIds) {
    super(useOffHeap, copyToSpark);
    this.rowIds = rowIds;
  }

  /**
   * Initialize ORC file reader and batch record reader.
   * Please note that `initBatch` is needed to be called after this.
   * This method is customized by Oap.
   */
  @Override
  public void initialize(
      Path file, Configuration conf) throws IOException {
    super.initialize(file, conf);
    this.rowLength = this.rowIds.length;
    this.curRowIndex = 0;
    recordReader.seekToRow(rowIds[curRowIndex]);
  }

  /**
   * Return true if there exists more data in the next batch. If exists, prepare the next batch
   * by copying from ORC VectorizedRowBatch columns to Spark ColumnarBatch columns.
   */
  @Override
  public boolean nextBatch() throws IOException {
    if (curRowIndex >= rowLength) return false;
    recordReader.nextBatch(batch);
    int batchSize = batch.size;
    if (batchSize == 0) {
      return false;
    }
    int j = curRowIndex + 1;
    /* Orc readers support backward scan if the row Ids are out of order.
     * However, with the ascending ordered row Ids, the adjacent rows will
     * be scanned in the same batch. Below is expected that the row Ids are
     * ascending order.
     * Find the next row Id which is not in the same batch with the current row Id.
     */
    while (j < rowLength && (rowIds[curRowIndex] + batchSize) >= rowIds[j]) {
      j++;
    }
    curRowIndex = j;
    // Prepare to jump to the row for the next batch.
    if (j < rowLength) {
      recordReader.seekToRow(rowIds[curRowIndex]);
    }
    columnarBatch.setNumRows(batchSize);

    if (!copyToSpark) {
      for (int i = 0; i < requiredFields.length; i++) {
        if (requestedColIds[i] != -1) {
          ((OrcColumnVector) orcVectorWrappers[i]).setBatchSize(batchSize);
        }
      }
      return true;
    }

    for (WritableColumnVector vector : columnVectors) {
      vector.reset();
    }

    for (int i = 0; i < requiredFields.length; i++) {
      StructField field = requiredFields[i];
      WritableColumnVector toColumn = columnVectors[i];

      if (requestedColIds[i] >= 0) {
        ColumnVector fromColumn = batch.cols[requestedColIds[i]];

        if (fromColumn.isRepeating) {
          putRepeatingValues(batchSize, field, fromColumn, toColumn);
        } else if (fromColumn.noNulls) {
          putNonNullValues(batchSize, field, fromColumn, toColumn);
        } else {
          putValues(batchSize, field, fromColumn, toColumn);
        }
      }
    }
    return true;
  }
}
