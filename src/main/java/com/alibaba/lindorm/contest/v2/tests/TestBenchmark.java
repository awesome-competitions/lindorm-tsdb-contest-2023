//
// A simple evaluation program example helping you to understand how the
// evaluation program calls the protocols you will implement.
// Formal evaluation program is much more complex than this.
//

/*
 * Copyright Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.lindorm.contest.v2.tests;

import com.alibaba.lindorm.contest.TSDBEngine;
import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.v1.Const;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * This is an evaluation program sample.
 *  The evaluation program will create a new Database using the targeted
 *  local disk path, then write several rows, then check the correctness
 *  of the written data, and then run the read test.
 *  <p>
 *  The actual evaluation program is far more complex than the sample, e.g.,
 *  it might contain the restarting progress to clean all memory cache, it
 *  might test the memory cache strategies by a pre-warming procedure, and
 *  it might perform read and write tests crosswise, or even concurrently.
 *  Besides, as long as you write to the interface specification, you don't
 *  have to worry about incompatibility with our evaluation program.
 */

public class TestBenchmark {

  public static void main(String[] args) {
    File dataDir = new File(Const.TEST_DATA_DIR);
    if (dataDir.isFile()) {
      throw new IllegalStateException("Clean the directory before we start the demo");
    }
    if (!dataDir.isDirectory()) {
      boolean ret = dataDir.mkdirs();
      if (!ret) {
        throw new IllegalStateException("Cannot create the temp data directory: " + dataDir);
      }
    }
    for (File file : Objects.requireNonNull(dataDir.listFiles())) {
      boolean ret = file.delete();
      if (!ret) {
        throw new IllegalStateException("Cannot delete the temp data file: " + file);
      }
    }

    TSDBEngine tsdbEngineSample = new TSDBEngineImpl(dataDir);

    try {
      // Stage1: write
      tsdbEngineSample.connect();

      Map<String, ColumnValue.ColumnType> columnTypes = new HashMap<>();
      columnTypes.put("col1", ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
      columnTypes.put("col2", ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
      columnTypes.put("col3", ColumnValue.ColumnType.COLUMN_TYPE_STRING);
      Schema schema = new Schema(columnTypes);
      tsdbEngineSample.createTable("test", schema);
      String vin = "LSVNV2182E0200001";

      int startIntVal = 1;
      double startDoubleVal = 1.1;
      ByteBuffer stringVal = ByteBuffer.wrap(new byte[1024]);
      long startTimestamp = 1689091210000L;

      long s = System.currentTimeMillis();

      ArrayList<Row> rowList = new ArrayList<>();
      for (int i = 0; i < 10000; i ++){
        Map<String, ColumnValue> columns = new HashMap<>();
        columns.put("col1", new ColumnValue.IntegerColumn(startIntVal++));
        columns.put("col2", new ColumnValue.DoubleFloatColumn(startDoubleVal ++));
        columns.put("col3", new ColumnValue.StringColumn(stringVal));
        rowList.add(new Row(new Vin(vin.getBytes(StandardCharsets.UTF_8)), startTimestamp += 1000, columns));
      }
      for (int j = 0; j < 1000; j ++){
        tsdbEngineSample.write(new WriteRequest("test", rowList));
      }
      tsdbEngineSample.shutdown();
      System.out.println("write time:" + (System.currentTimeMillis() - s));

    } catch (IOException e) {
      e.printStackTrace();
    }
  }

}