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
import com.alibaba.lindorm.contest.v2.Const;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;

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

public class TestBenchmarkWrite {


  static final int parallel = 1;

  static final int vinCount = 150;

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

    final TSDBEngine tsdbEngineSample = new TSDBEngineImpl(dataDir);

    try {
      // Stage1: write
      tsdbEngineSample.connect();

      Map<String, ColumnValue.ColumnType> columnTypes = new HashMap<>();
      for (int i = 0; i < 40; i ++){
        columnTypes.put("col_int_" + i, ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
      }
      for (int i = 0; i < 10; i ++){
        columnTypes.put("col_double_" + i, ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
      }
      for (int i = 0; i < 10; i ++){
        columnTypes.put("col_str_" + i, ColumnValue.ColumnType.COLUMN_TYPE_STRING);
      }
      Schema schema = new Schema(columnTypes);
      tsdbEngineSample.createTable("test", schema);

      long s = System.currentTimeMillis();
      final CountDownLatch cdl = new CountDownLatch(parallel);
      for (int i = 0; i < parallel; i ++){
        final int index = i;
        new Thread(() -> {
          try {
            write(tsdbEngineSample, 200001 + 2000 * index);
          } catch (IOException e) {
            e.printStackTrace();
          }
          cdl.countDown();
        }).start();
      }
      cdl.await();
      tsdbEngineSample.shutdown();
      System.out.println("write time:" + (System.currentTimeMillis() - s));
      System.out.println("wait for dump heap....");
      Thread.sleep(100000000);

    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void write(TSDBEngine tsdbEngineSample, int vinId) throws IOException {
    int startIntVal = 1;
    double startDoubleVal = 1.1;
    ByteBuffer stringVal = ByteBuffer.wrap(new byte[21]);
    long startTimestamp = 1689091210000L;

    for (int k = 0; k < 36000; k ++){
      startTimestamp += 1000;
      int f = 0;
      for (int n1 = 0; n1 < vinCount / 50; n1 ++){
        ArrayList<Row> rowList = new ArrayList<>();
        Map<String, ColumnValue> columns = new HashMap<>();
        for (int n = 0; n < 40; n ++){
          columns.put("col_int_" + n, new ColumnValue.IntegerColumn(startIntVal++));
        }
        for (int n = 0; n < 10; n ++){
          columns.put("col_double_" + n, new ColumnValue.DoubleFloatColumn(startDoubleVal ++));
        }
        for (int n = 0; n < 10;n ++){
          columns.put("col_str_" + n, new ColumnValue.StringColumn(stringVal));
        }
        for (int j = 0; j < 50; j ++){
          f ++;
          String vin = "LSVNV2182E0" + (vinId + f);
          rowList.add(new Row(new Vin(vin.getBytes(StandardCharsets.UTF_8)), startTimestamp, columns));
        }
        tsdbEngineSample.write(new WriteRequest("test", rowList));
      }
    }
  }

}