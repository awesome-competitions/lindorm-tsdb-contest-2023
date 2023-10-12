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

public class TestBenchmarkRead {


  static final int parallel = 1;

  static final int vinCount = 150 / parallel;

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

    final TSDBEngine tsdbEngineSample = new TSDBEngineImpl(dataDir);

    try {
      // reload
      tsdbEngineSample.connect();

      final CountDownLatch cdl1 = new CountDownLatch(parallel);
      for (int i = 0; i < parallel; i ++){
        final int index = i;
        new Thread(() -> {
          try {
            query(tsdbEngineSample, 200001 + 2000 * index);
          } catch (IOException e) {
            e.printStackTrace();
          }
          cdl1.countDown();
        }).start();
      }
      cdl1.await();

    } catch (IOException e) {
      e.printStackTrace();
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static void query(TSDBEngine tsdbEngineSample, int vinId) throws IOException {
    long startTimestamp = 1689091210000L;
    long s = System.currentTimeMillis();

    // latest query
    ArrayList<Vin> vinList = new ArrayList<>();

    s = System.currentTimeMillis();
    for (int i = 0; i < vinCount; i ++){
      String vin = "LSVNV2182E0" + (vinId + i);
      vinList.clear();
      vinList.add(new Vin(vin.getBytes(StandardCharsets.UTF_8)));
      tsdbEngineSample.executeLatestQuery(new LatestQueryRequest("test", vinList, Const.EMPTY_COLUMNS));
    }
    System.out.println("latest query time:" + (System.currentTimeMillis() - s));

    // range query
    s = System.currentTimeMillis();
    for (int j = 0; j < vinCount; j ++){
      String vin = "LSVNV2182E0" + (vinId + j);
      startTimestamp = 1689091210000L;
      for (int k = 0; k < 360; k ++){
        tsdbEngineSample.executeTimeRangeQuery(new TimeRangeQueryRequest("test", new Vin(vin.getBytes(StandardCharsets.UTF_8)), Const.EMPTY_COLUMNS, startTimestamp, startTimestamp + 100 * 1000));
        startTimestamp += 100 * 1000;
      }
    }
    System.out.println("range query time:" + (System.currentTimeMillis() - s));

    // agg query
    s = System.currentTimeMillis();
    for (int j = 0; j < vinCount; j ++){
      String vin = "LSVNV2182E0" + (vinId + j);
      startTimestamp = 1689091210000L;
      for (int k = 0; k < 3; k ++){
        tsdbEngineSample.executeAggregateQuery(new TimeRangeAggregationRequest("test", new Vin(vin.getBytes(StandardCharsets.UTF_8)), "col_int_2", startTimestamp, startTimestamp + 10800 * 1000, Aggregator.AVG));
        startTimestamp += 10800 * 1000;
      }
    }
    System.out.println("agg query time:" + (System.currentTimeMillis() - s));
  }

}