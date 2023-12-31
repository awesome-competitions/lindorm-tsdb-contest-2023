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

package com.alibaba.lindorm.contest.v1.tests;

import com.alibaba.lindorm.contest.TSDBEngine;
import com.alibaba.lindorm.contest.TSDBEngineImpl;
import com.alibaba.lindorm.contest.v1.Const;
import com.alibaba.lindorm.contest.structs.*;

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

public class EvaluationSample {

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


      ByteBuffer buffer = ByteBuffer.allocate(3);
      buffer.put((byte) 70);
      buffer.put((byte) 71);
      buffer.put((byte) 72);
      buffer.flip();
      ArrayList<Vin> vinList = new ArrayList<>();

      Map<String, ColumnValue> columns = new HashMap<>();
      columns.put("col1", new ColumnValue.IntegerColumn(123));
      columns.put("col2", new ColumnValue.DoubleFloatColumn(37.16));
      columns.put("col3", new ColumnValue.StringColumn(buffer));
      String str = "LSVNV2182E0200001";
      vinList.add(new Vin(str.getBytes(StandardCharsets.UTF_8)));
      ArrayList<Row> rowList = new ArrayList<>();
      rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 1689091210001L, columns));
      rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 1689091210000L, columns));
      str = "LSVNV2182E0200002";
      vinList.add(new Vin(str.getBytes(StandardCharsets.UTF_8)));
      rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 1689091211000L, columns));

      columns = new HashMap<>();
      columns.put("col1", new ColumnValue.IntegerColumn(124));
      columns.put("col2", new ColumnValue.DoubleFloatColumn(36.17));
      columns.put("col3", new ColumnValue.StringColumn(buffer));
      rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 1689091212000L, columns));

      columns = new HashMap<>();
      columns.put("col1", new ColumnValue.IntegerColumn(125));
      columns.put("col2", new ColumnValue.DoubleFloatColumn(36.18));
      columns.put("col3", new ColumnValue.StringColumn(buffer));
      rowList.add(new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), 1689091213000L, columns));

      String str3 = "LSVNV2182E0200003";
      long startTimestamp = 1689091400000L;
      for(int i = 0; i < 20; i ++){
        columns = new HashMap<>();
        columns.put("col1", new ColumnValue.IntegerColumn(i));
        columns.put("col2", new ColumnValue.DoubleFloatColumn((double) i /10));
        columns.put("col3", new ColumnValue.StringColumn(buffer));
        rowList.add(new Row(new Vin(str3.getBytes(StandardCharsets.UTF_8)), startTimestamp, columns));
        startTimestamp += 1000;
      }

      Map<String, ColumnValue.ColumnType> columnTypes = new HashMap<>();
      columnTypes.put("col1", ColumnValue.ColumnType.COLUMN_TYPE_INTEGER);
      columnTypes.put("col2", ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
      columnTypes.put("col3", ColumnValue.ColumnType.COLUMN_TYPE_STRING);
      Schema schema = new Schema(columnTypes);

      tsdbEngineSample.createTable("test", schema);
      tsdbEngineSample.write(new WriteRequest("test", rowList));
      Set<String> requestedColumns = new HashSet<>(Arrays.asList("col1", "col2", "col3"));

      tsdbEngineSample.shutdown();

      tsdbEngineSample = new TSDBEngineImpl(dataDir);
      tsdbEngineSample.connect();

      ArrayList<Row> resultSet = tsdbEngineSample.executeLatestQuery(new LatestQueryRequest("test", vinList, requestedColumns));
      showResult("executeLatestQuery", resultSet);
      resultSet = tsdbEngineSample.executeTimeRangeQuery(new TimeRangeQueryRequest("test", new Vin(str.getBytes(StandardCharsets.UTF_8)), requestedColumns, 1689091211000L, 1689091311000L));
      showResult("executeTimeRangeQuery", resultSet);
      resultSet = tsdbEngineSample.executeAggregateQuery(new TimeRangeAggregationRequest("test", new Vin(str.getBytes(StandardCharsets.UTF_8)), "col2", 1689091211000L, 1689091311000L, Aggregator.AVG));
      showResult("executeAggregateQuery col2", resultSet);
      resultSet = tsdbEngineSample.executeAggregateQuery(new TimeRangeAggregationRequest("test", new Vin(str.getBytes(StandardCharsets.UTF_8)), "col1", 1689091211000L, 1689091311000L, Aggregator.AVG));
      showResult("executeAggregateQuery col1", resultSet);
      resultSet = tsdbEngineSample.executeAggregateQuery(new TimeRangeAggregationRequest("test", new Vin(str.getBytes(StandardCharsets.UTF_8)), "col1", 1689091211000L, 1689091111000L, Aggregator.AVG));
      showResult("executeAggregateQuery empty", resultSet);
      resultSet = tsdbEngineSample.executeAggregateQuery(new TimeRangeAggregationRequest("test", new Vin(str.getBytes(StandardCharsets.UTF_8)), "col2", 1689091211000L, 1689091311000L, Aggregator.MAX));
      showResult("executeAggregateQuery col2 max", resultSet);
      resultSet = tsdbEngineSample.executeAggregateQuery(new TimeRangeAggregationRequest("test", new Vin(str.getBytes(StandardCharsets.UTF_8)), "col1", 1689091211000L, 1689091311000L, Aggregator.MAX));
      showResult("executeAggregateQuery col1 max", resultSet);
      resultSet = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest("test", new Vin(str.getBytes(StandardCharsets.UTF_8)), "col1", 1689091211000L, 1689091213000L, Aggregator.AVG, 1000, new CompareExpression(new ColumnValue.IntegerColumn(0), CompareExpression.CompareOp.GREATER)));
      showResult("executeDownsampleQuery", resultSet);
      resultSet = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest("test", new Vin(str3.getBytes(StandardCharsets.UTF_8)), "col2", 1689091400000L, 1689091400000L + 20 * 1000, Aggregator.MAX, 4000, new CompareExpression(new ColumnValue.DoubleFloatColumn(-1), CompareExpression.CompareOp.GREATER)));
      showResult("executeDownsampleQuery str3", resultSet);
      resultSet = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest("test", new Vin(str3.getBytes(StandardCharsets.UTF_8)), "col1", 1689091400000L, 1689091400000L + 20 * 1000, Aggregator.MAX, 4000, new CompareExpression(new ColumnValue.IntegerColumn(-1), CompareExpression.CompareOp.GREATER)));
      showResult("executeDownsampleQuery str3", resultSet);
      resultSet = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest("test", new Vin(str3.getBytes(StandardCharsets.UTF_8)), "col2", 1689091400000L, 1689091400000L + 20 * 1000, Aggregator.AVG, 4000, new CompareExpression(new ColumnValue.DoubleFloatColumn(-1), CompareExpression.CompareOp.GREATER)));
      showResult("executeDownsampleQuery str3", resultSet);
      resultSet = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest("test", new Vin(str3.getBytes(StandardCharsets.UTF_8)), "col1", 1689091400000L, 1689091400000L + 20 * 1000, Aggregator.AVG, 4000, new CompareExpression(new ColumnValue.IntegerColumn(-1), CompareExpression.CompareOp.GREATER)));
      showResult("executeDownsampleQuery str3", resultSet);
      resultSet = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest("test", new Vin(str3.getBytes(StandardCharsets.UTF_8)), "col1", 1689091500000L, 1689091500000L + 20 * 1000, Aggregator.AVG, 4000, new CompareExpression(new ColumnValue.IntegerColumn(-1), CompareExpression.CompareOp.GREATER)));
      showResult("executeDownsampleQuery str3", resultSet);
      resultSet = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest("test", new Vin(str3.getBytes(StandardCharsets.UTF_8)), "col1", 1689091400000L, 1689091400000L + 20 * 1000, Aggregator.AVG, 4000, new CompareExpression(new ColumnValue.IntegerColumn(100), CompareExpression.CompareOp.GREATER)));
      showResult("executeDownsampleQuery str3", resultSet);


    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void showResult(String name, ArrayList<Row> resultSet) {
    for (Row result : resultSet)
      System.out.println(result);
    System.out.println("-------" + name + "-------");
  }
}