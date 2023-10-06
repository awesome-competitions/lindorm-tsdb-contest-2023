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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

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

public class TestAggregation {

    static String str = "LSVNV2182E0200001";

    static Row newRow(long timestamp, double v){
        Map<String, ColumnValue> columns = new HashMap<>();
        columns.put("col", new ColumnValue.DoubleFloatColumn(v));
        return new Row(new Vin(str.getBytes(StandardCharsets.UTF_8)), timestamp, columns);
    }

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
            columnTypes.put("col", ColumnValue.ColumnType.COLUMN_TYPE_DOUBLE_FLOAT);
            Schema schema = new Schema(columnTypes);
            tsdbEngineSample.createTable("test", schema);

            ArrayList<Row> rowList = new ArrayList<>();
            rowList.add(newRow(1693274400000L, 15));
            rowList.add(newRow(1693274401000L, 11));
            rowList.add(newRow(1693274402000L, 13));
            rowList.add(newRow(1693274403000L, 15));
            rowList.add(newRow(1693274404000L, 17));
            rowList.add(newRow(1693274405000L, 19));
            rowList.add(newRow(1693274406000L, 21));
            rowList.add(newRow(1693274407000L, 20));
            rowList.add(newRow(1693274408000L, 18));
            rowList.add(newRow(1693274409000L, 16));
            tsdbEngineSample.write(new WriteRequest("test", rowList));
            tsdbEngineSample.shutdown();

            tsdbEngineSample = new TSDBEngineImpl(dataDir);
            tsdbEngineSample.connect();

            ArrayList<Row> resultSet = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest("test", new Vin(str.getBytes(StandardCharsets.UTF_8)), "col",
                    1693274400183L, 1693274410183L, Aggregator.AVG, 5000, new CompareExpression(new ColumnValue.DoubleFloatColumn(11), CompareExpression.CompareOp.GREATER)));
            showResult("executeDownsampleQuery", resultSet);
            resultSet = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest("test", new Vin(str.getBytes(StandardCharsets.UTF_8)), "col",
                    1693274400183L, 1693274410183L, Aggregator.AVG, 5000, new CompareExpression(new ColumnValue.DoubleFloatColumn(20), CompareExpression.CompareOp.EQUAL)));
            showResult("executeDownsampleQuery", resultSet);
            resultSet = tsdbEngineSample.executeDownsampleQuery(new TimeRangeDownsampleRequest("test", new Vin(str.getBytes(StandardCharsets.UTF_8)), "col",
                    1693274410000L, 1693274420000L, Aggregator.AVG, 5000, new CompareExpression(new ColumnValue.DoubleFloatColumn(10), CompareExpression.CompareOp.GREATER)));
            showResult("executeDownsampleQuery", resultSet);
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