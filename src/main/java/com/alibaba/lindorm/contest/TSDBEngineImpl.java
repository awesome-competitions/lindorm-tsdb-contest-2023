//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.LatestQueryRequest;
import com.alibaba.lindorm.contest.structs.Row;
import com.alibaba.lindorm.contest.structs.Schema;
import com.alibaba.lindorm.contest.structs.TimeRangeAggregationRequest;
import com.alibaba.lindorm.contest.structs.TimeRangeDownsampleRequest;
import com.alibaba.lindorm.contest.structs.TimeRangeQueryRequest;
import com.alibaba.lindorm.contest.structs.WriteRequest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class TSDBEngineImpl extends TSDBEngine {

  /**
   * This constructor's function signature should not be modified.
   * Our evaluation program will call this constructor.
   * The function's body can be modified.
   */
  public TSDBEngineImpl(File dataPath) {
    super(dataPath);
  }

  @Override
  public void connect() throws IOException {

  }

  @Override
  public void createTable(String tableName, Schema schema) throws IOException {

  }

  @Override
  public void shutdown() {

  }

  @Override
  public void write(WriteRequest wReq) throws IOException {

  }

  @Override
  public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
    return null;
  }

  @Override
  public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
    return null;
  }

  @Override public ArrayList<Row> executeAggregateQuery(TimeRangeAggregationRequest aggregationReq) throws IOException {
    return null;
  }

  @Override public ArrayList<Row> executeDownsampleQuery(TimeRangeDownsampleRequest downsampleReq) throws IOException {
    return null;
  }
}
