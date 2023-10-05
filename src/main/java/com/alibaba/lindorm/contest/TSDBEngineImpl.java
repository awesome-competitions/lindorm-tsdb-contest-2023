//
// You should modify this file.
//
// Refer TSDBEngineSample.java to ensure that you have understood
// the interface semantics correctly.
//

package com.alibaba.lindorm.contest;

import com.alibaba.lindorm.contest.structs.*;
import com.alibaba.lindorm.contest.v1.Table;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class TSDBEngineImpl extends TSDBEngine {

  private Table table;

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
    File directory = getDataPath();
    File[] children = directory.listFiles(f -> f.getName().endsWith(".schema"));
    if (children != null) {
      for(File child: children){
        String tableName = child.getName().substring(0, child.getName().indexOf("."));
        try{
          this.table = Table.load(directory.getAbsolutePath(), tableName);
        }catch(Throwable e){
          e.printStackTrace();
          throw new RuntimeException("connect err:" + e.getMessage());
        }
      }
    }
  }

  @Override
  public void createTable(String tableName, Schema schema) throws IOException {
    try {
      this.table = new Table(getDataPath().getAbsolutePath(), tableName);
      this.table.setSchema(schema);
//      Monitor.start(table);
    } catch (Throwable e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void shutdown() {
    try {
      table.force();
      System.out.println("shutdown table:" + table.getName() + ", size:" + table.size());
      table.close();
    } catch (Throwable e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
  }

  @Override
  public void write(WriteRequest wReq) throws IOException {
    try {
      this.table.upsert(wReq.getRows());
    } catch (Throwable e) {
      e.printStackTrace();
      throw new IOException("upsert err:" + e.getMessage());
    }
  }

  @Override
  public ArrayList<Row> executeLatestQuery(LatestQueryRequest pReadReq) throws IOException {
    try{
      return this.table.executeLatestQuery(pReadReq.getVins(), pReadReq.getRequestedColumns());
    }catch (Throwable e){
      e.printStackTrace();
      StringBuilder builder = new StringBuilder();
      for (Vin vin: pReadReq.getVins()){
        builder.append(vin.toString()).append(" ");
      }
      throw new IOException("executeLatestQuery err:" + e.getMessage() + ";" + builder);
    }
  }

  @Override
  public ArrayList<Row> executeTimeRangeQuery(TimeRangeQueryRequest trReadReq) throws IOException {
    try {
      return this.table.executeTimeRangeQuery(trReadReq.getVin(), trReadReq.getTimeLowerBound(), trReadReq.getTimeUpperBound(), trReadReq.getRequestedColumns());
    } catch (Throwable e) {
      e.printStackTrace();
      throw new IOException("executeTimeRangeQuery err:" + e.getMessage() + ";" + trReadReq.getVin());
    }
  }

  @Override public ArrayList<Row> executeAggregateQuery(TimeRangeAggregationRequest aggregationReq) throws IOException {
    try {
      return this.table.executeAggregateQuery(aggregationReq.getVin(), aggregationReq.getTimeLowerBound(), aggregationReq.getTimeUpperBound(), aggregationReq.getColumnName(), aggregationReq.getAggregator());
    } catch (Throwable e) {
      e.printStackTrace();
      throw new IOException("executeAggregateQuery err:" + e.getMessage() + ";" + aggregationReq.getVin());
    }
  }

  @Override public ArrayList<Row> executeDownsampleQuery(TimeRangeDownsampleRequest downsampleReq) throws IOException {
    try {
      return this.table.executeDownsampleQuery(downsampleReq.getVin(), downsampleReq.getTimeLowerBound(), downsampleReq.getTimeUpperBound(), downsampleReq.getColumnName(), downsampleReq.getAggregator(), downsampleReq.getInterval(), downsampleReq.getColumnFilter());
    } catch (Throwable e) {
      e.printStackTrace();
      throw new IOException("executeDownsampleQuery err:" + e.getMessage() + ";" + downsampleReq.getVin());
    }
  }
}
