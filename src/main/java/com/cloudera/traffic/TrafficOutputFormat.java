package com.cloudera.traffic;
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TrafficOutputFormat extends FileOutputFormat<IdWritable, DoubleWritable> {

  @Override
  public RecordWriter<IdWritable, DoubleWritable> getRecordWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();

    int numStations = conf.getInt("num.stations", -1);
    if (numStations == -1) {
      throw new IllegalStateException("num stations not specified");
    }
    long startTime = conf.getLong("start.time", -1);
    if (startTime == -1) {
      throw new IllegalStateException("start time not specified");
    }
    
    Path file = getDefaultWorkFile(context, "");
    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(file, false);
    
    return new TrafficIndexRecordWriter(fileOut, numStations, startTime);
  }
  
  class TrafficIndexRecordWriter extends RecordWriter<IdWritable, DoubleWritable> {
    
    private DataOutputStream dos;
    private int numStations;
    private long startTime;
    private boolean firstRecord;
    
    public TrafficIndexRecordWriter(FSDataOutputStream outputStream,
        int numStations, long startTime) throws IOException {
      
      dos = new DataOutputStream(outputStream);
      this.startTime = startTime;
      this.numStations = numStations;
      firstRecord = true;
    }

    @Override
    public void write(IdWritable key, DoubleWritable value) throws IOException,
        InterruptedException {
      if (firstRecord) {
        System.out.println("writing out start time: " + key.getTime());
        System.out.println("writing out num stations: " + numStations);
        dos.writeLong(key.getTime());
        dos.writeInt(numStations);
        firstRecord = false;
      }
      
      dos.writeInt(key.getStationId());
      dos.writeLong(key.getTime());
      dos.writeDouble(value.get());
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException,
        InterruptedException {
      dos.close();
    }
  }
}
