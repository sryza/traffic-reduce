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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IndexMapper extends Mapper<LongWritable, Text, IdWritable, DoubleWritable> {
  
  private final static Map<String, Double> averageMap = new HashMap<String, Double>();

  private IdWritable outKey = new IdWritable();
  private DoubleWritable outValue = new DoubleWritable();
  
  @Override
  public void setup(Context context) throws IOException {
    Path[] cacheFiles = context.getLocalCacheFiles();
    System.out.println("cache files: " + Arrays.asList(cacheFiles));
//    URI[] otherCacheFiles = context.getCacheFiles();
//    System.out.println("other cache files: " + Arrays.asList(otherCacheFiles));
    Path averagesFile = cacheFiles[0];
    readAveragesFile(averagesFile, context.getConfiguration());
  }
  
  @Override
  public void map(LongWritable key, Text line, Context context)
      throws IOException, InterruptedException {
    String[] tokens = line.toString().split(",");
    try {
      String dateTime = tokens[0];
      String stationId = tokens[1];
      String trafficCount = tokens[9];
      
      String timeOfWeek = stationId + "_" + TimeUtil.toTimeOfWeek(dateTime);
      outKey.set(Integer.parseInt(stationId), TimeUtil.toTime(dateTime));
  
      Double averageFlow = averageMap.get(timeOfWeek);

      outValue.set(Integer.parseInt(trafficCount) - averageFlow);
    } catch (Exception ex) {
      context.getCounter("Traffic Counters", ex.getClass().getName()).increment(1);
      outValue.set(-1.0);
      context.getCounter("Traffic Counters", "Skipped values").increment(1);
    }
    
    context.write(outKey, outValue);
  }
  
  private static synchronized void readAveragesFile(Path averagesFile, Configuration conf)
      throws IOException {
    // if we're using local runner, only need to read once
    if (!averageMap.isEmpty()) {
      return;
    }
    FileSystem fs = averagesFile.getFileSystem(conf);
    SequenceFile.Reader reader = new SequenceFile.Reader(fs, averagesFile, conf);
    Text key = new Text();
    AverageWritable value = new AverageWritable();
    while (reader.next(key, value)) {
      averageMap.put(key.toString(), value.getAverage());
    }
    reader.close();
  }
}
