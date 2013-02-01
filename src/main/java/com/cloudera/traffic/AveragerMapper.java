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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AveragerMapper extends Mapper<LongWritable, Text, Text, AverageWritable> {
  
  private AverageWritable outAverage = new AverageWritable();
  private Text id = new Text();
  
  @Override
  public void map(LongWritable key, Text line, Context context)
      throws InterruptedException, IOException {
    String[] tokens = line.toString().split(",");
    if (tokens.length < 10) {
      context.getCounter("Averager Counters", "Blank lines").increment(1);
      return;
    }
    String dateTime = tokens[0];
    String stationId = tokens[1];
    String trafficCount = tokens[9];

    if (trafficCount.length() > 0) {
      id.set(stationId + "_" + TimeUtil.toTimeOfWeek(dateTime));
      if (trafficCount.matches("[0-9]+")) {
        outAverage.set(1, Integer.parseInt(trafficCount));
      } else {
        context.getCounter("Averager Counters", "Missing vehicle flows").increment(1);
      }
      
      context.write(id, outAverage);
    }
  }
}
