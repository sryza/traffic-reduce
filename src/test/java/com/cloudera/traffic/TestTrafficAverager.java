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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class TestTrafficAverager {
  private MapDriver<LongWritable, Text, Text, AverageWritable> mapDriver;
  private ReduceDriver<Text, AverageWritable, Text, AverageWritable> reduceDriver;
  
  @Before
  public void setup() {
    AveragerMapper mapper = new AveragerMapper();
    AveragerReducer reducer = new AveragerReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
  }
  
  @Test
  public void testMapper() throws IOException {
    String line = "01/01/2012 00:00:00,311831,3,5,S,OR,,118,0,200,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,";
    mapDriver.withInput(new LongWritable(0), new Text(line));
    Text outKey = new Text("311831_" + TimeUtil.toTimeOfWeek("01/01/2012 00:00:00"));
    AverageWritable outVal = new AverageWritable();
    outVal.set(1, 200.0);
    mapDriver.withOutput(outKey, outVal);
    mapDriver.runTest();
  }
  
  @Test
  public void testReducer() {
    AverageWritable avg1 = new AverageWritable();
    avg1.set(1, 2.0);
    AverageWritable avg2 = new AverageWritable();
    avg2.set(3, 1.0);
    AverageWritable outAvg = new AverageWritable();
    outAvg.set(4, 1.25);
    Text key = new Text("331831_86400");
    
    reduceDriver.withInput(key, Arrays.asList(avg1, avg2));
    reduceDriver.withOutput(key, outAvg);
    reduceDriver.runTest();
  }
}
