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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AveragerReducer extends Reducer<Text, AverageWritable, Text, AverageWritable> {
  
  private AverageWritable outAverage = new AverageWritable();
  
  @Override
  public void reduce(Text key, Iterable<AverageWritable> averages, Context context)
      throws InterruptedException, IOException {
    double sum = 0.0;
    int numElements = 0;
    for (AverageWritable average : averages) {
      sum += average.getAverage() * average.getNumElements();
      numElements += average.getNumElements();
    }
    double average = sum / numElements;
    
    outAverage.set(numElements, average);
    context.write(key, outAverage);
  }
}
