/**
 * Copyright (c) 2013, Cloudera, Inc. All Rights Reserved.
 *
 * Cloudera, Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"). You may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for
 * the specific language governing permissions and limitations under the
 * License.
 */

package com.cloudera.traffic;

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
