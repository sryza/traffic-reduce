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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AverageWritable implements Writable {

  private int numElements;
  private double average;
 
  public AverageWritable() {}
  
  public void set(int numElements, double average) {
    this.numElements = numElements;
    this.average = average;
  }
  
  public int getNumElements() {
    return numElements;
  }
  
  public double getAverage() {
    return average;
  }
  
  @Override
  public void readFields(DataInput input) throws IOException {
    numElements = input.readInt();
    average = input.readDouble();
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeInt(numElements);
    output.writeDouble(average);
  }
  
  public String toString() {
    return "(" + numElements + ", " + average + ")";
  }
  
  @Override
  public boolean equals(Object o) {
    AverageWritable other = (AverageWritable)o;
    return other.numElements == numElements && other.average - average < .0001;
  }
  
  @Override
  public int hashCode() {
    return numElements * 31 + (int)average;
  }
}
