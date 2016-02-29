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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class IdWritable implements WritableComparable<IdWritable> {
  private int stationId;
  private long time;
  
  public void set(int stationId, long time) {
    this.stationId = stationId;
    this.time = time;
  }
  
  public int getStationId() {
    return stationId;
  }
  
  public long getTime() {
    return time;
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    stationId = in.readInt();
    time = in.readLong();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(stationId);
    out.writeLong(time);
  }
  
  @Override
  public int compareTo(IdWritable other) {
    if (time != other.time) {
      return (int)(time - other.time);
    }
    return stationId - other.stationId;
  }
  
  @Override
  public boolean equals(Object o) {
    if (o instanceof IdWritable) {
      IdWritable other = (IdWritable)o;
      return other.time == time && other.stationId == stationId;
    } else {
      return false;
    }
  }
  
  @Override
  public int hashCode() {
    return stationId * 31 + (int)(time % Integer.MAX_VALUE);
  }
}
