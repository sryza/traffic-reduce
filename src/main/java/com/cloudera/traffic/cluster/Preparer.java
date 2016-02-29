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

package com.cloudera.traffic.cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.crunch.DoFn;
import org.apache.crunch.Emitter;
import org.apache.crunch.PGroupedTable;
import org.apache.crunch.Pair;
import org.apache.crunch.PCollection;
import org.apache.crunch.PTable;
import org.apache.crunch.Pipeline;
import org.apache.crunch.Tuple3;
import org.apache.crunch.impl.mem.MemPipeline;
import org.apache.crunch.types.PTypeFamily;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Preparer extends Configured implements Tool, Serializable {
  private static final int DATE_INDEX = 0;
  private static final int STATION_INDEX = 1;
  private static final int COUNT_INDEX = 9;
  private static final int SPEED_INDEX = 11;
  
  private static final class MeasurementTupleComparator
    implements Comparator<Tuple3<String, Integer, Double>> {
    @Override
    public int compare(Tuple3<String, Integer, Double> o1,
        Tuple3<String, Integer, Double> o2) {
      return o1.first().compareTo(o2.first());
    }
  }
  
  @Override
  @SuppressWarnings("serial")
  public int run(String[] args) throws Exception {
//    Pipeline pipeline = new MRPipeline(Preparer.class, getConf());
    Pipeline pipeline = MemPipeline.getInstance();

    PCollection<String> lines = pipeline.readTextFile(args[0]);
    PTypeFamily tf = lines.getTypeFamily();
    
    PTable<Integer, Tuple3<String, Integer, Double>> words = lines.parallelDo("my splitter",
        new DoFn<String, Pair<Integer, Tuple3<String, Integer, Double>>>() {
      
      @Override
      public void process(String line, Emitter<Pair<Integer, Tuple3<String, Integer, Double>>> emitter) {
        String[] tokens = line.split(",\\s*");
        try {
          int stationId = Integer.parseInt(tokens[STATION_INDEX]);
          int trafficCount = (tokens.length <= COUNT_INDEX || tokens[COUNT_INDEX].isEmpty()) ? -1 :
            Integer.parseInt(tokens[COUNT_INDEX]);
          double speed = (tokens.length <= SPEED_INDEX || tokens[SPEED_INDEX].isEmpty()) ? -1 :
            Double.parseDouble(tokens[SPEED_INDEX]);
          
          Tuple3<String, Integer, Double> tuple = new Tuple3(
              tokens[DATE_INDEX], trafficCount, speed);
          
          emitter.emit(new Pair(stationId, tuple));
        } catch (Exception ex) {
          getContext().getCounter("Exceptions", ex.getClass().getName()).increment(1);
          System.out.println("Exception in map: " + ex + " for tokens " + Arrays.asList(tokens));
        }
      }
    }, tf.tableOf(tf.ints(), tf.triples(tf.strings(), tf.ints(), tf.doubles())));
    
    PGroupedTable<Integer, Tuple3<String, Integer, Double>> grouped = words.groupByKey();
    PCollection<String> output = grouped.parallelDo(
        new DoFn<Pair<Integer, Iterable<Tuple3<String, Integer, Double>>>, String>() {
      @Override
      public void process(
          Pair<Integer, Iterable<Tuple3<String, Integer, Double>>> measurements,
          Emitter<String> emitter) {
        List<Tuple3<String, Integer, Double>> measurementsList =
            new ArrayList<Tuple3<String, Integer, Double>>();
        for (Tuple3<String, Integer, Double> measurement : measurements.second()) {
          measurementsList.add(measurement);
        }
        // sort by date
        Collections.sort(measurementsList, new MeasurementTupleComparator());
        StringBuffer sb = new StringBuffer();
        sb.append(measurements.first());
        for (Tuple3<String, Integer, Double> measurement : measurementsList) {
          sb.append(",");
          sb.append(measurement.second());
          sb.append(",");
          sb.append(measurement.third());
        }
        emitter.emit(sb.toString());
      }
    }, tf.strings());

    pipeline.writeTextFile(output, args[1]);
    pipeline.run();
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new Preparer(), args);
  }
}
