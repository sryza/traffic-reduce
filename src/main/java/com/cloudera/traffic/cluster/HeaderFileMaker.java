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

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class HeaderFileMaker {
  public static void main(String[] args) throws IOException {
    File outPath = new File("header.csv");
    int month = 1;
    int daysInMonth = 31;
    int year = 2013;
    
    ArrayList<String> hourList = new ArrayList<String>();
    for (int day = 1; day <= daysInMonth; day++) {
      for (int hour = 0; hour < 24; hour++) {
        hourList.add(pad(month, 2) + "/" + pad(day, 2) + "/" + year + "_" + pad(hour, 2) + ":00:00");
      }
    }
    Collections.sort(hourList);

    BufferedWriter bw = new BufferedWriter(new FileWriter(outPath));
    bw.write("stationId,id\n");
    for (String time : hourList) {
      bw.write(time + "_flow" + ",double\n");
      bw.write(time + "_speed" + ",double\n");
    }
    bw.close();
  }
  
  private static String pad(int num, int howMany) {
    String str = "" + num;
    while (str.length() < howMany) {
      str = "0" + str;
    }
    return str;
  }
}
