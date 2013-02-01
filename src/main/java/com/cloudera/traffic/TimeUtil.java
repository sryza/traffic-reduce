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

import java.util.Calendar;

public class TimeUtil {
  /**
   * Parse a string that looks like 01/01/2012 00:00:00.
   * Returns an integer between 0 and 604,800, the number of seconds in a week.
   */
  public static int toTimeOfWeek(String dateTime) {
    String[] tokens = dateTime.split(" ");
    String date = tokens[0];
    String time = tokens[1];
    String[] dateTokens = date.split("/");
    String[] timeTokens = time.split(":");
    
    Calendar calendar = Calendar.getInstance();
    calendar.set(Integer.parseInt(dateTokens[2]), Integer.parseInt(dateTokens[0])-1,
        Integer.parseInt(dateTokens[1]));
    int dayOfWeek = calendar.get(Calendar.DAY_OF_WEEK);
    
    int daySeconds = Integer.parseInt(timeTokens[0]) * 60 * 60 +
        Integer.parseInt(timeTokens[1]) * 60 + Integer.parseInt(timeTokens[2]);
    return dayOfWeek * 60 * 60 * 24 + daySeconds;
  }
  
  public static long toTime(String dateTime) {
    String[] tokens = dateTime.split(" ");
    String date = tokens[0];
    String time = tokens[1];
    String[] dateTokens = date.split("/");
    String[] timeTokens = time.split(":");
    
    Calendar calendar = Calendar.getInstance();
    calendar.set(Integer.parseInt(dateTokens[2]), Integer.parseInt(dateTokens[0])-1,
        Integer.parseInt(dateTokens[1]), Integer.parseInt(timeTokens[0]),
        Integer.parseInt(timeTokens[1]), Integer.parseInt(timeTokens[2]));
    
    return calendar.getTimeInMillis();
  }
}
