package com.cloudera.traffic.web;

import java.util.Calendar;

public class TrafficWebServer {
  public static void main(String[] args) {
    long now = System.currentTimeMillis();
    System.out.println("now: " + now);
    Calendar calendar = Calendar.getInstance();
    calendar.clear();
    calendar.set(Calendar.YEAR, 2013);
    calendar.set(Calendar.MONTH, 0);
    calendar.set(Calendar.DATE, 1);
    calendar.set(Calendar.HOUR, 5);
    System.out.println("then: " + calendar.getTimeInMillis());
    calendar.set(Calendar.HOUR, 6);
    System.out.println("then2: " + calendar.getTimeInMillis());
  }
}
