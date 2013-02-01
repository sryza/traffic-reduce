Traffic Induce
==============
This repository contains MapReduce programs for analyzing traffic data.  This
document contains instructions for compiling them and running them. Everything
below should be run from the project's root directory.  We assume an existing
hadoop installation, as well as maven and java.

## Building

    mvn install

## Placing the sample data on your cluster

    hadoop fs -mkdir trafficcounts
    hadoop fs -put samples/input.txt trafficcounts

## Running

    hadoop jar target/trafficinduce-1.0-SNAPSHOT.jar AveragerRunner trafficcounts/input.txt trafficcounts/output

## Inspecting the output

    hadoop fs -cat /trafficcounts/output/part-00000

## Running the MRUnit tests

    mvn test

