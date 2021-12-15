#!/bin/bash

rm *.class
rm *.jar

hdfs dfs -rm -r -f test

java -version
yarn classpath

javac -classpath "$(yarn classpath)" -d . CountRecsMapper.java
javac -classpath "$(yarn classpath)" -d . CountRecsReducer.java
javac -classpath "$(yarn classpath)":. -d . CountRecs.java

jar -cvf CountRecs.jar *.class

hdfs dfs -mkdir test
hdfs dfs -mkdir test/input

hdfs dfs -put COVID-19_Reported_Patient_Impact_and_Hospital_Capacity_by_Facility.csv test/input

hadoop jar CountRecs.jar CountRecs test/input/COVID-19_Reported_Patient_Impact_and_Hospital_Capacity_by_Facility.csv /user/rav321/test/output

hdfs dfs -text test/output/part-r-00000