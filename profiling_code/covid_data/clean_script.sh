#!/bin/bash

rm *.class
rm *.jar

hdfs dfs -rm -r -f test

java -version
yarn classpath

javac -classpath "$(yarn classpath)" -d . CleanMapper.java
javac -classpath "$(yarn classpath)" -d . CleanReducer.java
javac -classpath "$(yarn classpath)":. -d . Clean.java

jar -cvf Clean.jar *.class

hdfs dfs -mkdir test
hdfs dfs -mkdir test/input

hdfs dfs -put COVID-19_Reported_Patient_Impact_and_Hospital_Capacity_by_Facility.csv test/input

hadoop jar Clean.jar Clean test/input/COVID-19_Reported_Patient_Impact_and_Hospital_Capacity_by_Facility.csv /user/rav321/test/output