#!/bin/bash

rm *.class
rm *.jar

java -version
yarn classpath

javac -classpath "$(yarn classpath)" -d . CountRecsMapper.java
javac -classpath "$(yarn classpath)" -d . CountRecsReducer.java
javac -classpath "$(yarn classpath)":. -d . CountRecs.java

jar -cvf CountRecs.jar *.class

hadoop jar CountRecs.jar CountRecs test/output/part-m-00000 /user/rav321/test/output2

hdfs dfs -text test/output2/part-r-00000