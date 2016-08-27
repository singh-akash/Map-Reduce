# Authors: Akash Singh, Surekha Jadhwani

cp opencsv-3.6.jar $HADOOP_HOME/share/hadoop/common/

export CLASSPATH=$HADOOP_HOME/share/hadoop/mapreduce/lib/hadoop-annotations-2.6.3.jar:$HADOOP_HOME/share/hadoop/common/hadoop-common-2.6.3.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-2.6.3.jar:$HADOOP_HOME/bin/Code/org/northeaster/mapreduce:$HADOOP_HOME/bin/Code:$HADOOP_HOME/share/hadoop/common/opencsv-3.6.jar:

# compile code
cd code
mvn clean package
cd ../

# copy jar
cp code/target/uber-perf-0.0.1.jar ./job.jar

# compile code for running on EMR
mvn clean package

cp target/uber-perf-0.0.1.jar ./