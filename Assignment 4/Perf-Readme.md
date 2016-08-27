# Perf 

A simple tool for repeatedly running Java MapReduce jobs and Java applications for the purpose of coarse-grained performance measurements. 

The tool support three kinds of execution modes: plain (Java on the local machine), local (pseudo distributed MR jobs) and could (EMR jobs).

Arguments are passed to it via the command line and a configuration file. None of this is very general, the tool is tailored to a particular set of assignments.

## Building

Typing 

```mvn clean package```

will create a self-contained JAR file with App.

## Running

Create a file named ``perf.txt`` in the local directory. Invoke Java with, for example,

```
java -cp uber-perf-0.0.1.jar  neu.perf.App -num=5 \
    -jar=job.jar  -kind=plain  -main=neu.otp.Main \
    -arguments="-query=A_3_Median" -input=../data  \
    -output=/tmp  -results=results.csv -name=i-median 
```


As sample cloud run looks like this

```
java -cp uber-perf-0.0.1.jar  neu.perf.App -num=3 \
  -jar=s3n://mrclassvitek/job.jar    -kind=cloud -main=neu.otp.Main_A3 \
  -arguments="-median -fsroot=s3n://mrclassvitek/ -awskeyid=$(AWS_ACCESS_KEY) -awskey=$(AWS_SECRET_KEY)" \
  -results=results.csv -input=s3n://mrclassvitek/years14-15 \
  -output=s3n://mrclassvitek/output -name=iv-medianfast 
```


A sample perf.txt file could be:

```

num = 1
# how many time do we run this job?

results = results.csv
# where to store the time of each run?

jar = job.jar
# where is the code of the job? 

name = iii-median
# how to call this job in the results.csv?

main = opt.neu.Main
# where is this job's main() method?

input = input
# where is the input data?

output = output
# which directory to use for outputs?

arguments ="-query=AvgPrice"
# additional arguments (other than -input and -output)

############### Hadoop Initialization ###################

hadoop.home = /usr/local/hadoop
# where is hadoop on this machine?

############ AWS S3 Initialization #######################

region = us-east-1
# where are we running? 

check.bucket = s3://mrclassvitek
# what bucket are we using?

check.input = s3://mrclassvitek/years14-15
# where is the input data?

check.logs = s3://mrclassvitek/logs
# where are the logs?

delete.output = s3://mrclassvitek/output
# which directory should we delete before running?

upload.jar = s3://mrclassvitek
# where should we upload the jar?

######### AWS EMR Cluster Configuration #################
cluster.name = MyCluster
# how to name the cluster?

step.name = Step
# how to name the step?

release.label = emr-4.3.0
# what EMR release?

log.uri = s3://mrclassvitek/logs
# where are the logs?

service.role = EMR_DefaultRole
# role?

job.flow.role = EMR_EC2_DefaultRole
# role?

ec2.key.name = jankey
# which ec2 key should we use?

instance.count = 2
# how many nodes to provision?

keep.job.flow.alive = false
# keep cluster alive?

master.instance.type = m3.xlarge
# which master instance type?

slave.instance.type = m3.xlarge
# which node instance type?
```
