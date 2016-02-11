PROBLEM STATEMENT:
Refer Assignment_3_Report.pdf for details

DESCRIPTION:
This code can be executed in different environments by passing appropriate command line parameters.
It can run in single-threaded, multi-threaded, pseudo-distributed, fully-distributed modes.
Output is in the for of "m C v" triples where m identifies the month, C is an airline, and v is the median or mean. 
Single-threaded and multi-threaded modes give the output on command line directly(Sysout) while other two modes write output at remote directories.
We can check their output by fetching the output file from remote directory.

This code also contains a benchmarking harness(Makefile) which runs the code in all environments and generates a detailed report for analysis.
The harness also writes the results of each execution in a .csv file (results.csv).

NOTE:
uber-perf-0.0.1.jar : contains benchmarking harness code
job.jar : contains core logic code

PRE-REQUISITES:
Following programs should be installed and proper environment variables should be set:
	1. Hadoop 2.6.3
	2. AWS S3 bucket should be created and data files and jar uploaded
	3. R ggplot, grid and markdown libraries
	4. Pandoc (For PDF generation from R) 
	5. opencsv-3.6.jar should be added to $HADOOP_HOME/share/hadoop/common/ location

TO EXECUTE THE PROGRAM:
You need to update perf*.txt files as per your environment in order to execute the program
Refer Perf-Readme.md for details.


1. Single-threaded mode:
Type following command:
a. mean price:
java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind="single-thread" -main=neu.mr.Plain -arguments="-input=${INPUT_DIR} -query=AvgPrice" -input=${INPUT_DIR} -output=/tmp -results=results.csv -name=mean -properties=perf_plain_seq.txt

b. median price:
java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind="single-thread" -main=neu.mr.Plain -arguments="-input=${INPUT_DIR} -query=Median" -input=${INPUT_DIR} -output=/tmp -results=results.csv -name=median -properties=perf_plain_seq.txt

c. fast median price:
java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind="single-thread" -main=neu.mr.Plain -arguments="-input=${INPUT_DIR} -query=FastMedian" -input=${INPUT_DIR} -output=/tmp -results=results.csv -name=fast-median -properties=perf_plain_seq.txt

where: ${INPUT_DIR} : is the input directory containing data files 
Example: ${INPUT_DIR} = /Users/Surekha/hadoop_ws/src/resources/all 

2. Multithreaded mode:
Type following command:
a. mean price:
java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind="multithread" -main=neu.mr.Plain -arguments="-p -input=${INPUT_DIR} -query=AvgPrice" -input=${INPUT_DIR} -output=/tmp -results=results.csv -name=mean -properties=perf_plain_par.txt

b. median price:
java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind="multithread" -main=neu.mr.Plain -arguments="-p -input=${INPUT_DIR} -query=Median" -input=${INPUT_DIR} -output=/tmp -results=results.csv -name=median -properties=perf_plain_par.txt

c. fast median price:
java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind="multithread" -main=neu.mr.Plain -arguments="-p -input=${INPUT_DIR} -query=FastMedian" -input=${INPUT_DIR} -output=/tmp -results=results.csv -name=fast-median -properties=perf_plain_par.txt

where: ${INPUT_DIR} : is the input directory containing data files 
Example: ${INPUT_DIR} = /Users/Surekha/hadoop_ws/src/resources/all 

3. Pseudo-distributed mode:
make format
make hstart
make INPUT_DIR=${INPUT_DIR} dir
hadoop fs -put ${INPUT_DIR}/* input
hadoop fs -rm -rf output

Type following command:
a. mean price:
java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind=local -main=neu.mr.A2Mean -arguments="-input=input -output=output" -input=input -output=output  -results=results.csv -name=mean -properties=perf_local.txt

b. median price:
java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind=local -main=neu.mr.A2Median -arguments="-input=input -output=output" -input=input -output=output  -results=results.csv -name=median -properties=perf_local.txt

c. fast median price:
java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind=local -main=neu.mr.A2FastMedian -arguments="-input=input -output=output" -input=input -output=output  -results=results.csv -name=fast-median -properties=perf_local.txt

make hstop

where: ${INPUT_DIR} : is the input directory containing data files 
Example: ${INPUT_DIR} = /Users/Surekha/hadoop_ws/src/resources/all 

4. Fully-distributed mode:
Type the following command:
a. mean price:
java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=s3n://${BUCKET}/job.jar -kind=cloud -main=neu.mr.A2Mean -arguments="-input=s3://${BUCKET}/input -output=s3://${BUCKET}/output" -fsroot=s3n://${BUCKET}/ -awskeyid="${ACCESS_KEY}" -awskey="${SECRET_ACCESS_KEY}" -results=results.csv -input=s3n://${BUCKET}/input -output=s3n://${BUCKET}/output -name=mean -properties=perf_cloud.txt

b. median price:
java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=s3n://${BUCKET}/job.jar -kind=cloud -main=neu.mr.A2Median -arguments="-input=s3://${BUCKET}/input -output=s3://${BUCKET}/output" -fsroot=s3n://${BUCKET}/ -awskeyid="${ACCESS_KEY}" -awskey="${SECRET_ACCESS_KEY}" -results=results.csv -input=s3n://${BUCKET}/input -output=s3n://${BUCKET}/output -name=median -properties=perf_cloud.txt

c. fast median price:
java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=s3n://${BUCKET}/job.jar -kind=cloud -main=neu.mr.A2FastMedian -arguments="-input=s3://${BUCKET}/input -output=s3://${BUCKET}/output" -fsroot=s3n://${BUCKET}/ -awskeyid="${ACCESS_KEY}" -awskey="${SECRET_ACCESS_KEY}" -results=results.csv -input=s3n://${BUCKET}/input -output=s3n://${BUCKET}/output -name=fast-median -properties=perf_cloud.txt

where: ${BUCKET} is the AWS S3 bucket name, example: mra3sa
	   ${ACCESS_KEY} is the AWS access key
	   ${SECRET_ACCESS_KEY} is the AWS secret access key

5. Benchmarking harness:
make USER=${USER} INPUT_DIR=${INPUT_DIR} BUCKET=${BUCKET} ACCESS_KEY=${ACCESS_KEY} SECRET_ACCESS_KEY=${SECRET_ACCESS_KEY} benchmark

where ${USER} is the current user logged into the terminal for creating hadoop directory
	  ${INPUT_DIR} is the input directory location of the data files
	  ${BUCKET} is the AWS S3 bucket name, example: mra3sa
	  ${ACCESS_KEY} is the AWS access key
	  ${SECRET_ACCESS_KEY} is the AWS secret access key


