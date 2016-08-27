PROBLEM STATEMENT:
Refer Assignment_4_Report.pdf for details

DESCRIPTION:
This code can be executed on AWS EMR.
Output of this program is a report with two graphs for N=1 and N=200 where N is the scheduled flight minutes.
Intermediate outputs will be copied to output and output2 directory respectively.

NOTE:
code directory : has core logic
src directory : has code for executing on AWS EMR
uber-perf-0.0.1.jar : contains execution helper code 
job.jar : contains core logic code
LM.csv : contains intercept and slope for each carrier and each year considering the entire data set as training data set.

PRE-REQUISITES:
Following programs should be installed and proper environment variables should be set:
	1. Hadoop 2.6.3 (If other version is installed, update the jar names in createJar.sh)
	3. R ply, markdown library
	4. Pandoc (For PDF generation from R) 
	5. opencsv-3.6.jar should be added to $HADOOP_HOME/share/hadoop/common/ location

TO BUILD THE PROGRAM:
Use the following command in the extracted directory
  make build
 - This command will build job.jar as well as uber-perf-0.0.1.jar

TO INITIALIZE S3:
Use the following command in the extracted directory
  make BUCKET_NAME=mra4 initS3
 - where: ${BUCKET_NAME} is the name of the new bucket to create
 For example: make BUCKET_NAME=mra4 initS3
 This command will create the mentioned bucket and copy all 337 files from Professor’s bucket to input directory	

TO EXECUTE THE PROGRAM:
You need to update perf_cloud.txt files as per your environment in order to execute the program
Refer Perf-Readme.md for details.

Type following command:
make BUCKET_NAME=${BUCKET_NAME} ACCESS_KEY=${ACCESS_KEY} SECRET_ACCESS_KEY=${SECRET_ACCESS_KEY} N=${TIME} run

where   ${BUCKET_NAME} is the existing/newly created AWS S3 bucket name having required input files, example: mra4sa
	${ACCESS_KEY} is the AWS access key
	${SECRET_ACCESS_KEY} is the AWS secret access key
	${TIME} is scheduled flight minutes, example: 200

