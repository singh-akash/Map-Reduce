format: 
	hdfs namenode -format

hstart:
	start-dfs.sh
	start-yarn.sh
	mr-jobhistory-daemon.sh start historyserver

hstop:
	mr-jobhistory-daemon.sh stop historyserver 
	stop-yarn.sh
	stop-dfs.sh

dir:
	hadoop fs -mkdir -p /user/${USER}
	hadoop fs -mkdir -p /user/${USER}/input

benchmark:
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind="single-thread" -main=neu.mr.Plain -arguments="-input=${INPUT_DIR} -query=AvgPrice" -input=${INPUT_DIR} -output=/tmp -results=results.csv -name=mean -properties=perf_plain_seq.txt
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind="single-thread" -main=neu.mr.Plain -arguments="-input=${INPUT_DIR} -query=Median" -input=${INPUT_DIR} -output=/tmp -results=results.csv -name=median -properties=perf_plain_seq.txt
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind="single-thread" -main=neu.mr.Plain -arguments="-input=${INPUT_DIR} -query=FastMedian" -input=${INPUT_DIR} -output=/tmp -results=results.csv -name=fast-median -properties=perf_plain_seq.txt
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind="multithread" -main=neu.mr.Plain -arguments="-p -input=${INPUT_DIR} -query=AvgPrice" -input=${INPUT_DIR} -output=/tmp -results=results.csv -name=mean -properties=perf_plain_par.txt
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind="multithread" -main=neu.mr.Plain -arguments="-p -input=${INPUT_DIR} -query=Median" -input=${INPUT_DIR} -output=/tmp -results=results.csv -name=median -properties=perf_plain_par.txt
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind="multithread" -main=neu.mr.Plain -arguments="-p -input=${INPUT_DIR} -query=FastMedian" -input=${INPUT_DIR} -output=/tmp -results=results.csv -name=fast-median -properties=perf_plain_par.txt
	make format
	make hstart
	make INPUT_DIR=${INPUT_DIR} dir
	hadoop fs -put ${INPUT_DIR}/* input
	-hadoop fs -rm -r output
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind=local -main=neu.mr.A2Mean -arguments="-input=input -output=output" -input=input -output=output  -results=results.csv -name=mean -properties=perf_local.txt
	-hadoop fs -rm -r output
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind=local -main=neu.mr.A2Median -arguments="-input=input -output=output" -input=input -output=output  -results=results.csv -name=median -properties=perf_local.txt
	-hadoop fs -rm -r output
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=job.jar -kind=local -main=neu.mr.A2FastMedian -arguments="-input=input -output=output" -input=input -output=output  -results=results.csv -name=fast-median -properties=perf_local.txt
	make hstop
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=s3n://${BUCKET}/job.jar -kind=cloud -main=neu.mr.A2Mean -arguments="-input=s3://${BUCKET}/input -output=s3://${BUCKET}/output" -fsroot=s3n://${BUCKET}/ -awskeyid="${ACCESS_KEY}" -awskey="${SECRET_ACCESS_KEY}" -results=results.csv -input=s3n://${BUCKET}/input -output=s3n://${BUCKET}/output -name=mean -properties=perf_cloud.txt
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=s3n://${BUCKET}/job.jar -kind=cloud -main=neu.mr.A2Median -arguments="-input=s3://${BUCKET}/input -output=s3://${BUCKET}/output" -fsroot=s3n://${BUCKET}/ -awskeyid="${ACCESS_KEY}" -awskey="${SECRET_ACCESS_KEY}" -results=results.csv -input=s3n://${BUCKET}/input -output=s3n://${BUCKET}/output -name=median -properties=perf_cloud.txt
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=s3n://${BUCKET}/job.jar -kind=cloud -main=neu.mr.A2FastMedian -arguments="-input=s3://${BUCKET}/input -output=s3://${BUCKET}/output" -fsroot=s3n://${BUCKET}/ -awskeyid="${ACCESS_KEY}" -awskey="${SECRET_ACCESS_KEY}" -results=results.csv -input=s3n://${BUCKET}/input -output=s3n://${BUCKET}/output -name=fast-median -properties=perf_cloud.txt
	RScript A3.R