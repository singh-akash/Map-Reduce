build:
	chmod 777 createJar.sh
	./createJar.sh

initS3:
	aws s3 mb s3://${BUCKET_NAME}
	aws s3 sync s3://mrclassvitek/data s3://${BUCKET_NAME}/input
	aws s3 rm s3://${BUCKET_NAME}/output/ --recursive
		
run:
	aws s3 cp job.jar s3://${BUCKET_NAME}/
	aws s3 rm s3://${BUCKET_NAME}/output/ --recursive
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=s3n://${BUCKET_NAME}/job.jar -kind=cloud -main=neu.mr.A4CarrierMeanAndN -arguments="-input=s3://${BUCKET_NAME}/input -output=s3://${BUCKET_NAME}/output" -fsroot=s3n://${BUCKET_NAME}/ -awskeyid="${ACCESS_KEY}" -awskey="${SECRET_ACCESS_KEY}" -results=results.csv -input=s3n://${BUCKET_NAME}/input -output=s3n://${BUCKET_NAME}/output -name=mean -properties=perf_cloud.txt		
	aws s3 sync s3://${BUCKET_NAME}/output/ ./output
	RScript A4_Linear_Regression.R output	
	aws s3 cp LM.csv s3://${BUCKET_NAME}/
	aws s3 rm s3://${BUCKET_NAME}/output2/ --recursive
	java -cp uber-perf-0.0.1.jar neu.perf.App -num=1 -jar=s3n://${BUCKET_NAME}/job.jar -kind=cloud -main=neu.mr.A4MedianComputer -arguments="-rInput=s3://${BUCKET_NAME}/LM.csv -time=${N} ${BUCKET_NAME}" -fsroot=s3n://${BUCKET_NAME}/ -awskeyid="${ACCESS_KEY}" -awskey="${SECRET_ACCESS_KEY}" -results=results.csv -input=s3n://${BUCKET_NAME}/input -output=s3n://${BUCKET_NAME}/output2 -name=median -properties=perf_cloud.txt	
	aws s3 sync s3://${BUCKET_NAME}/output2/ ./output2
	RScript A4_Plot_Graph.R ./output2
	