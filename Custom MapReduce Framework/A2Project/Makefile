# Makefile for Map Reduce A2 project.
# Joseph Sackett, Chris Willig

# Customize these paths and credentials for your environment.
#############################################################
local.input=input
local.output=output
#local.input=s3://mrascj/fullinput
#local.output=s3://mrascj/fulloutput
#local.input=/home/clw/6240/all
aws.region=us-east-1a
#aws.region=us-east-1d
#aws.a2.bucket.name=mr-median2
aws.a2.bucket.name=mrascj
aws.input.dir=fullinput
aws.output.dir=output
#############################################################

# These are only for customizing development (not testing).
scajoop.home=../MRProj
jar.name=A2Proj.jar
driver.app=flight.app.A2Main
# A2Proj.jar goes in ascj-bucket. Do not change this value.
aws.bucket.name=ascj-bucket

export SCAJOOP_HOME:= ${scajoop.home}
export PATH:= ${scajoop.home}:${PATH}

# Build SCAJoop
build-scajoop:
	(cd ../MRProj && make build)
	
# Compiles code and builds jar.
jar: build-scajoop
	mvn clean compile package
	cp target/${jar.name} ./${jar.name}

# Deletes local output dir.
clean-local-output:
	rm -rf ${local.output}
	mkdir ${local.output}

# Run this A2 app on local SCAJoop.
run-local: jar clean-local-output
	${scajoop.home}/scajoop ${jar.name} ${driver.app} ${local.input} ${local.output}

# Upload jar to shared S3 bucket.
upload-jar-aws:
	aws s3 cp ${jar.name} s3://${aws.bucket.name}/ --grants full=uri=http://acs.amazonaws.com/groups/global/AllUsers

# Delete S3 output dir.
delete-output-aws:
	aws s3 rm -r -f s3://${aws.a2.bucket.name}/${aws.output.dir} --recursive

# Run this A2 app on AWS EC2 cluster SCAJoop.
run-cloud: jar upload-jar-aws delete-output-aws
	${scajoop.home}/scajoop ${jar.name} ${driver.app} s3://${aws.a2.bucket.name}/${aws.input.dir} s3://${aws.a2.bucket.name}/${aws.output.dir}

# Create S3 bucket.
make-bucket:
	aws s3api create-bucket --bucket ${aws.a2.bucket.name} --grant-full-control 'uri="http://acs.amazonaws.com/groups/global/AllUsers"'

# Upload data to S3 input dir.
upload-input-aws: make-bucket
	aws s3 sync ${local.input} s3://${aws.a2.bucket.name}/${aws.input.dir} --grants full=uri=http://acs.amazonaws.com/groups/global/AllUsers

# Download output from S3.
download-output-aws:
	aws s3 sync s3://${aws.a2.bucket.name}/${aws.output.dir} ${local.output}

# Creates plot.png image of airline prices per month results.
graph:
	Rscript scripts/library_check.r   # Make sure required R libraries are installed.
	if [ -f $(local.output)/out ]; then rm $(local.output)/out; fi;   # Remove an old out file (if exists)
	find $(local.output) -maxdepth 1 -type f -name 'part-r-*' -print0 | sort -z | xargs -0 cat -- >> $(local.output)/out   # Concatenate part-r-* files into one file called 'out'
	sed -i '1 i\Airline,January,February,March,April,May,June,July,August,September,October,November,December,Active2015,TotalFlights' $(local.output)/out   # Create header for out file.
	Rscript scripts/graph.r $(local.output)/out   # Create a graph from reading in out file. Graph is named plot.png

# Generates the final report.
report: graph
	pdflatex report.tex
	rm report.aux
	rm report.log

# Packages final distribution.
distro:
	rm -rf build
	mkdir build
	mkdir build/deliv
	mkdir build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop
	mkdir build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/MRProj
	cp -r ../MRProj/src build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/MRProj	
	cp ../MRProj/pom.xml build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/MRProj
	cp ../MRProj/log4j2.xml build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/MRProj
	cp ../MRProj/Makefile build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/MRProj
	cp ../MRProj/README build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/MRProj
	cp ../MRProj/scajoop build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/MRProj
	mkdir build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/A2Project
	cp -r src build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/A2Project
	cp -r scripts build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/A2Project
	cp pom.xml build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/A2Project
	cp cluster_conf build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/A2Project
	cp report.tex build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/A2Project
	cp report.pdf build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/A2Project
	cp SCAJoopArch.png build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/A2Project
	cp plot.png build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/A2Project
	cp Makefile build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/A2Project
	cp README build/deliv/Jadhwani-Sackett-Singh-Willig-SCAJoop/A2Project
	tar -czf Jadhwani-Sackett-Singh-Willig-SCAJoop.tar.gz -C build/deliv Jadhwani-Sackett-Singh-Willig-SCAJoop

