# Makefile for Map Reduce Final Project.
# Joseph Sackett, Chris Willig, Surekha Jadhwani, Akash Singh

# Do not change these.
jar.name = SCAJoop.jar
jar.dep.name = SCAJoop-1.0-jar-with-dependencies.jar
# SCAJoop.jar goes in ascj-bucket. Do not change this value.
aws.bucket.name = ascj-bucket

# Compiles code and builds jar.
build:
	mvn clean compile package
	cp target/${jar.dep.name} ./${jar.name}
	mvn install:install-file -Dfile=${jar.name} -DgroupId=neu.mr -DartifactId=SCAJoop -Dversion=1.0 -Dpackaging=jar -DgeneratePom=true
	aws s3 cp ${jar.name} s3://${aws.bucket.name}/ --grants full=uri=http://acs.amazonaws.com/groups/global/AllUsers
