#seperate control
#e.x, in order to build jar file, : make -f make.mk build

spark="/Users/yzh/Desktop/cour/parallel/spark-2.3.0-bin-hadoop2.7"
job.name=TestScala
local.master=local[4]
app.name=Wiki Page Rank
jar.name=spark_2.11-0.1.jar
local.input=input
local.output=output
num.iter=10
k=100
aws.input=workFold/input
aws.output=
aws.bucket.name=michaelyangcs
aws.release.label=emr-5.11.1
aws.instance.type=m4.large
aws.num.nodes=11
aws.log.dir=log

awsoutput="s3://michaelyangcs/workFold/output10"
localout="/Users/yzh/Desktop/cour/parallel/RankOutput"

.PHONY:build
build:
	cd ${project}; sbt package;

.PHONY:standalone
standalone:
	cd ${spark}; bin/spark-submit --class ${job.name} --master ${local.master} --name "${app.name}" ${jar.name} ${local.input} ${local.output} ${num.iter} ${k}


.PHONY:awsrun
awsrun:
	aws emr create-cluster \
		--name "Wiki Spark Cluster" \
		--release-label ${aws.release.label} \
		--instance-groups '[{"InstanceCount":${aws.num.nodes},"InstanceGroupType":"CORE","InstanceType":"${aws.instance.type}"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"${aws.instance.type}"}]' \
	    --applications Name=Hadoop Name=Spark \
		--steps Type=CUSTOM_JAR,Name="${app.name}",Jar="command-runner.jar",ActionOnFailure=TERMINATE_CLUSTER,Args=["spark-submit","--deploy-mode","cluster","--class","${job.name}","s3://${aws.bucket.name}/${jar.name}","s3://${aws.bucket.name}/${aws.input}","s3://${aws.bucket.name}/${aws.output}","${num.iter}","${k}"] \
		--log-uri s3://${aws.bucket.name}/${aws.log.dir} \
		--service-role EMR_DefaultRole \
		--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=subnet-520b7f0f \
		--region us-east-1 \
		--enable-debugging \
		--auto-terminate

.PHONY:sync
sync:
	aws s3 sync ${awsoutput} ${localout}


.PHONY:move
move:
	aws s3 mv s3://michaelyangcs/input s3://michaelyangcs/workFold/input --recursive

