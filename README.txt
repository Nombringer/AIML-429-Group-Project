* README

group: 22
authors: Bex Campbell-Redl, Fabian Day, Jason Pollock
email: campberebe3@myvuw.ac.nz, dayfabi@myvuw.ac.nz, pollocjaso@myvuw.ac.nz

* How to install

If you are reading this, it is installed.

* Directory Layout

SparkWordCount/input - the dataset extracted and ready for use
SparkWordCount/bin - where the code is placed
SparkWordCount/staged_output - where local runs place their output instead of HDFS
DecisionTree/input - the dataset extracted and ready for use
DecisionTree/bin - where the code is placed
DecisionTree/staged_output - where local runs place their output instead of HDFS
LogisticRegression/input - the dataset extracted and ready for use
LogisticRegression/bin - where the code is placed
LogisticRegression/staged_output - where local runs place their output instead of HDFS

ecs_hadoop_env - the ECS cluster Hadoop makefile variables
ecs_spart_env - the ECS cluster Spark makefile variables
local_hadoop_env - the Hadoop makefile variables for local execution
local_spark_env - the Hadoop makefile variables for local execution

* How to run

The makefile supports two execution modes, ecs and local, picked via a ENV environment variable. "ecs" is the default.

The makefile will ensure proper configured access to kerberos, HDFS and Spark before attempting to launch the job.

This is configured to run via makefile. To run a variant: 

$ make submit_DecisionTree
$ make submit_SparkWordCount
$ make submit_LogisticRegression

or optionally

$ make all

The output will be placed in:

DecisionTree/output
LogisticRegression/output
SparkWordCount/output

$ cat DecisionTree/output/*.csv
$ cat LogisticRegression/output/*.csv
$ cat SparkWordCount/output/*.csv

To clean the files when complete:

$ make clean_DecisionTree
$ make clean_LogisticRegression
$ make clean_SparkWordCount

or

$ make clean

