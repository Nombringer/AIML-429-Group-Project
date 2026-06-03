* README

group: 22
authors: Bex Campbell-Redl, Fabian Day, Jason Pollock
email: campberebe3@myvuw.ac.nz, dayfabi@myvuw.ac.nz, pollocjaso@myvuw.ac.nz

How to Install and Run Group22's Decision Tree and Logistic Regression code on the Hadoop cluster

Important note on Makefile:
This project uses a makefile. A makefile is the most efficient way to ensure all required setup, admin and installation tasks have been successfully executed before attempting to run code. This is particularly important when working in a group to ensure everyone has the same setup when running or writing code. The makefile for this includes hadoop setup, java8, kinit and sparkclasspath setup. Please read the makefile for further information.

Part 1: Setup & Environment
1. Ensure that you have a user directory on the HDFS cluster (if you do not have one, create one)
2. Download the submitted Tarball into a folder on your ECS desktop
3. Extract the tarball (alternatively you can clone our git repo)
4. cd into the extracted folder

Part 2: Understanding the Directory Layout
Directories:

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

Part 3: How to run the code

The makefile supports two execution modes, ecs and local, picked via a ENV environment variable. "ecs" is the default.
The makefile will ensure proper configured access to kerberos, HDFS and Spark before attempting to launch the job.

This is configured to run via makefile. To run a variant: 

Part 3a: Decision Tree

$ make submit_DecisionTree

The output will be placed in:
DecisionTree/output
$ cat DecisionTree/output/*.csv
To clean the files when complete:
$ make clean_DecisionTree

Part 3b: Logistic Regression

$ make submit_LogisticRegression

The output will be placed in:
LogisticRegression/output
cat LogisticRegression/output/*.csv
To clean the files when complete:
$ make clean_LogisticRegression

Optional Variant modes:
Runs spark wordcount
$ make submit_SparkWordCount
To Run all modes
$ make all

The output(s) will be placed in:

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

