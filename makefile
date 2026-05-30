.PHONY: kinit_check env_check clean

ENV?=ecs

env_check:
	echo $(HADOOP_HOME)
	echo $(PATH)
	echo $$HADOOP_CONF_DIR

kinit_check:
	@echo "Checking Kerberos"
	@if [ "$(ENV)" != "ecs" ]; then \
		echo "Skipping ECS only steps..."; \
		exit 0; \
	fi; \
	@klist -s || (echo "No valid kerberos token, run kinit" && exit 1)

hdfs_check:
	@echo "Checking HDFS"
	@if [ "$(ENV)" != "ecs" ]; then \
		echo "Skipping ECS only steps..."; \
		exit 0; \
	fi; \
	hdfs dfs -ls / >/dev/null || (echo "Unable to connect to hdfs" && exit 1)

spark_check:
	@echo "Checking Spark"
	@if [ "$(ENV)" != "ecs" ]; then \
		echo "Skipping ECS only steps..."; \
		exit 0; \
	fi ; \
	echo 'val data = spark.range(1, 1000); data.count()' | spark-shell >/dev/null 2>&1 || (echo "Unable to connect to Spark" && exit 1)

# Each script has it's own tree.
# ScriptName/input
# ScriptName/output
# ScriptName/bin
# ScriptName/sentinel
#
# input goes in the input directory
# output will be placed in the output directory
# bin is where the script goes.
# sentinel are where timestamped files are placed in an
# attempt to keep HDFS in sync.

# To use this, take a copy change "SCRIPT" to the script you're adding
# and change the name in _NAME.
#
# To use:
# make clean_SparkWordCount - clean out the HDFS tree and the local sentinels
# make submit_SparkWordCount - prepare HDFS, submit the job and copy the output back
#
# make local_SparkWordCount ENV=local - run the script locally.

SCRIPT_NAME=SparkWordCount
SCRIPT_BIN_DIR=$(SCRIPT_NAME)/bin
SCRIPT_INPUT_DIR=$(SCRIPT_NAME)/input
SCRIPT_OUTPUT_DIR=$(SCRIPT_NAME)/output
SCRIPT_SENTINEL_DIR=$(SCRIPT_NAME)/sentinel

SCRIPT_EXEC=$(SCRIPT_BIN_DIR)/$(SCRIPT_NAME).py
SCRIPT_INPUT=$(wildcard $(SCRIPT_INPUT_DIR)/*)
SCRIPT_OUTPUT=$(SCRIPT_OUTPUT_DIR)/$(SCRIPT_NAME).output

SCRIPT_HDFS_SENTINEL=$(SCRIPT_SENTINEL_DIR)/hdfs_path_created
SCRIPT_PUSH_SENTINEL=$(SCRIPT_SENTINEL_DIR)/input_pushed
SCRIPT_SENTINELS=$(SCRIPT_HDFS_SENTINEL) $(SCRIPT_PUSH_SENTINEL)
SCRIPT_CHECK=$(SCRIPT_NAME)_check

$(SCRIPT_CHECK): | kinit_check hdfs_check spark_check

clean_$(SCRIPT_NAME): | $(SCRIPT_CHECK)
	-hdfs dfs -rm $(SCRIPT_INPUT)
	-hdfs dfs -rm "$(SCRIPT_OUTPUT_DIR)/*"
	-hdfs dfs -rmdir $(SCRIPT_INPUT_DIR)
	-hdfs dfs -rmdir "$(SCRIPT_OUTPUT_DIR)"
	-hdfs dfs -rmdir "$(SCRIPT_NAME)"
	-rm $(SCRIPT_SENTINELS)
	-rm $(SCRIPT_STAGED_OUTPUT_DIR)/*
	-rm $(SCRIPT_STAGED_OUTPUT_DIR)/.*
	-rmdir $(SCRIPT_STAGED_OUTPUT_DIR)

submit_$(SCRIPT_NAME): $(SCRIPT_OUTPUT)


SCRIPT_STAGED_OUTPUT_DIR=$(SCRIPT_NAME)/staged_output
local_$(SCRIPT_NAME):
	echo "LOCAL - Submitting $(SCRIPT_EXEC)"
	-rm $(SCRIPT_STAGED_OUTPUT_DIR)/*
	-rmdir $(SCRIPT_STAGED_OUTPUT_DIR)
	spark-submit --name $(SCRIPT_NAME) \
	  --master "local[*]"  \
	  $(SCRIPT_EXEC) \
	  $(SCRIPT_INPUT) \
	  $(SCRIPT_STAGED_OUTPUT_DIR) 2>&1
	cp $(SCRIPT_STAGED_OUTPUT_DIR)/* $(SCRIPT_OUTPUT_DIR)
	touch $(SCRIPT_OUTPUT)

$(SCRIPT_HDFS_SENTINEL): | $(SCRIPT_CHECK)
	-hdfs dfs -rm $(SCRIPT_INPUT)
	-hdfs dfs -rm "$(SCRIPT_OUTPUT_DIR)/*"
	-hdfs dfs -rmdir $(SCRIPT_INPUT_DIR)
	-mkdir -p $(SCRIPT_SENTINEL_DIR)
	hdfs dfs -mkdir -p $(SCRIPT_INPUT_DIR)
	touch $(SCRIPT_HDFS_SENTINEL)

$(SCRIPT_PUSH_SENTINEL): $(SCRIPT_INPUT) $(SCRIPT_HDFS_SENTINEL) | $(SCRIPT_CHECK)
	@echo "Pushing $?"
	-for i in $(filter-out $(SCRIPT_SENTINEL_DIR)/%, $?) ; do \
	  echo "Removing $$i"; \
	  hdfs dfs -rm $$i; \
	done
	for i in $(filter-out $(SCRIPT_SENTINEL_DIR)/%, $?) ; do \
	  echo "Pushing $$i"; \
	  hdfs dfs -put $$i $$i; \
	done;
	touch $(SCRIPT_PUSH_SENTINEL)

$(SCRIPT_OUTPUT): $(SRIPT_HDFS_SENTINEL) $(SCRIPT_PUSH_SENTINEL) $(SCRIPT_EXEC) | $(SCRIPT_CHECK)
	echo "Submitting $(SCRIPT_EXEC)"
	-hdfs dfs -rm "$(SCRIPT_OUTPUT_DIR)/*"
	-hdfs dfs -rmdir $(SCRIPT_OUTPUT_DIR)
	-mkdir -p $(SCRIPT_OUTPUT_DIR)
	spark-submit --name $(SCRIPT_NAME) \
	  --master yarn \
	  --deploy-mode cluster \
	  --conf spark.yarn.appMasterEnv.JAVA_HOME=/usr/lib/jvm/java-21-openjdk \
	  --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-21-openjdk \
	  $(SCRIPT_EXEC) \
	  $(SCRIPT_INPUT) \
	  $(SCRIPT_OUTPUT_DIR) 2>&1
	-rm $(SCRIPT_OUTPUT_DIR)/*
	hdfs dfs -get "$(SCRIPT_OUTPUT_DIR)/*" $(SCRIPT_OUTPUT_DIR)
	touch $(SCRIPT_OUTPUT)

include $(ENV)_hadoop_env
export $(shell sed 's/=.*//' $(ENV)_hadoop_env)
include $(ENV)_spark_env
export $(shell sed 's/=.*//' $(ENV)_spark_env)
