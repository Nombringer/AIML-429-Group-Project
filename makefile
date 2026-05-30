
# Environment variable loading and exporting.
ENV?=ecs


.PHONY: kinit_check env_check clean

# Environment checks to ensure values are populated and working.
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
	klist -s || (echo "No valid kerberos token, run kinit" && exit 1)


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
	-rm $(SCRIPT_OUTPUT_DIR)/*
	-rm $(SCRIPT_OUTPUT_DIR)/.*
	-rmdir $(SCRIPT_OUTPUT_DIR)
	-rm $(SCRIPT_STAGED_OUTPUT_DIR)/*
	-rm $(SCRIPT_STAGED_OUTPUT_DIR)/.*
	-rmdir $(SCRIPT_STAGED_OUTPUT_DIR)

submit_$(SCRIPT_NAME): $(SCRIPT_OUTPUT)


SCRIPT_STAGED_OUTPUT_DIR=$(SCRIPT_NAME)/staged_output
local_$(SCRIPT_NAME):
	echo "LOCAL - Submitting $(SCRIPT_EXEC)"
	-rm $(SCRIPT_STAGED_OUTPUT_DIR)/*
	-rm $(SCRIPT_STAGED_OUTPUT_DIR)/.*
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

$(SCRIPT_OUTPUT): $(SCRIPT_HDFS_SENTINEL) $(SCRIPT_PUSH_SENTINEL) $(SCRIPT_EXEC) | $(SCRIPT_CHECK)
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

DT_NAME=DecisionTree
DT_BIN_DIR=$(DT_NAME)/bin
DT_INPUT_DIR=$(DT_NAME)/input
DT_OUTPUT_DIR=$(DT_NAME)/output
DT_SENTINEL_DIR=$(DT_NAME)/sentinel

DT_EXEC=$(DT_BIN_DIR)/$(DT_NAME).py
DT_INPUT=$(wildcard $(DT_INPUT_DIR)/*)
DT_OUTPUT=$(DT_OUTPUT_DIR)/$(DT_NAME).output

DT_HDFS_SENTINEL=$(DT_SENTINEL_DIR)/hdfs_path_created
DT_PUSH_SENTINEL=$(DT_SENTINEL_DIR)/input_pushed
DT_SENTINELS=$(DT_HDFS_SENTINEL) $(DT_PUSH_SENTINEL)
DT_CHECK=$(DT_NAME)_check
DT_NUM_RUNS=10

$(DT_CHECK): | kinit_check hdfs_check spark_check

clean_$(DT_NAME): | $(DT_CHECK)
	-hdfs dfs -rm $(DT_INPUT)
	-hdfs dfs -rm "$(DT_OUTPUT_DIR)/*"
	-hdfs dfs -rmdir $(DT_INPUT_DIR)
	-hdfs dfs -rmdir "$(DT_OUTPUT_DIR)"
	-hdfs dfs -rmdir "$(DT_NAME)"
	-rm $(DT_SENTINELS)
	-rm $(DT_OUTPUT_DIR)/*
	-rm $(DT_OUTPUT_DIR)/.*
	-rmdir $(DT_OUTPUT_DIR)
	-rm $(DT_STAGED_OUTPUT_DIR)/*
	-rm $(DT_STAGED_OUTPUT_DIR)/.*
	-rmdir $(DT_STAGED_OUTPUT_DIR)

submit_$(DT_NAME): $(DT_OUTPUT)


DT_STAGED_OUTPUT_DIR=$(DT_NAME)/staged_output
local_$(DT_NAME):
	echo "LOCAL - Submitting $(DT_EXEC)"
	-rm $(DT_STAGED_OUTPUT_DIR)/*
	-rm $(DT_STAGED_OUTPUT_DIR)/.*
	-rmdir $(DT_STAGED_OUTPUT_DIR)
	spark-submit --name $(DT_NAME) \
	  --master "local[*]"  \
	  $(DT_EXEC) \
	  $(DT_NUM_RUNS) \
	  $(DT_INPUT) \
	  $(DT_STAGED_OUTPUT_DIR) 2>&1
	-mkdir -p $(DT_OUTPUT_DIR)
	cp $(DT_STAGED_OUTPUT_DIR)/* $(DT_OUTPUT_DIR)
	cat $(DT_OUTPUT_DIR)/*.csv
	touch $(DT_OUTPUT)


$(DT_HDFS_SENTINEL): | $(DT_CHECK)
	-hdfs dfs -rm $(DT_INPUT)
	-hdfs dfs -rm "$(DT_OUTPUT_DIR)/*"
	-hdfs dfs -rmdir $(DT_INPUT_DIR)
	-mkdir -p $(DT_SENTINEL_DIR)
	hdfs dfs -mkdir -p $(DT_INPUT_DIR)
	touch $(DT_HDFS_SENTINEL)

$(DT_PUSH_SENTINEL): $(DT_INPUT) $(DT_HDFS_SENTINEL) | $(DT_CHECK)
	@echo "Pushing $?"
	-for i in $(filter-out $(DT_SENTINEL_DIR)/%, $?) ; do \
	  echo "Removing $$i"; \
	  hdfs dfs -rm $$i; \
	done
	for i in $(filter-out $(DT_SENTINEL_DIR)/%, $?) ; do \
	  echo "Pushing $$i"; \
	  hdfs dfs -put $$i $$i; \
	done;
	touch $(DT_PUSH_SENTINEL)

$(DT_OUTPUT): $(DT_HDFS_SENTINEL) $(DT_PUSH_SENTINEL) $(DT_EXEC) | $(DT_CHECK)
	echo "Submitting $(DT_EXEC)"
	-hdfs dfs -rm "$(DT_OUTPUT_DIR)/*"
	-hdfs dfs -rmdir $(DT_OUTPUT_DIR)
	-mkdir -p $(DT_OUTPUT_DIR)
	spark-submit --name $(DT_NAME) \
	  --master yarn \
	  --deploy-mode cluster \
	  --conf spark.yarn.appMasterEnv.JAVA_HOME=/usr/lib/jvm/java-21-openjdk \
	  --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-21-openjdk \
	  $(DT_EXEC) \
	  $(DT_NUM_RUNS) \
	  $(DT_INPUT) \
	  $(DT_OUTPUT_DIR) 2>&1
	-rm $(DT_OUTPUT_DIR)/*
	hdfs dfs -get "$(DT_OUTPUT_DIR)/*" $(DT_OUTPUT_DIR)
	cat $(DT_OUTPUT_DIR)/*.csv
	touch $(DT_OUTPUT)

#Logistic Regression, currently local only

LR_NAME=LogisticRegression
LR_BIN_DIR=$(LR_NAME)/bin
LR_INPUT_DIR=$(LR_NAME)/input
LR_OUTPUT_DIR=$(LR_NAME)/output
LR_SENTINEL_DIR=$(LR_NAME)/sentinel
LR_EXEC=$(LR_BIN_DIR)/$(LR_NAME).py
LR_INPUT=$(wildcard $(LR_INPUT_DIR)/*)
LR_OUTPUT=$(LR_OUTPUT_DIR)/$(LR_NAME).output
LR_HDFS_SENTINEL=$(LR_SENTINEL_DIR)/hdfs_path_created
LR_PUSH_SENTINEL=$(LR_SENTINEL_DIR)/input_pushed
LR_SENTINELS=$(LR_HDFS_SENTINEL) $(LR_PUSH_SENTINEL)
LR_CHECK=$(LR_NAME)_check
LR_NUM_RUNS=10

$(LR_CHECK): | kinit_check hdfs_check spark_check

LR_STAGED_OUTPUT_DIR=$(LR_NAME)/staged_output

local_$(LR_NAME):
	echo "LOCAL - Submitting $(LR_EXEC)"
	-rm $(LR_STAGED_OUTPUT_DIR)/*
	-rm $(LR_STAGED_OUTPUT_DIR)/.*
	-rmdir $(LR_STAGED_OUTPUT_DIR)
	spark-submit --name $(LR_NAME) \
		  --master "local[*]"  \
		  $(LR_EXEC) \
		  $(LR_NUM_RUNS) \
		  $(LR_INPUT) \
		  $(LR_STAGED_OUTPUT_DIR) 2>&1
	-mkdir -p $(LR_OUTPUT_DIR)
	cp $(LR_STAGED_OUTPUT_DIR)/* $(LR_OUTPUT_DIR)
	cat $(LR_OUTPUT_DIR)/*.csv
	touch $(LR_OUTPUT)

clean_LogisticRegression:
	echo "Cleaning LogisticRegression generated files"
	-rm -rf $(LR_STAGED_OUTPUT_DIR)
	-rm -rf $(LR_OUTPUT_DIR)
	-rm -rf $(LR_SENTINEL_DIR)

include $(ENV)_hadoop_env
export $(shell sed 's/=.*//' $(ENV)_hadoop_env)

include $(ENV)_spark_env
export $(shell sed 's/=.*//' $(ENV)_spark_env)

