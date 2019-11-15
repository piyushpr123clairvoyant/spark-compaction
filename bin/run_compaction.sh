#!/usr/bin/env bash

HELP_STR="""

Required Arguments:
    1. --input-path
    2. --output-path
    3. --input-compression
    4. --input-serialization

Optional Arguments:
    1. --output-compression(Taken as input compression if not provided)
    2. --output-serialization(Taken as input serialization if not provided)
    3. --compaction-strategy(Taken as default if not provided)

Please provide the arguments as follows
Example 1: Strategy: size_range
    sh run_compaction.sh --input-path {input_path} --output-path {output_path} --input-compression [none snappy gzip bz2 lzo] --input-serialization [text parquet avro] --output-compression [none snappy gzip bz2 lzo] --output-serialization [text parquet avro] --compaction-strategy size_range
Example 2: Strategy: Default
    sh run_compaction.sh --input-path {input_path} --output-path {output_path} --input-compression [none snappy gzip bz2 lzo] --input-serialization [text parquet avro] --output-compression [none snappy gzip bz2 lzo] --output-serialization [text parquet avro] --compaction-strategy default
        (or)
    sh run_compaction.sh --input-path {input_path} --output-path {output_path} --input-compression [none snappy gzip bz2 lzo] --input-serialization [text parquet avro] --output-compression [none snappy gzip bz2 lzo] --output-serialization [text parquet avro]
"""

POSITIONAL=()
while [[ $# -gt 0 ]]
do
key="$1"

case $key in
    -ip|--input-path)
    INPUT_PATH="$2"
    shift # past argument
    shift # past value
    ;;
    -op|--output-path)
    OUTPUT_PATH="$2"
    shift # past argument
    shift # past value
    ;;
    -ic|--input-compression)
    INPUT_COMPRESSION="$2"
    shift # past argument
    shift # past value
    ;;
    -oc|--output-compression)
    OUTPUT_COMPRESSION="$2"
    shift # past argument
    shift # past value
    ;;
    -is|--input-serialization)
    INPUT_SERIALIZATION="$2"
    shift # past argument
    shift # past value
    ;;
    -os|--output-serialization)
    OUTPUT_SERIALIZATION="$2"
    shift # past argument
    shift # past value
    ;;
    -cs|--compaction-strategy)
    COMPACTION_STRATEGY="$2"
    shift # past argument
    shift # past value
    ;;
esac
done
set -- "${POSITIONAL[@]}" # restore positional parameters

if [[ -z "${INPUT_PATH}" || -z "${OUTPUT_PATH}" || -z "${INPUT_COMPRESSION}" || -z ${INPUT_SERIALIZATION} ]]; then
    echo "Please provide all the required arguments to proceed with the compaction Job. Please provide the arguments as follows"
    echo "${HELP_STR}"
    exit 0
fi

if [[ -z "${OUTPUT_COMPRESSION}" ]]; then
    OUTPUT_COMPRESSION=${INPUT_COMPRESSION}
fi

if [[ -z "${OUTPUT_SERIALIZATION}" ]]; then
    OUTPUT_SERIALIZATION=${INPUT_SERIALIZATION}
fi

if [[ -z "${COMPACTION_STRATEGY}" ]]; then
    COMPACTION_STRATEGY="default"
fi

BIN_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LIB_DIR="${BIN_DIR}/../lib"
CONF_DIR="${BIN_DIR}/../conf"
JAR_FILE_LOCATION="${LIB_DIR}/spark-compaction-1.0.0-jar-with-dependencies.jar"
APPLICATION_CONF_FILE="${CONF_DIR}/application_configs.json"

SPARK_APP_NAME=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['spark']['app_name'];"`
SPARK_MASTER=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['spark']['master'];"`
SPARK_EXECUTOR_INSTANCES=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['spark']['spark_executor_instances'];"`
SPARK_EXECUTOR_MEMORY=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['spark']['spark_executor_memory'];"`
SPARK_EXECUTOR_CORES=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['spark']['spark_executor_cores'];"`
KEYTAB_LOCATION=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['kerberos']['keytab'];"`
KERBEROS_PRINCIPAL=`cat "$APPLICATION_CONF_FILE" | python -c "import json,sys;obj=json.load(sys.stdin);print obj['kerberos']['principal'];"`

SPARK_SUBMIT_CMD="spark2-submit"
LOG_FILE="${BIN_DIR}/../logs/${SPARK_APP_NAME}__started_at_`date '+%Y%m%d%H%M%S'`"

echo ${SPARK_MASTER}

if [[ "${SPARK_MASTER}" = "yarn-client" ]]; then
    echo "Launching Spark Streaming Application in Yarn Client Mode"
    SPARK_SUBMIT_STARTUP_CMD="nohup ${SPARK_SUBMIT_CMD} --master yarn --deploy-mode client --class com.apache.bigdata.spark_compaction.Compact --num-executors ${SPARK_EXECUTOR_INSTANCES} --executor-memory ${SPARK_EXECUTOR_MEMORY} --executor-cores ${SPARK_EXECUTOR_CORES} --driver-class-path ${CONF_DIR}:${JAR_FILE_LOCATION} ${JAR_FILE_LOCATION} --input-path ${INPUT_PATH} --output-path ${OUTPUT_PATH} --input-compression ${INPUT_COMPRESSION} --input-serialization ${INPUT_SERIALIZATION} --output-compression ${OUTPUT_COMPRESSION} --output-serialization ${OUTPUT_SERIALIZATION} --compaction-strategy ${COMPACTION_STRATEGY} &> ${LOG_FILE} &"
elif [[ "${SPARK_MASTER}" = "yarn-cluster" ]]; then
    echo "Launching Spark Streaming Application in Yarn Cluster Mode"
    SPARK_SUBMIT_STARTUP_CMD="nohup ${SPARK_SUBMIT_CMD} --keytab ${KEYTAB_LOCATION} --principal ${KERBEROS_PRINCIPAL} --master yarn --deploy-mode cluster --class com.apache.bigdata.spark_compaction.Compact --num-executors ${SPARK_EXECUTOR_INSTANCES} --executor-memory ${SPARK_EXECUTOR_MEMORY} --executor-cores ${SPARK_EXECUTOR_CORES} --name ${SPARK_APP_NAME} --files ${CONF_DIR}/application.json ${JAR_FILE_LOCATION} --input-path ${INPUT_PATH} --output-path ${OUTPUT_PATH} --input-compression ${INPUT_COMPRESSION} --input-serialization ${INPUT_SERIALIZATION} --output-compression ${OUTPUT_COMPRESSION} --output-serialization ${OUTPUT_SERIALIZATION} --compaction-strategy ${COMPACTION_STRATEGY} &> ${LOG_FILE} &"
fi

echo "executing: ${SPARK_SUBMIT_STARTUP_CMD}"
eval ${SPARK_SUBMIT_STARTUP_CMD}
echo "PID: $!"
