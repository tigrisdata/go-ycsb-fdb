#!/usr/bin/env bash

# Entrypoint script to run the workload

KEY_PREFIX=$(echo $RANDOM | md5sum | awk '{print $1}' | cut -c -6)
export KEY_PREFIX

TEST_DB="${TEST_DB:-ycsb_tigris}"
RECORDCOUNT=${RECORDCOUNT:-5000}
OPERATIONCOUNT=${OPERATIONCOUNT:-1000000000}
READALLFIELDS=${READALLFIELDS:-true}
READPROPORTION=${READPROPORTION:-0.4}
UPDATEPROPORTION=${UPDATEPROPORTION:-0.4}
SCANPROPORTION=${SCANPROPORTION:-0.2}
INSERTPROPORTION=${INSERTPROPORTION:-0}
REQUESTDISTRIBUTION=${REQUESTDISTRIBUTION:-uniform}
LOADTHREADCOUNT=${LOADTHREADCOUNT:-1}
RUNTHREADCOUNT=${RUNTHREADCOUNT:-1}
# Run mode, single for repeating a single benchmark run with the same configuration, threaded to run
# in different thread configurations
RUNMODE=${RUNMODE:-single}
RUNTHREADCONF=${RUNTHREADCONF:-"1 2 4 8 16 32 64"}
RUNTHREADDURATION=${RUNTHREADDURATION:-"1h"}
RUNTHREADSLEEPINTERVAL=${RUNTHREADSLEEPINTERVAL:-30}
STARTWITHLOAD=${STARTWITHLOAD:-1}
ENGINE=${ENGINE:-"foundationdb"}
FDB_CLUSTER_FILE=${FDB_CLUSTER_FILE:-"/mnt/fdb-config-volume/cluster-file"}
FDB_API_VERSION=${FDB_API_VERSION:-730}
FIELDLENGTH=${FIELDLENGTH:-100}
FIELDCOUNT=${FIELDCOUNT:-10}
MAXSCANLENGTH=${MAXSCANLENGTH:-1000}
SCANLENGTHDISTRIBUTION=${SCANLENGTHDISTRIBUTION:-uniform}
FDB_USE_CACHED_READ_VERSION=${FDB_USE_CACHED_READ_VERSION:-false}
FDB_VERSION_CACHE_TIME=${FDB_VERSION_CACHE_TIME:-2s}
BENCHMARK_NAME_PREFIX=${BENCHMARK_NAME_PREFIX:-"UnnamedBenchmark"}
BATCH_SIZE=${BATCH_SIZE:-1}
INSERT_ORDER=${INSERT_ORDER:-"hashed"}
REQUEST_DISTRIBUTION=${REQUEST_DISTRIBUTION:-"zipfian"}
S3_BUCKET=${S3_BUCKET:-"sample_bucket_name"}
S3_ENDPOINT=${S3_ENDPOINT:-"https://sample.url"}
AWS_ACCESS_KEY_ID=${AWS_ACCESS_KEY_ID:-"supply_access_key_through_secret"}
AWS_SECRET_ACCESS_KEY=${AWS_SECRET_ACCESS_KEY:-"supply_secret_key_through_secret"}
S3_USE_PATH_STYLE=${S3_USE_PATH_STYLE:-"true"}
STOP_AFTER_LOAD=${STOP_AFTER_LOAD:-"false"}
SLEEP_INTERVAL_AFTER_STOP=${SLEEP_INTERVAL_AFTER_STOP:-"14400"}

function benchmark_fdb() {
  echo "Benchmark start (foundationdb)"
	echo "Using FDB_CLUSTER_FILE ${FDB_CLUSTER_FILE}"
	echo "Using FDB_API_VERSION ${FDB_API_VERSION}"
	echo "Using LOADTHREADCOUNT ${LOADTHREADCOUNT}"
	echo "Using RUNTHREADCOUNT ${RUNTHREADCOUNT}"
	echo "Using RUNTHREADDURATION ${RUNTHREADDURATION}"
	echo "Using cached read version ${FDB_USE_CACHED_READ_VERSION}, version cache time: ${FDB_VERSION_CACHE_TIME}"

  WORKLOAD_FDB="recordcount=${RECORDCOUNT}
operationcount=${OPERATIONCOUNT}
workload=core

readallfields=${READALLFIELDS}

readproportion=${READPROPORTION}
updateproportion=${UPDATEPROPORTION}
scanproportion=${SCANPROPORTION}
insertproportion=${INSERTPROPORTION}

requestdistribution=${REQUESTDISTRIBUTION}
maxscanlength=${MAXSCANLENGTH}
scanlengthdistribution=${SCANLENGTHDISTRIBUTION}
batch.size=${BATCH_SIZE}
"

  echo "${WORKLOAD_FDB}" > workloads/dynamic

	if [ ${STARTWITHLOAD} -gt 0 ]
	then
		echo "Loading new database"
		export BENCHMARK_NAME="${BENCHMARK_NAME_PREFIX}-load"
    ${BIN_PATH}/go-ycsb load foundationdb -p keyprefix="${KEY_PREFIX}" -p fdb.clusterfile="${FDB_CLUSTER_FILE}" -p fdb.apiversion="${FDB_API_VERSION}" -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -P workloads/dynamic -p threadcount=${LOADTHREADCOUNT}
    echo "Load completed"
	fi
	if [ "x${RUNMODE}" == "xsingle" ]
	then
		while true
		do
			echo "Running benchmark"
      export BENCHMARK_NAME="${BENCHMARK_NAME_PREFIX}-run"
      timeout ${RUNTHREADDURATION} ${BIN_PATH}/go-ycsb run foundationdb -p keyprefix="${KEY_PREFIX}" -p fdb.clusterfile="${FDB_CLUSTER_FILE}" -p fdb.apiversion="${FDB_API_VERSION}" -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -p fdb.usecachedreadversions=${FDB_USE_CACHED_READ_VERSION} -p fdb.versioncachetime=${FDB_VERSION_CACHE_TIME} -P workloads/dynamic -p threadcount=${RUNTHREADCOUNT}
			echo "Run completed, sleeping before running again"
			sleep ${RUNTHREADSLEEPINTERVAL}
		done
	elif [ "x${RUNMODE}" == "xmultiple_threads" ]
	then
		while true
		do
			for th in ${RUNTHREADCONF}
			do
				echo "Running benchmark for ${th} thread(s)"
    		export BENCHMARK_NAME="${BENCHMARK_NAME_PREFIX}-run-th${th}"
        timeout ${RUNTHREADDURATION} ${BIN_PATH}/go-ycsb run foundationdb -p keyprefix="${KEY_PREFIX}" -p fdb.clusterfile="${FDB_CLUSTER_FILE}" -p fdb.apiversion="${FDB_API_VERSION}" -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -p fdb.usecachedreadversions=${FDB_USE_CACHED_READ_VERSION} -p fdb.versioncachetime=${FDB_VERSION_CACHE_TIME} -P workloads/dynamic -p threadcount=${th}
				sleep ${RUNTHREADSLEEPINTERVAL}
			done
		done
	else
		echo "Invalid value in RUNMODE variable. Choose between single and multiple_threads."
	fi
}

function benchmark_s3() {
  echo "Benchmark start (s3)"

WORKLOAD_S3="recordcount=${RECORDCOUNT}
  operationcount=${OPERATIONCOUNT}
  workload=core

  readallfields=${READALLFIELDS}

  readproportion=${READPROPORTION}
  updateproportion=${UPDATEPROPORTION}
  scanproportion=${SCANPROPORTION}
  insertproportion=${INSERTPROPORTION}

  insertorder=${INSERT_ORDER}
  requestdistribution=${REQUEST_DISTRIBUTION}

  fieldlength=${FIELDLENGTH}
  fieldcount=${FIELDCOUNT}
  "

  echo "${WORKLOAD_S3}" > workloads/dynamic

	if [ ${STARTWITHLOAD} -gt 0 ]
	then
		echo "Loading new database"
		export BENCHMARK_NAME="${BENCHMARK_NAME_PREFIX}-load"
    ${BIN_PATH}/go-ycsb load s3 -p s3.bucket=${S3_BUCKET} -p s3.endpoint=${S3_ENDPOINT} -p s3.access_key=${AWS_ACCESS_KEY_ID} -p s3.secret_key=${AWS_SECRET_ACCESS_KEY} -p s3.use_path_style=${S3_USE_PATH_STYLE} -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -P workloads/dynamic -p threadcount=${LOADTHREADCOUNT}
    echo "Load completed"
	fi

  if [ "x${STOP_AFTER_LOAD}" == "xtrue" ]
  then
    echo "Load finished, requested to stop, sleeping"
    sleep ${SLEEP_INTERVAL_AFTER_STOP}
    exit 0
  fi

	if [ "x${RUNMODE}" == "xsingle" ]
	then
		while true
		do
			echo "Running benchmark"
      export BENCHMARK_NAME="${BENCHMARK_NAME_PREFIX}-run"
      timeout ${RUNTHREADDURATION} ${BIN_PATH}/go-ycsb run s3 -p s3.bucket=${S3_BUCKET} -p s3.endpoint=${S3_ENDPOINT} -p s3.access_key=${AWS_ACCESS_KEY_ID} -p s3.secret_key=${AWS_SECRET_ACCESS_KEY} -p s3.use_path_style=${S3_USE_PATH_STYLE} -p fieldcount="${FIELDCOUNT}" -p fieldlength=${FIELDLENGTH} -P workloads/dynamic -p threadcount=${RUNTHREADCOUNT}
			echo "Run completed, sleeping before running again"
			sleep ${RUNTHREADSLEEPINTERVAL}
		done
	fi


}

echo "Using engine ${ENGINE}"

case ${ENGINE} in
  "foundationdb")
  	benchmark_fdb
  ;;
  "s3")
    benchmark_s3
  ;;
  *)
  	echo "Unknown engine"
  	exit 1
  	;;
esac

