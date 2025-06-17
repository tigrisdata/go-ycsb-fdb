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

WORKLOAD="recordcount=${RECORDCOUNT}
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

function benchmark_fdb() {
  echo "Benchmark start"
	echo "Using FDB_CLUSTER_FILE ${FDB_CLUSTER_FILE}"
	echo "Using FDB_API_VERSION ${FDB_API_VERSION}"
	echo "Using LOADTHREADCOUNT ${LOADTHREADCOUNT}"
	echo "Using RUNTHREADCOUNT ${RUNTHREADCOUNT}"
	echo "Using RUNTHREADDURATION ${RUNTHREADDURATION}"
	echo "Using cached read version ${FDB_USE_CACHED_READ_VERSION}, version cache time: ${FDB_VERSION_CACHE_TIME}"


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

echo "${WORKLOAD}" > workloads/dynamic

echo "Using engine ${ENGINE}"

case ${ENGINE} in
  "foundationdb")
  	benchmark_fdb
  ;;
  *)
  	echo "Unknown engine"
  	exit 1
  	;;
esac

