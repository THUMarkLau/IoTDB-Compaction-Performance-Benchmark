#!/benchmark/bash
REPOSITORY="https://github.com/apache/iotdb"
BRANCH="master"
COMMIT="HEAD"
VERSION="NEW"
DATASET="all"
NEW_CLONE="false"
SKIP_MD5="false"

while [[ $# -gt 0 ]]; do
  key=${1}
  case ${key} in
  -r | --repository)
    REPOSITORY=${2}
    shift 2
    ;;
  -b | --branch)
    BRANCH=${2}
    shift 2
    ;;
  -c | --commit)
    COMMIT=${2}
    shift 2
    ;;
  -d | --dataset)
    DATASET=${2}
    shift 2
    ;;
  -nc | --new_clone)
    NEW_CLONE=${2}
    shift 2
    ;;
  -smd | --skip_md5)
    SKIP_MD5=${2}
    shift 2
    ;;
  -v | --version)
    VERSION=${2}
    shift 2
    ;;
  *)
    shift
    ;;
  esac
done

kill-iotdb() {
  iotdb_pid=$(jps | grep -i iotdb | cut -d " " -f1)
  if [ ! "$iotdb_pid" ]; then
    echo "No IoTDB instance is running"
  else
    echo IoTDB pid is "$iotdb_pid", kill it
    # shellcheck disable=SC2091
    $(kill -9 "$iotdb_pid")
  fi
}

wait_unseq_clear() {
  if [ "${VERSION}" == "NEW" ] || [ "${VERSION}" == "new" ]; then
    cross_file_num=$(find data/datanode/data/sequence -name "*.cross" | wc -l)
  else
    cross_file_num=$(find data/data/sequence -name "*.merge" | wc -l)
  fi

  echo Waiting compaction to start
  while [ $cross_file_num -eq 0 ]; do
    if [ "${VERSION}" == "NEW" ] || [ "${VERSION}" == "new" ]; then
      cross_file_num=$(find data/datanode/data/sequence -name "*.cross" | wc -l)
    else
      cross_file_num=$(find data/data/sequence -name "*.merge" | wc -l)
    fi
    sleep 1s
  done

  echo Compaction starts

  unseq_num=$(find data/datanode/data/unsequence -name "*.tsfile" | wc -l)
  echo "$unseq_num"
  start_time=$(date +"%Y-%m-%d %H:%M:%S")

  while [ "$unseq_num" -ne 0 ]; do
    # shellcheck disable=SC2038
    if [ "${VERSION}" == "NEW" ] || [ "${VERSION}" == "new" ]; then
      unseq_file_size=$(find . -name "*.cross" | xargs du -ch | tail -n 1 | cut -d " " -f1)
    else
      unseq_file_size=$(find . -name "*.merge" | xargs du -ch | tail -n 1 | cut -d " " -f1)
    fi
    echo The size of cross-temp files, "$unseq_file_size"
    sleep 1s
    unseq_num=$(find data/datanode/data/unsequence -name "*.tsfile" | wc -l)
  done
  end_time=$(date +"%Y-%m-%d %H:%M:%S")
  s1=$(date -d "$start_time" +%s)
  s2=$(date -d "$end_time" +%s)
  time=$(expr "$s2" - $s1)
  echo $1 "$time" seconds >>"../test-result.txt"
}

clear_cache() {
  echo 3 | sudo tee /proc/sys/vm/drop_caches
}

test_one_dataset() {
  echo Testing "$1"
  echo Copying dataset $1
  # using hard link to accelerate copy
  if [ "${VERSION}" == "NEW" ] || [ "${VERSION}" == "new" ]; then
    mkdir -p test-server/data/datanode
    cp -r -v --link test-data/"$1"/data/* test-server/data/datanode/
  else
    mkdir -p test-server/data
    cp -r -v --link test-data/"$1"/data/* test-server/data/
  fi

  echo Finish copying

  cd test-server || exit

  echo Starting IoTDB
  # make sure no other iotdb is running
  kill-iotdb

  clear_cache

  nohup bash sbin/start-server.sh >/dev/null 2>&1 &

  wait_unseq_clear "$1"

  echo Compaction finish, kill IoTDB
  kill-iotdb
  mv logs logs-"$1"
  rm -rf data
  cd ..
}

prepare_env() {
  sudo apt-get update
  sudo apt-get install git maven openjdk-8-jdk p7zip-full python3 python3-pip -y

  pip3 install -r requirements.txt --user
  if [ ! -e "/etc/sudoers.bk" ]; then
    sudo cp /etc/sudoers /etc/sudoers.bk
    echo "Defaults env_reset, timestamp_timeout=10000000" | sudo tee -a /etc/sudoers
  fi

  kill-iotdb
}

clone_and_compile_iotdb() {
  if [ -e "iotdb" ]; then
    if [ "${NEW_CLONE}" == "true" ]; then
      rm -rf iotdb
      while [ ! -e "iotdb" ]; do
        git clone "${REPOSITORY}"
      done
    fi
  else
    while [ ! -e "iotdb" ]; do
      git clone "${REPOSITORY}"
    done
  fi

  cd ./iotdb || exit
  git checkout --track origin/"${BRANCH}"
  # shellcheck disable=SC2086
  git reset --hard ${COMMIT} || exit

  mvn clean package -pl server -am -DskipTests -Dspotless.check.skip=true -Dcheckstyle.skip=true -Dxml-format.skip=true || exit

  cd ..
}

unzip_iotdb() {
  cd iotdb/server/target || exit
  zip_server=$(ls | grep iotdb-server-*.zip)
  echo "$zip_server"
  cp "$zip_server" ../../..
  cd ../../..
  rm -rf test-server
  unzip "$zip_server" -d "test-server"
}

set_iotdb_config() {
  echo Setting IoTDB Config
  # shellcheck disable=SC2129
  if [ "${VERSION}" == "new" ] || [ "${VERSION}" == "NEW" ]; then
    echo " " >> test-server/conf/iotdb-datanode.properties
    echo "enable_seq_space_compaction=false" >>test-server/conf/iotdb-datanode.properties
    echo "enable_unseq_space_compaction=false" >>test-server/conf/iotdb-datanode.properties
    echo "max_cross_compaction_candidate_file_size=53687091200" >>test-server/conf/iotdb-datanode.properties
    # do not limit the write speed
    echo "compaction_write_throughput_mb_per_sec=1024" >>test-server/conf/iotdb-datanode.properties
    echo "write_read_schema_free_memory_proportion=7:1:1:1:0" >>test-server/conf/iotdb-datanode.properties
    echo "cross_compaction_file_selection_time_budget=300000" >>test-server/conf/iotdb-datanode.properties
    sed -i s/#MAX_HEAP_SIZE=\"2G\"/MAX_HEAP_SIZE=\"30G\"/g test-server/conf/datanode-env.sh
    sed -i s/#HEAP_NEWSIZE=\"2G\"/HEAP_NEWSIZE=\"30G\"/g test-server/conf/datanode-env.sh
    echo "enable_memory_control=false" >> test-server/conf/iotdb-datanode.properties
  else
    echo " " >> test-server/conf/iotdb-engine.properties
    sed -i s/#MAX_HEAP_SIZE=\"2G\"/MAX_HEAP_SIZE=\"30G\"/g test-server/conf/iotdb-env.sh
    sed -i s/#HEAP_NEWSIZE=\"2G\"/HEAP_NEWSIZE=\"30G\"/g test-server/conf/iotdb-env.sh
    echo "merge_write_throughput_mb_per_sec=1024" >> test-server/conf/iotdb-engine.properties
    echo "merge_memory_budget=17179869184" >> test-server/conf/iotdb-engine.properties
  fi
}

download_dataset() {
  echo Downloading dataset $1
  # shellcheck disable=SC2164
  python3 ./test_data_manager.py test-data "non_chunk_overlap_1G" "${SKIP_MD5}"
}

test_all() {
  commit=$(git log -1 --pretty=format:%H)
  date +"%Y-%m-%d %H:%M:%S" >> "test-result.txt"
  echo repository: ${REPOSITORY} >>"test-result.txt"
  echo branch: ${BRANCH} >>"test-result.txt"
  echo commit: ${commit} >>"test-result.txt"
#  test_one_dataset "non_file_overlap"
#  test_one_dataset "non_file_overlap_aligned"
#  test_one_dataset "non_chunk_overlap"
#  test_one_dataset "non_chunk_overlap_aligned"
#  test_one_dataset "multi_page_overlap"
#  test_one_dataset "multi_page_overlap_aligned"
#  test_one_dataset "massive_page_overlap"
#  test_one_dataset "massive_page_overlap_aligned"
  test_one_dataset "non_chunk_overlap_1G"
#  test_one_dataset "non_chunk_overlap_2G"
#  test_one_dataset "non_chunk_overlap_3G"
#  test_one_dataset "non_chunk_overlap_4G"
#  test_one_dataset "non_chunk_overlap_1seq_9unseq"
#  test_one_dataset "non_chunk_overlap_3seq_7unseq"
#  test_one_dataset "non_chunk_overlap_7seq_3unseq"
#  test_one_dataset "non_chunk_overlap_9seq_1unseq"
}

echo repository is "${REPOSITORY}"
echo branch is "${BRANCH}"
echo commit is "${COMMIT}"

prepare_env

clone_and_compile_iotdb

unzip_iotdb

set_iotdb_config

download_dataset "${DATASET}"

# shellcheck disable=SC2205
if [ "${DATASET}" == "all" ]; then
  echo Test all
  test_all
else
  echo Test "${DATASET}"
  cd iotdb
  commit=$(git log -1 --pretty=format:%H)
  cd ..
  # shellcheck disable=SC2129
  date +"%Y-%m-%d %H:%M:%S" >> "test-result.txt"
  echo repository: ${REPOSITORY} >>"test-result.txt"
  echo branch: ${BRANCH} >>"test-result.txt"
  echo commit: ${commit} >>"test-result.txt"
  test_one_dataset "${DATASET}"
fi

# shellcheck disable=SC2103
cd ..
