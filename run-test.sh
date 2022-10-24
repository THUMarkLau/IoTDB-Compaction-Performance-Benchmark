#!/bin/bash
REPOSITORY="https://github.com/apache/iotdb"
BRANCH="master"
COMMIT="HEAD"
while [[ $# -gt 0 ]];do
  key=${1}
  case ${key} in
    -r|--repository)
      REPOSITORY=${2}
      shift 2
      ;;
    -b|--branch)
      BRANCH=${2}
      shift 2
      ;;
    -c|--commit)
      COMMIT=${2}
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

echo repository is "${REPOSITORY}"
echo branch is "${BRANCH}"
echo commit is "${COMMIT}"

sudo apt-get update
sudo apt-get install git maven openjdk-8-jdk p7zip-full python3 python3-pip -y

pip3 install -r requirements.txt --user

#rm -rf iotdb
#git clone "${REPOSITORY}" || exit
cd ./iotdb || exit
git checkout --track origin/"${BRANCH}"
# shellcheck disable=SC2086
git reset --hard ${COMMIT} || exit

#mvn clean package -pl server -am -DskipTests || exit

cd server/target || exit

# shellcheck disable=SC2062
# shellcheck disable=SC2010
zip_server=$(ls|grep iotdb-server-*.zip)
echo "$zip_server"
cp "$zip_server" ../../..
cd ../../..
rm -rf test-server
unzip "$zip_server" -d "test-server"

# shellcheck disable=SC2164
python3 ./test_data_manager.py test-data non_file_overlap
mkdir -p test-server/data/datanode
cp -r test-data/non_file_overlap/data test-server/data/datanode/

cd test-server

nohup bash sbin/start-server.sh >/dev/null 2>&1 &
sleep 30s

kill-iotdb(){
  iotdb_pid=$(jps | grep -i iotdb | cut -d " " -f1)
  if [ ! "$iotdb_pid" ]; then
    echo "No IoTDB instance is running"
  else
    echo IoTDB pid is "$iotdb_pid", kill it
    # shellcheck disable=SC2091
    $(kill -9 "$iotdb_pid")
  fi
}

kill-iotdb
# shellcheck disable=SC2103
cd ..
