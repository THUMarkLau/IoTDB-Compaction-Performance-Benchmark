# IoTDB-Compaction-Performance-Benchmark

This tool aims to benchmark the performance of the compaction module in IoTDB by recording the time of compaction using
different standard dataset with a single click. All dataset is available in Tsinghua-Cloud.

# Usage

There is two directory, `benchmark` and `test-data-generation`. To start the benchmark, run as follow:

```bash
bash ./benchmark/run-benchmark.sh [-r REPOSITORY_URL] [-b BRANCH_NAME] [-c COMMIT] [-d DATASET_NAME] [-nc true] [-smd true]
```

```
-r: Repository url of IoTDB, default is https://github.com/apache/iotdb
-b: Branch to be tested, default is master
-c: Commit to be tested, default is HEAD
-d: Dataset to be run, default is ALL
-nc: If a directory of iotdb already exists, the script will remove the directory and clone a new IoTDB from the repository if nc is set to true
-smd: Skip the calculation of MD5 for the dataset if it is set to true
```

The script will automatically set up the environment, compile the IoTDB, download the dataset and run the test.
Notice, **THE SCRIPT CAN ONLY BE RAN IN UBUNTU**. The test result will be store in a txt file named "result.txt".