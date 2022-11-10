#!/usr/bin/bash

bash ./run-benchmark.sh -r https://github.com/apache/iotdb -b rel/0.12 -v 0.12 -nc true
mv test-server rel/0.12
bash ./run-benchmark.sh -r https://github.com/apache/iotdb -b master -v NEW -nc true -smd true
mv test-server master
bash ./run-benchmark.sh -r https://github.com/choubenson/iotdb -b newFastCompactionPerformer -ncc true -smd true
mv test-server fast-compaction