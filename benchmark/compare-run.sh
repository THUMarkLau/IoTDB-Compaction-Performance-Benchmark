#!/usr/bin/bash

bash ./run-benchmark.sh -r https://github.com/apache/iotdb -b rel/0.12 -v 0.12 -nc true
rm -rf rel-0.12
mv test-server rel-0.12
bash ./run-benchmark.sh -r https://github.com/apache/iotdb -b rel/0.13 -v 0.13 -nc true -smd true
rm -rf rel-0.13
mv test-server rel-0.13
bash ./run-benchmark.sh -r https://github.com/choubenson/iotdb -b newFastCompactionPerformer -nc true -smd true -v new
rm -rf fast-compaction
mv test-server fast-compaction