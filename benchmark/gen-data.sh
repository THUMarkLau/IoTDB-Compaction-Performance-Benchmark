#!/bin/bash

java -jar ./test-data-generation-1.0-SNAPSHOT-jar-with-dependencies.jar -size 2147483648  -dir data/data
cp -r system data/
7z a non_chunk_overlap_1G.7z data/ -v4G
rm -rf data

java -jar ./test-data-generation-1.0-SNAPSHOT-jar-with-dependencies.jar -size 4294967296  -dir data/data
cp -r system data/
7z a non_chunk_overlap_2G.7z data/ -v4G
rm -rf data

java -jar ./test-data-generation-1.0-SNAPSHOT-jar-with-dependencies.jar -size 6442450944  -dir data/data
cp -r system data/
7z a non_chunk_overlap_3G.7z data/ -v4G
rm -rf data

java -jar ./test-data-generation-1.0-SNAPSHOT-jar-with-dependencies.jar -size 8589934592  -dir data/data
cp -r system data/
7z a non_chunk_overlap_4G.7z data/ -v4G
rm -rf data

java -jar ./test-data-generation-1.0-SNAPSHOT-jar-with-dependencies.jar -size 10737418240  -dir data/data -seqSizeRatio 0.1
cp -r system data/
7z a non_chunk_overlap_1seq_9unseq.7z data/ -v4G
rm -rf data

java -jar ./test-data-generation-1.0-SNAPSHOT-jar-with-dependencies.jar -size 10737418240  -dir data/data -seqSizeRatio 0.3
cp -r system data/
7z a non_chunk_overlap_3seq_7unseq.7z data/ -v4G
rm -rf data

java -jar ./test-data-generation-1.0-SNAPSHOT-jar-with-dependencies.jar -size 10737418240  -dir data/data -seqSizeRatio 0.7
cp -r system data/
7z a non_chunk_overlap_7seq_3unseq.7z data/ -v4G
rm -rf data

java -jar ./test-data-generation-1.0-SNAPSHOT-jar-with-dependencies.jar -size 10737418240  -dir data/data -seqSizeRatio 0.9
cp -r system data/
7z a non_chunk_overlap_9seq_1unseq.7z data/ -v4G
rm -rf data

ls | grep *.7z* | xargs -i md5sum {} | tee md5sum.txt