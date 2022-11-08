/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.File;
import java.io.IOException;
import java.util.Random;

public class TsFileWriter {
  private TsFileIOWriter writer;
  private String sgName;
  private int deviceNum;
  private int seriesNum;
  private long fileSize;

  public TsFileWriter(File file, long fileSize) throws IOException {
    this.writer = new TsFileIOWriter(file);
    this.sgName = Args.sgName;
    this.deviceNum = Args.deviceNum;
    this.seriesNum = Args.seriesNum;
    this.fileSize = fileSize;
  }

  public void write(long startTime, long step) throws IOException {
    Random random = new Random();
    try {
      for (int deviceIndex = 0; deviceIndex < deviceNum; ++deviceIndex) {
        writer.startChunkGroup(sgName + ".d" + deviceIndex);
        for (int seriesIndex = 0; seriesIndex < seriesNum; ++seriesIndex) {
          ChunkWriterImpl chunkWriter =
              new ChunkWriterImpl(
                  new MeasurementSchema(
                      "s" + seriesIndex,
                      TSDataType.DOUBLE,
                      TSEncoding.PLAIN,
                      Args.enableCompression
                          ? CompressionType.SNAPPY
                          : CompressionType.UNCOMPRESSED));
          for (long time = startTime; time < startTime + step; ++time) {
            chunkWriter.write(time, random.nextDouble());
          }
          chunkWriter.writeToFileWriter(writer);
        }
        writer.endChunkGroup();
      }
      writer.endFile();
    } finally {
      writer.close();
    }
  }
}
