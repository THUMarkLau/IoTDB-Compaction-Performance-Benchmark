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

import java.io.File;
import java.io.IOException;

public class Main {
  public static void main(String[] args) throws IOException {
    Args.parseArgs(args);
    writeSeqFile();
    writeUnseqFile();
  }

  private static void writeSeqFile() throws IOException {
    long sizeForEachSeqFile = (long) (Args.totalFileSize * Args.seqFileSizeRatio / Args.seqFileNum);
    long sizeForEachUnseqFile =
        (long) (Args.totalFileSize * Args.unseqFileSizeRatio / Args.unseqFileNum);
    // Because of the encoding of time, the size of timestamp can be nearly ignored
    // We estimate the size of time as 1 byte
    int stepForUnseq =
        (int) Math.ceil(sizeForEachUnseqFile * 1.0d / Args.deviceNum / Args.seriesNum / 8);
    int stepForSeq =
        (int) Math.ceil(sizeForEachSeqFile * 1.0d / Args.deviceNum / Args.seriesNum / 8);
    File dataDir =
        new File(
            Args.dataRootDir
                + File.separator
                + "sequence"
                + File.separator
                + "0"
                + File.separator
                + "0");
    System.out.printf(
        "Writing sequence file, file num is %d, each file size is %.2f MB%n",
        Args.seqFileNum, sizeForEachSeqFile / 1024 / 1024.0d);
    if (!dataDir.exists() && !dataDir.mkdirs()) {
      System.out.println("Cannot create directory " + dataDir.getAbsolutePath());
      System.exit(-1);
    }
    for (int fileIndex = 0; fileIndex < Args.seqFileNum; ++fileIndex) {
      File file =
          new File(
              dataDir,
              String.format("%d-%d-0-0.tsfile", System.currentTimeMillis(), fileIndex + 1));
      TsFileWriter writer = new TsFileWriter(file, sizeForEachSeqFile);
      writer.write(
          Args.chunkTimeAlternating
              ? (long) fileIndex * (stepForSeq + stepForUnseq)
              : (long) Args.unseqFileNum * stepForUnseq + (long) fileIndex * stepForSeq,
          stepForSeq);
    }
  }

  private static void writeUnseqFile() throws IOException {
    long sizeForEachUnseqFile =
        (long) (Args.totalFileSize * Args.unseqFileSizeRatio / Args.unseqFileNum);
    long sizeForEachSeqFile = (long) (Args.totalFileSize * Args.seqFileSizeRatio / Args.seqFileNum);
    int stepForUnseq =
        (int) Math.ceil(sizeForEachUnseqFile * 1.0d / Args.deviceNum / Args.seriesNum / 8);
    int stepForSeq =
        (int) Math.ceil(sizeForEachSeqFile * 1.0d / Args.deviceNum / Args.seriesNum / 8);
    File dataDir =
        new File(
            Args.dataRootDir
                + File.separator
                + "unsequence"
                + File.separator
                + "0"
                + File.separator
                + "0");
    System.out.printf(
        "Writing unsequence file, file num is %d, each file size is %.2f MB%n",
        Args.unseqFileNum, sizeForEachUnseqFile / 1024 / 1024.0d);
    if (!dataDir.exists() && !dataDir.mkdirs()) {
      System.out.println("Cannot create directory " + dataDir.getAbsolutePath());
      System.exit(-1);
    }
    for (int fileIndex = Args.seqFileNum;
        fileIndex < Args.seqFileNum + Args.unseqFileNum;
        ++fileIndex) {
      File file =
          new File(
              dataDir,
              String.format("%d-%d-0-0.tsfile", System.currentTimeMillis(), fileIndex + 1));
      TsFileWriter writer = new TsFileWriter(file, sizeForEachUnseqFile);
      writer.write(
          Args.chunkTimeAlternating
              ? (long) fileIndex * (stepForSeq + stepForUnseq) + stepForSeq
              : (long) fileIndex * stepForUnseq,
          stepForUnseq);
    }
  }
}
