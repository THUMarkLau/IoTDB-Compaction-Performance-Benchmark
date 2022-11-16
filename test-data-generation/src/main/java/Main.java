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
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class Main {
  private static long seqStart = 0;
  private static long seqEnd = 0;
  private static long unseqStart = 0;
  private static long unseqEnd = 0;

  public static void main(String[] args) throws IOException, InterruptedException {
    Args.parseArgs(args);
    writeSeqFile();
    writeUnseqFile();
//    if (args[0].equals("0")) {
//      testReadingConcurrently(true);
//    } else {
//      testReading();
//    }
  }

  private static void testReading() throws IOException {
    try (FileChannel channel = FileChannel.open(Paths.get("/dev/sda"), StandardOpenOption.READ)) {
      long size = 2000398934016L;
      Random random = new Random();
      int readSize = 1024 * 1024;
      ByteBuffer buffer = ByteBuffer.allocate(readSize);
      long timeCost = 0;
      for (int i = 0; i < 2560; i++) {
        buffer.clear();
        long offset = Math.abs(random.nextLong() % (size - readSize));
        int currSize = 0;
        channel.position(offset);
        long startTime = System.nanoTime();
        while (currSize < readSize) {
          currSize += channel.read(buffer);
        }
        timeCost += System.nanoTime() - startTime;
      }
      System.out.println("TimeCost for reading 2560MB in a single thread is " + timeCost + " ns");
    }
  }

  private static void testReadingConcurrently(boolean sleep) throws IOException, InterruptedException {
    Thread[] threads = new Thread[10];
    AtomicLong timeCost = new AtomicLong(0L);
    for (int t = 0; t < threads.length; ++t) {
      threads[t] = new Thread(() -> {
        try (FileChannel channel = FileChannel.open(Paths.get("/dev/sda"), StandardOpenOption.READ)) {
          long size = 2000398934016L;
          Random random = new Random();
          int readSize = 1024 * 1024;
          ByteBuffer buffer = ByteBuffer.allocate(readSize);
          for (int i = 0; i < 256; i++) {
            buffer.clear();
            long offset = Math.abs(random.nextLong() % (size - readSize));
            int currSize = 0;
            channel.position(offset);
            long startTime = System.nanoTime();
            while (currSize < readSize) {
              currSize += channel.read(buffer);
            }
            timeCost.addAndGet(System.nanoTime() - startTime);
            if (sleep) {
              Thread.sleep(random.nextInt(200));
            }
          }
        } catch (IOException | InterruptedException e) {
          e.printStackTrace();
        }
      });
    }
    long startTime = System.nanoTime();
    for (Thread thread : threads) {
      thread.start();
    }
    for (Thread thread : threads) {
      thread.join();
    }
    System.out.println("TimeCost for reading 2560GB in 10 threads is " + timeCost.get() + " ns");
  }

  private static void writeSeqFile() throws IOException {
    long sizeForEachSeqFile = (long) (Args.totalFileSize * Args.seqFileSizeRatio / Args.seqFileNum);
    long sizeForEachUnseqFile =
        (long) (Args.totalFileSize * Args.unseqFileSizeRatio / Args.unseqFileNum);
    // Because of the encoding of time, the size of timestamp can be nearly ignored
    // We estimate the size of time as 1 byte
    int stepForUnseq =
        (int) Math.ceil(sizeForEachUnseqFile * 1.0d / Args.deviceNum / Args.seriesNum / 8.2);
    int stepForSeq =
        (int) Math.ceil(sizeForEachSeqFile * 1.0d / Args.deviceNum / Args.seriesNum / 8.2);
    File dataDir =
        new File(
            Args.dataRootDir
                + File.separator
                + "sequence"
                + File.separator
                + Args.sgName
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
      if (Args.overlap) {
        writer.write((long) fileIndex * stepForSeq, stepForSeq);
      } else {
        writer.write(
            Args.chunkTimeAlternating
                ? (long) fileIndex * (stepForSeq + stepForUnseq) + stepForUnseq
                : (long) Args.unseqFileNum * stepForUnseq + (long) fileIndex * stepForSeq,
            stepForSeq);
      }
    }
  }

  private static void writeUnseqFile() throws IOException {
    long sizeForEachUnseqFile =
        (long) (Args.totalFileSize * Args.unseqFileSizeRatio / Args.unseqFileNum);
    long sizeForEachSeqFile = (long) (Args.totalFileSize * Args.seqFileSizeRatio / Args.seqFileNum);
    int stepForUnseq =
        (int) Math.ceil(sizeForEachUnseqFile * 1.0d / Args.deviceNum / Args.seriesNum / 8.2);
    int stepForSeq =
        (int) Math.ceil(sizeForEachSeqFile * 1.0d / Args.deviceNum / Args.seriesNum / 8.2);
    File dataDir =
        new File(
            Args.dataRootDir
                + File.separator
                + "unsequence"
                + File.separator
                + Args.sgName
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
    if (Args.overlap) {
      int unseqFileNum =
          (int) (Math.ceil(Args.seqFileNum * Args.unseqFileSizeRatio / Args.seqFileSizeRatio));
      for (int fileIndex = 0; fileIndex < unseqFileNum; ++fileIndex) {
        File file =
            new File(
                dataDir,
                String.format(
                    "%d-%d-0-0.tsfile",
                    System.currentTimeMillis(), fileIndex + 1 + Args.seqFileNum));
        TsFileWriter writer = new TsFileWriter(file, sizeForEachUnseqFile);
        writer.write((long) fileIndex % Args.seqFileNum * stepForSeq, stepForSeq);
      }
    } else {
      for (int fileIndex = 0; fileIndex < Args.unseqFileNum; ++fileIndex) {
        File file =
            new File(
                dataDir,
                String.format(
                    "%d-%d-0-0.tsfile",
                    System.currentTimeMillis(), fileIndex + 1 + Args.seqFileNum));
        TsFileWriter writer = new TsFileWriter(file, sizeForEachUnseqFile);
        writer.write(
            Args.chunkTimeAlternating
                ? (long) fileIndex * (stepForSeq + stepForUnseq)
                : (long) fileIndex * stepForUnseq,
            stepForUnseq);
      }
    }
  }
}
