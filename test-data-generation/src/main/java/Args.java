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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class Args {
  public static String dataRootDir = "data";
  public static String sgName = "root.test";
  public static int deviceNum = 1000;
  public static int seriesNum = 50;
  public static int seqFileNum = 1;
  public static int unseqFileNum = 1;
  public static long totalFileSize = 1024L * 1024L * 1024L;
  public static double seqFileSizeRatio = 0.5;
  public static double unseqFileSizeRatio = 0.5;
  public static boolean enableCompression = false;
  public static boolean chunkTimeAlternating = false;

  public static void parseArgs(String[] args) {
    Options options = buildOptions();
    CommandLine cli = null;
    CommandLineParser cliParser = new DefaultParser();
    HelpFormatter helpFormatter = new HelpFormatter();

    try {
      cli = cliParser.parse(options, args);
    } catch (ParseException e) {
      helpFormatter.printHelp(">>>>>> test cli options", options);
      e.printStackTrace();
    }

    assert cli != null;
    if (cli.hasOption("dir")) {
      dataRootDir = cli.getOptionValue("dir", "data");
    }
    if (cli.hasOption("sgName")) {
      sgName = cli.getOptionValue("sgName", "root.test");
    }
    if (cli.hasOption("device")) {
      deviceNum = Integer.parseInt(cli.getOptionValue("device", "1000"));
    }
    if (cli.hasOption("series")) {
      seriesNum = Integer.parseInt(cli.getOptionValue("series", "50"));
    }
    if (cli.hasOption("seqNum")) {
      seqFileNum = Integer.parseInt(cli.getOptionValue("seqNum", "1"));
    }
    if (cli.hasOption("unseqNum")) {
      unseqFileNum = Integer.parseInt(cli.getOptionValue("unseqNum", "1"));
    }
    if (cli.hasOption("size")) {
      totalFileSize = Long.parseLong(cli.getOptionValue("size", "1073741824"));
    }
    if (cli.hasOption("seqSizeRatio")) {
      seqFileSizeRatio = Double.parseDouble(cli.getOptionValue("seqSizeRatio", "0.5"));
      unseqFileSizeRatio = 1 - seqFileSizeRatio;
    }
    if (cli.hasOption("compress")) {
      enableCompression = true;
    }
    if (cli.hasOption("alter")) {
      chunkTimeAlternating = true;
    }

    System.out.println("********************************************");
    System.out.println("Configuration");
    System.out.println("********************************************");
    System.out.println("dataRootDir: " + dataRootDir);
    System.out.println("sgName: " + sgName);
    System.out.println("deviceNum: " + deviceNum);
    System.out.println("seriesNum: " + seriesNum);
    System.out.println("seqFileNum: " + seqFileNum);
    System.out.println("unseqFileNum: " + unseqFileNum);
    System.out.println("totalFileSize: " + totalFileSize);
    System.out.println("seqFileSizeRatio: " + seqFileSizeRatio);
    System.out.println("unseqFileSizeRatio: " + unseqFileSizeRatio);
    System.out.println("enableCompress: " + enableCompression);
    System.out.println("chunkTimeAlternating: " + chunkTimeAlternating);
    System.out.println("********************************************");
  }

  private static Options buildOptions() {
    Options options = new Options();

    Option optionForDataRootDir =
        new Option("dir", "dataRootDir", true, "The root dir for generated data");
    optionForDataRootDir.setRequired(false);
    options.addOption(optionForDataRootDir);

    Option optionForSgName = new Option("sgName", "sgName", true, "Storage group name");
    optionForSgName.setRequired(false);
    options.addOption(optionForSgName);

    Option optionForDevice =
        new Option("device", "deviceNum", true, "The num of devices in each file");
    optionForDevice.setRequired(false);
    options.addOption(optionForDevice);

    Option optionForSeries =
        new Option("series", "seriesNum", true, "The num of series of each device");
    optionForSeries.setRequired(false);
    options.addOption(optionForSeries);

    Option optionForSeqFileNum =
        new Option("seqNum", "seqFileNum", true, "The num of sequence file");
    optionForSeqFileNum.setRequired(false);
    options.addOption(optionForSeqFileNum);

    Option optionForUnseqFileNum =
        new Option("unseqNum", "unseqFileNum", true, "The num of unsequence file");
    optionForUnseqFileNum.setRequired(false);
    options.addOption(optionForUnseqFileNum);

    Option optionForTotalSize =
        new Option("size", "totalSize", true, "The total size of generated file");
    optionForTotalSize.setRequired(false);
    options.addOption(optionForTotalSize);

    Option optionForSeqFileSizeRatio =
        new Option("seqSizeRatio", "seqFileSizeRatio", true, "The ratio of sequence file size");
    optionForSeqFileSizeRatio.setRequired(false);
    options.addOption(optionForSeqFileSizeRatio);

    Option optionForEnableCompression =
        new Option("compress", "enableCompression", false, "Enable compression or not");
    optionForEnableCompression.setRequired(false);
    options.addOption(optionForEnableCompression);

    Option optionForChunkTimeAlternating =
        new Option("alter", "timeAlternate", false, "Chunk time alternating or not");
    optionForChunkTimeAlternating.setRequired(false);
    options.addOption(optionForChunkTimeAlternating);

    return options;
  }
}
