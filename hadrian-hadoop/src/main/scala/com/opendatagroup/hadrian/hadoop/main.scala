package com.opendatagroup.hadrian

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.util.ToolRunner

package hadoop {
  object Main {
    //////////////////////////////////////////////////////////// command line interface
    def main(args: Array[String]) {
      val conf: Configuration = new Configuration
      val otherArgs: Array[String] = new GenericOptionsParser(conf, args).getRemainingArgs
      otherArgs.head match {
        case "score" => ToolRunner.run(new ScoringJob, otherArgs.tail)
        case "train" => ToolRunner.run(new TrainingJob, otherArgs.tail)
        case "snapshot" => ToolRunner.run(new SnapshotJob, otherArgs.tail)
      }
    }
  }
}
