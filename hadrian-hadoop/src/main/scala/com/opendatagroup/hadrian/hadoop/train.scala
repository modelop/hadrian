package com.opendatagroup.hadrian

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.filecache.DistributedCache
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.util.Tool

import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapred.AvroValue
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.avro.mapreduce.AvroKeyRecordReader
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat
import org.apache.avro.Schema

import com.opendatagroup.hadrian.data.fromJson
import com.opendatagroup.hadrian.data.toJson
import com.opendatagroup.hadrian.datatype.AvroRecord
import com.opendatagroup.hadrian.datatype.AvroField
import com.opendatagroup.hadrian.datatype.AvroString

package hadoop {
  //////////////////////////////////////////////////////////// reducer used for training only
  class TrainReducer extends Reducer[AvroKey[CharSequence], AvroValue[AnyRef], AvroKey[CharSequence], AvroValue[AnyRef]] {
    type Context = Reducer[AvroKey[CharSequence], AvroValue[AnyRef], AvroKey[CharSequence], AvroValue[AnyRef]]#Context

    var inputValueSchema: Schema = null
    var segmentationSchema: Schema = null
    var processBuilder: java.lang.ProcessBuilder = null
    var processInput: java.io.PrintWriter = null
    var processOutput: java.util.Scanner = null
    var errorThread: Thread = null

    override def setup(context: Context) {
      val conf = context.getConfiguration
      val engine = reducerEngine(conf)
      inputValueSchema = engine.config.input.asInstanceOf[AvroRecord].field("value").avroType.schema
      segmentationSchema = engine.config.pools(conf.get("pfa.training.pool")).avroType.schema

      val localFiles = DistributedCache.getLocalCacheFiles(conf)
      for (file <- localFiles)
        println(file)

      println(new java.io.File(".").getAbsolutePath)
      for (file <- new java.io.File(".").listFiles)
        println(file.getAbsolutePath)

      processBuilder = new ProcessBuilder(localFiles.head.toString)
      val process = processBuilder.start()
      processInput = new java.io.PrintWriter(process.getOutputStream)
      processOutput = new java.util.Scanner(process.getInputStream)

      errorThread = new Thread(new Runnable {
        def run() {
          val processError = new java.util.Scanner(process.getErrorStream)
          while (processError.hasNextLine)
            System.err.println(processError.next())
        }})
      errorThread.setDaemon(true)
      errorThread.start()
    }

    override def reduce(wrappedKey: AvroKey[CharSequence], wrappedValues: java.lang.Iterable[AvroValue[AnyRef]], context: Context) {
      processInput.println("BEGIN " + wrappedKey.datum.toString)

      for (wrappedValue: AvroValue[AnyRef] <- wrappedValues) {
        processInput.println(toJson(wrappedValue.datum, inputValueSchema))
      }

      processInput.println("END")
      processInput.flush()

      val result = processOutput.nextLine()
      context.write(wrappedKey, new AvroValue(fromJson(result, segmentationSchema)))
    }
  }

  //////////////////////////////////////////////////////////// configures and executes a training job
  class TrainingJob extends Configured with Tool {
    override def run(args: Array[String]): Int =
      if (args.length != 7) {
        System.err.println("Usage: train <pfaMapper> <pfaReducerTemplate> <inputData> <outputParameters> <trainingScript> <segmentationPool> <mergedModel>")
        -1
      }
      else {
        val Array(pfaMapper, pfaReducerTemplate, inputData, outputParameters, trainingScript, segmentationPool, mergedModel) = args

        conf.set("pfa.mapper", pfaMapper)
        conf.set("pfa.reducer", pfaReducerTemplate)
        DistributedCache.addCacheFile(new java.net.URI(trainingScript), conf)
        conf.set("pfa.training.pool", segmentationPool)

        val job = new Job(conf, "hadrian-hadoop")
        job.setMapperClass(classOf[ScoreMapper])
        job.setReducerClass(classOf[TrainReducer])
        
        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        FileInputFormat.setInputPaths(job, inputPaths(inputData, fs))
        FileOutputFormat.setOutputPath(job, new Path(outputParameters))

        val mapper = mapperEngine(conf)
        val (mapperKey, mapperValue) = mapper.config.output match {
          case AvroRecord(Seq(AvroField("key", k, _, _, _, _), AvroField("value", v, _, _, _, _)), _, _, _, _) => (k, v)
          case AvroRecord(Seq(AvroField("value", v, _, _, _, _), AvroField("key", k, _, _, _, _)), _, _, _, _) => (k, v)
          case x => throw new IllegalArgumentException("pfaMapper must output a (key, value) record, not\n%s".format(x))
        }

        val reducer = reducerEngine(conf)
        val (reducerKey, reducerValue) = reducer.config.input match {
          case AvroRecord(Seq(AvroField("key", k, _, _, _, _), AvroField("value", v, _, _, _, _)), _, _, _, _) => (k, v)
          case AvroRecord(Seq(AvroField("value", v, _, _, _, _), AvroField("key", k, _, _, _, _)), _, _, _, _) => (k, v)
          case x => throw new IllegalArgumentException("pfaReducerTemplate must input a (key, value) record, not\n%s".format(x))
        }

        if (!mapperKey.accepts(reducerKey)  ||  !reducerKey.accepts(mapperKey))
          throw new IllegalArgumentException("pfaMapper output key is %s but pfaReducerTemplate input key is".format(mapperKey, reducerKey))
        if (!mapperValue.accepts(reducerValue)  ||  !reducerValue.accepts(mapperValue))
          throw new IllegalArgumentException("pfaMapper output value is %s but pfaReducerTemplate input value is".format(mapperValue, reducerValue))

        if (!mapperKey.isInstanceOf[AvroString])
          throw new IllegalArgumentException("key type must be string to train models")

        if (!reducer.config.pools.contains(segmentationPool))
          throw new IllegalArgumentException("pfaReducerTemplate must have a pool named \"%s\"".format(segmentationPool))
        val segmentationSchema = reducer.config.pools(segmentationPool).avroType.schema

        val mergedModelPath = new Path(mergedModel)
        if (fs.exists(mergedModelPath))
          throw new IllegalArgumentException("file \"%s\" already exists; delete it and try again".format(mergedModel))

        job.setInputFormatClass(classOf[PFAKeyInputFormat[_]])
        AvroJob.setInputKeySchema(job, mapper.config.input.schema)

        AvroJob.setMapOutputKeySchema(job, AvroString().schema)
        AvroJob.setMapOutputValueSchema(job, mapperValue.schema)

        job.setOutputFormatClass(classOf[AvroKeyValueOutputFormat[_, _]])
        AvroJob.setOutputKeySchema(job, AvroString().schema)
        AvroJob.setOutputValueSchema(job, reducer.config.pools(segmentationPool).avroType.schema)

        job.setJarByClass(classOf[ScoringJob])
        job.waitForCompletion(true)

        collectIntoMergedModel(fs, reducer.config, segmentationPool, segmentationSchema, outputParameters, mergedModelPath)
        println("Wrote finalized model to " + mergedModelPath.toString)
        0
      }
  }
}
