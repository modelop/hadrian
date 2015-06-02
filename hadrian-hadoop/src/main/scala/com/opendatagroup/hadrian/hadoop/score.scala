package com.opendatagroup.hadrian

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.util.Tool

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapred.AvroValue
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.mapreduce.AvroKeyOutputFormat

import com.opendatagroup.hadrian.data.AvroDataTranslator
import com.opendatagroup.hadrian.datatype.AvroRecord
import com.opendatagroup.hadrian.datatype.AvroField
import com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine

import scala.io.Source

package hadoop {
  //////////////////////////////////////////////////////////// reducer used for scoring only
  class ScoreReducer extends Reducer[AvroKey[AnyRef], AvroValue[AnyRef], AvroKey[AnyRef], NullWritable] {
    type Context = Reducer[AvroKey[AnyRef], AvroValue[AnyRef], AvroKey[AnyRef], NullWritable]#Context

    var translator: AvroDataTranslator = null
    var pair: GenericRecord = null

    override def setup(context: Context) {
      val engine = reducerEngine(context.getConfiguration)
      translator = new AvroDataTranslator(engine.inputType, engine.classLoader)
      pair = new org.apache.avro.generic.GenericData.Record(engine.inputType.schema)
      engine.begin()
    }

    override def reduce(wrappedKey: AvroKey[AnyRef], wrappedValues: java.lang.Iterable[AvroValue[AnyRef]], context: Context) {
      pair.put("key", wrappedKey.datum)

      for (wrappedValue: AvroValue[AnyRef] <- wrappedValues) {
        pair.put("value", wrappedValue.datum)
        val translated = translator.translate(pair)

        reducerEngine(context.getConfiguration) match {
          case emitEngine: PFAEmitEngine[_, _] =>
            // (might be a different context each time)
            emitEngine.emit = {result: AnyRef => context.write(new AvroKey(result), NullWritable.get)}
            emitEngine.action(translated)

          case otherEngine =>
            val result = otherEngine.action(translated)
            context.write(new AvroKey(result), NullWritable.get)
        }
      }
    }
  }

  //////////////////////////////////////////////////////////// configures and executes a scoring jbo
  class ScoringJob extends Configured with Tool {
    override def run(args: Array[String]): Int =
      if (args.length < 3 || args.length > 5) {
        System.err.println("Usage: score <pfaMapper> <pfaReducer> <inputData> <outputData>")
        -1
      }
      else {
        var pfaMapper: String = ""
        var pfaReducer: String = ""
        var inputData: String = ""
        var outputData: String = ""

        args.length match {
          case 4 => {
            pfaMapper = args(0)
            pfaReducer = args(1)
            inputData = args(2)
            outputData = args(3)
          }
          case 5 => {
            pfaMapper = args(0)
            pfaReducer = args(1)
            args(2) match {
              case "-file" => {
                val filename = args(3)
                System.err.println("File             ---> " + filename)
                var inputs = ArrayBuffer[String]()
                var nlines: Int = 0
                for (line <- Source.fromFile(filename).getLines()) {
                  inputs += line
                    nlines += 1
                  System.err.println(s"\t"+nlines+": "+line)
                }
                inputData = inputs.mkString(",")
                System.err.println("#Dirs             ---> " + nlines)
                outputData = args(4)
              }
              case _ => { 
                System.err.println("Usage: score <pfaMapper> <pfaReducer> -file <inputDataFile> <outputData>")
                return -1
              }
            }
          }
          case _ => {
            System.err.println("Usage: score <pfaMapper> <pfaReducer> -file <inputDataFile> <outputData>")
            return -1
          }
        }

        conf.set("pfa.mapper", pfaMapper)
        conf.set("pfa.reducer", pfaReducer)



        val job = new Job(conf, "hadrian-hadoop")
        job.setMapperClass(classOf[ScoreMapper])
        job.setReducerClass(classOf[ScoreReducer])

        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        FileInputFormat.setInputPaths(job, inputPaths(inputData, fs))
        FileOutputFormat.setOutputPath(job, new Path(outputData))

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
          case x => throw new IllegalArgumentException("pfaReducer must input a (key, value) record, not\n%s".format(x))
        }

        if (!mapperKey.accepts(reducerKey)  ||  !reducerKey.accepts(mapperKey))
          throw new IllegalArgumentException("pfaMapper output key is %s but pfaReducer input key is".format(mapperKey, reducerKey))
        if (!mapperValue.accepts(reducerValue)  ||  !reducerValue.accepts(mapperValue))
          throw new IllegalArgumentException("pfaMapper output value is %s but pfaReducer input value is".format(mapperValue, reducerValue))

        job.setInputFormatClass(classOf[PFAKeyInputFormat[_]])
        AvroJob.setInputKeySchema(job, mapper.config.input.schema)

        AvroJob.setMapOutputKeySchema(job, mapperKey.schema)
        AvroJob.setMapOutputValueSchema(job, mapperValue.schema)

        job.setOutputFormatClass(classOf[AvroKeyOutputFormat[_]])
        AvroJob.setOutputKeySchema(job, reducer.config.output.schema)

        job.setJarByClass(classOf[ScoringJob])
        job.waitForCompletion(true)
        0
      }
  }
}
