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
  //////////////////////////////////////////////////////////// mapper for the no-reducer case
  class MapOnlyMapper extends Mapper[AvroKey[AnyRef], NullWritable, AvroKey[AnyRef], NullWritable] {
    type Context = Mapper[AvroKey[AnyRef], NullWritable, AvroKey[AnyRef], NullWritable]#Context

    override def setup(context: Context) {
      mapperEngine(context.getConfiguration).begin()
    }

    override def map(wrappedInput: AvroKey[AnyRef], nullWritable: NullWritable, context: Context) {
      mapperEngine(context.getConfiguration) match {
        case emitEngine: PFAEmitEngine[_, _] =>
          // (might be a different context each time)
          emitEngine.emit = {result: AnyRef =>
            context.write(new AvroKey[AnyRef](result), NullWritable.get)
          }
          emitEngine.action(wrappedInput.datum)

        case otherEngine =>
          val result = otherEngine.action(wrappedInput.datum)
          context.write(new AvroKey(result), NullWritable.get)
      }
    }
  }

  //////////////////////////////////////////////////////////// configures and executes a maponly job
  class MapOnlyJob extends Configured with Tool {
    override def run(args: Array[String]): Int =
      if (args.length < 2 || args.length > 4) {
        System.err.println("Usage: score <pfaMapper> <inputData> <outputData>")
        -1
      }
      else {
        var pfaMapper: String = ""
        var inputData: String = ""
        var outputData: String = ""

        args.length match {
          case 3 => {
            pfaMapper = args(0)
            inputData = args(1)
            outputData = args(2)
          }
          case 4 => {
            pfaMapper = args(0)
            args(1) match {
              case "-file" => {
                val filename = args(2)
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
                outputData = args(3)
              }
              case _ => { 
                System.err.println("Usage: score <pfaMapper> -file <inputDataFile> <outputData>")
                return -1
              }
            }
          }
          case _ => {
            System.err.println("Usage: score <pfaMapper> -file <inputDataFile> <outputData>")
            return -1
          }
        }

        conf.set("pfa.mapper", pfaMapper)
        conf.set("mapred.reduce.tasks", "0")

        val job = new Job(conf, "hadrian-hadoop")
        job.setMapperClass(classOf[MapOnlyMapper])

        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        FileInputFormat.setInputPaths(job, inputPaths(inputData, fs))
        FileOutputFormat.setOutputPath(job, new Path(outputData))

        val mapper = mapperEngine(conf)

        job.setInputFormatClass(classOf[PFAKeyInputFormat[_]])
        AvroJob.setInputKeySchema(job, mapper.config.input.schema)

        job.setOutputFormatClass(classOf[AvroKeyOutputFormat[_]])
        AvroJob.setMapOutputKeySchema(job, mapper.config.output.schema)

        job.setJarByClass(classOf[MapOnlyJob])
        job.waitForCompletion(true)
        0
      }
  }
}
