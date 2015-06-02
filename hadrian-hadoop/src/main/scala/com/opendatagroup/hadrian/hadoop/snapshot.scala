package com.opendatagroup.hadrian

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.util.Tool

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapred.AvroValue
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.avro.mapreduce.AvroKeyRecordReader
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat
import org.apache.avro.Schema

import com.opendatagroup.hadrian.data.AvroDataTranslator
import com.opendatagroup.hadrian.data.fromJson
import com.opendatagroup.hadrian.datatype.AvroField
import com.opendatagroup.hadrian.datatype.AvroRecord
import com.opendatagroup.hadrian.datatype.AvroString
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine

import scala.io.Source

import org.apache.hadoop.util.Progress

package hadoop {
  //////////////////////////////////////////////////////////// reducer used for snapshot only
  class SnapshotReducer extends Reducer[AvroKey[CharSequence], AvroValue[AnyRef], AvroKey[CharSequence], AvroValue[AnyRef]] {
    type Context = Reducer[AvroKey[CharSequence], AvroValue[AnyRef], AvroKey[CharSequence], AvroValue[AnyRef]]#Context
    var engine: PFAEngine[AnyRef, AnyRef] = null
    var translator: AvroDataTranslator = null
    var pair: GenericRecord = null
    var segmentationPool: String = null
    var segmentationSchema: Schema = null

    override def setup(context: Context) {
      val conf = context.getConfiguration

      engine = reducerEngine(conf)
      translator = new AvroDataTranslator(engine.inputType, engine.classLoader)
      pair = new org.apache.avro.generic.GenericData.Record(engine.inputType.schema)

      segmentationPool = conf.get("pfa.training.pool")
      segmentationSchema = engine.config.pools(segmentationPool).avroType.schema
    }

    override def reduce(wrappedKey: AvroKey[CharSequence], wrappedValues: java.lang.Iterable[AvroValue[AnyRef]], context: Context) {
      // *effectively* create a new engine for each key
      engine.revert()
      engine.begin()

      pair.put("key", wrappedKey.datum)

      System.err.println(s"Reducing " + wrappedKey.datum)

      var cntr: Int = 0
      for (wrappedValue: AvroValue[AnyRef] <- wrappedValues) {
        pair.put("value", wrappedValue.datum)
        val translated = translator.translate(pair)

        cntr = cntr + 1
        if ( cntr % 100 == 0 ) {
          context.progress()
        }

        // never write out any results
        engine.action(translated)
      }

      // this kind of workflow has a well-defined end: when all values associated with a key are done
      engine.end()

      // now write out a snapshot of the segmentation pool's key-value pairs, if any
      try {
        engine.snapshot.pools(segmentationPool).init foreach {case (key, value) =>
          context.write(new AvroKey(key), new AvroValue(fromJson(value, segmentationSchema)))
        }
      }
      catch {
        case _ : Throwable => {
          System.err.println(s"Problem in writing pool information")
          System.err.println(s"NUM    --> " + cntr)
          System.err.println(s"POOL   --> " + segmentationPool)
          System.err.println(s"MAPKEY --> " + wrappedKey.datum)
          System.err.println(s"REC    --> " + engine.snapshot.pools(segmentationPool))
          //System.err.println(s"KEY    --> " + key)
          //System.err.println(s"VALUE  --> " + value)
        }
      }
    }
  }

  //////////////////////////////////////////////////////////// configures and executes a snapshot jbo
  class SnapshotJob extends Configured with Tool {
    override def run(args: Array[String]): Int =
      if (args.length < 5 ) {
        System.err.println(s"You gave " + args.length + " arguments.")
        System.err.println(s"You gave: " + args.mkString(" "))
        System.err.println("Usage: snapshot <pfaMapper> <pfaReducerTemplate> <inputData> <outputParameters> <segmentationPool> <mergedModel>")
        -1
      }
      else {
        var pfaMapper: String = ""
        var pfaReducerTemplate: String = ""
        var inputData: String = ""
        var outputParameters: String = ""
        var segmentationPool: String = ""
        var mergedModel: String = ""
        if ( args.length == 5 ) {
          pfaMapper=args(0)
          pfaReducerTemplate=args(1)
          inputData=args(2)
          outputParameters=args(3)
          segmentationPool=args(4)
        }
        if ( args.length == 6 ) {
          pfaMapper=args(0)
          pfaReducerTemplate=args(1)
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
              outputParameters=args(4)
              segmentationPool=args(5)
            }
            case _ => {
              inputData=args(2)
              outputParameters=args(3)
              segmentationPool=args(4)
              mergedModel=args(5)
            }
          }
        }
        if ( args.length == 7 ) {
          pfaMapper=args(0)
          pfaReducerTemplate=args(1)
          args(2) match {
            case "-base" => {
              val base = args(3)
              val subs=args(4).split(",")
              System.err.println("Base Directory    ---> " + base)
              System.err.println("#Dirs             ---> " + subs.length)
              var inputs = ArrayBuffer[String]()
              for ( sub <- subs ) {
                inputs += base + "/" + sub
              }
              inputData = inputs.mkString(",")
              println(s"Input ---> ",inputData)
              outputParameters=args(5)
              segmentationPool=args(6)
            }
            case _ => {
              System.err.println(s"You gave " + args.length + " arguments.")
              System.err.println(s"You gave: " + args.mkString(" "))
              System.err.println("Usage: snapshot <pfaMapper> <pfaReducerTemplate> -base <base> <inputData> <outputParameters> <segmentationPool>")
              return -1
            }
          }
        }
        
        //val Vector(pfaMapper, pfaReducerTemplate, inputData, outputParameters, segmentationPool, mergedModel) = args.toVector

        println(s"PFA Mapper  --> " + pfaMapper)
        println(s"PFA Reducer --> " + pfaReducerTemplate)
        println(s"PFA Pool    --> " + segmentationPool)

        conf.set("pfa.mapper", pfaMapper)
        conf.set("pfa.reducer", pfaReducerTemplate)
        conf.set("pfa.training.pool", segmentationPool)
        conf.set("mapreduce.task.classpath.user.precedence", "true")
        conf.set("mapreduce.job.user.classpath.first", "true")
        //conf.set("mapred.max.maps.per.node", "1")
        //conf.set("mapred.max.reduces.per.node", "1")

        //val  milliSeconds: Long = 1000*60*60 //; <default is 600000, likewise can give any value)
        //conf.setLong("mapred.task.timeout", milliSeconds);
        //conf.setLong("mapreduce.task.timeout", milliSeconds);

        //conf.setInt("mapred.task.ping.timeout", 360)
        //conf.setInt("mapreduce.task.timeout", 600)
        //conf.set("mapred.reduce.tasks", "10")

        val job = new Job(conf, "hadrian-hadoop")
        job.setMapperClass(classOf[ScoreMapper])
        job.setReducerClass(classOf[SnapshotReducer])

        val fs = org.apache.hadoop.fs.FileSystem.get(conf)
        //println(s"InpuData ---> " + inputData)
        FileInputFormat.setInputPaths(job, inputPaths(inputData, fs))
        //println(s"OutputParameters ---> " + outputParameters)
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


        job.setInputFormatClass(classOf[PFAKeyInputFormat[_]])
        AvroJob.setInputKeySchema(job, mapper.config.input.schema)

        AvroJob.setMapOutputKeySchema(job, AvroString().schema)
        AvroJob.setMapOutputValueSchema(job, mapperValue.schema)

        job.setOutputFormatClass(classOf[AvroKeyValueOutputFormat[_, _]])
        AvroJob.setOutputKeySchema(job, AvroString().schema)
        AvroJob.setOutputValueSchema(job, reducer.config.pools(segmentationPool).avroType.schema)

        job.setJarByClass(classOf[SnapshotJob])
        job.waitForCompletion(true)

        if ( mergedModel.length > 0 ) {
          val mergedModelPath = new Path(mergedModel)
          if (fs.exists(mergedModelPath))
            throw new IllegalArgumentException("file \"%s\" already exists; delete it and try again".format(mergedModel))

          collectIntoMergedModel(fs, reducer.config, segmentationPool, segmentationSchema, outputParameters, mergedModelPath)
          println("Wrote finalized model to " + mergedModelPath.toString)
        }
        0
      }
  }

}
