package com.opendatagroup.hadrian

import java.io.InputStream

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.conf.Configured
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.io.RawComparator
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Partitioner
import org.apache.hadoop.mapreduce.Reducer
import org.apache.hadoop.util.GenericOptionsParser
import org.apache.hadoop.util.Tool
import org.apache.hadoop.util.ToolRunner

import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapred.AvroValue
import org.apache.avro.mapred.Pair
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.avro.mapreduce.AvroKeyRecordReader
import org.apache.avro.mapreduce.AvroKeyRecordWriter
import org.apache.avro.mapreduce.AvroKeyValueInputFormat
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat
import org.apache.avro.mapreduce.AvroKeyValueRecordReader
import org.apache.avro.mapreduce.AvroKeyValueRecordWriter
import org.apache.avro.Schema

import com.opendatagroup.hadrian.ast.Method
import com.opendatagroup.hadrian.ast.Cell
import com.opendatagroup.hadrian.ast.Pool
import com.opendatagroup.hadrian.data.fromAvro
import com.opendatagroup.hadrian.data.PFADatumReader
import com.opendatagroup.hadrian.data.AnyPFARecord
import com.opendatagroup.hadrian.datatype.AvroField
import com.opendatagroup.hadrian.datatype.AvroRecord
import com.opendatagroup.hadrian.datatype.AvroString
import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine
import com.opendatagroup.hadrian.jvmcompiler.PFAMapEngine

import com.opendatagroup.antinous.engine.JythonEngine

package object mr {
  // global configuration for the client-side (mappers and reducers get their configurations from Hadoop)
  val configuration = new Configuration

  //////////////////////////////////////////////////////////// loading engines and avoiding multiple instances in the same actor
  private var firstLoadEngine = true
  def loadEngine(fileName: String, inputStream: InputStream): PFAEngine[AnyRef, AnyRef] = {
    val normalizedName = fileName.toLowerCase
    if (normalizedName.endsWith(".pfa")  ||  normalizedName.endsWith(".json"))
      PFAEngine.fromJson(inputStream).head
    else if (fileName.toLowerCase.endsWith(".yaml")  ||  fileName.toLowerCase.endsWith(".yml"))
      PFAEngine.fromYaml(new java.util.Scanner(inputStream).useDelimiter("\\A").next()).head
    else {
      if (firstLoadEngine) {
        val sys = org.python.core.Py.getSystemState
        sys.path.insert(0, new org.python.core.PyString(getClass.getProtectionDomain.getCodeSource.getLocation.getPath.toString + "/Lib"))
        firstLoadEngine = false
      }
      JythonEngine.fromPython(inputStream, fileName).head.asInstanceOf[PFAEngine[AnyRef, AnyRef]]
    }
  }

  private var mapperEngine: PFAEngine[AnyRef, AnyRef] = null
  def getMapper(configuration: Configuration): PFAEngine[AnyRef, AnyRef] = {
    if (mapperEngine == null)
      mapperEngine = loadEngine(configuration.get("pfa.mapper"), FileSystem.get(configuration).open(new Path(configuration.get("pfa.mapper"))))
    mapperEngine
  }

  private var reducerEngine: PFAEngine[AnyRef, AnyRef] = null
  def getReducer(configuration: Configuration): PFAEngine[AnyRef, AnyRef] = {
    if (reducerEngine == null)
      reducerEngine = loadEngine(configuration.get("pfa.reducer"), FileSystem.get(configuration).open(new Path(configuration.get("pfa.reducer"))))
    reducerEngine
  }
}

package mr {
  //////////////////////////////////////////////////////////// scoring in the no-reduce case
  class MapOnlyMapper extends Mapper[AvroKey[AnyRef], NullWritable, AvroKey[AnyRef], NullWritable] {
    type Context = Mapper[AvroKey[AnyRef], NullWritable, AvroKey[AnyRef], NullWritable]#Context

    override def setup(context: Context) {
      getMapper(context.getConfiguration).begin()
    }

    override def map(wrappedInput: AvroKey[AnyRef], nullWritable: NullWritable, context: Context) {
      getMapper(context.getConfiguration) match {
        // map engine: context.write the output of each call
        case mapEngine: PFAMapEngine[_, _] =>
          val result = mapEngine.action(wrappedInput.datum)
          context.write(new AvroKey(result), NullWritable.get)

        // emit engine: pass context.write as a callback
        case emitEngine: PFAEmitEngine[_, _] =>
          // reassign the emit function because the context object might change
          emitEngine.emit = {result: AnyRef =>
            context.write(new AvroKey[AnyRef](result), NullWritable.get)
          }
          emitEngine.action(wrappedInput.datum)

        // fold engines: not allowed in the mapper
      }
    }
  }

  //////////////////////////////////////////////////////////// scoring in the mapper stage
  class ScoreMapper extends Mapper[AvroKey[AnyRef], NullWritable, AvroKey[AnyRef], AvroValue[AnyRef]] {
    type Context = Mapper[AvroKey[AnyRef], NullWritable, AvroKey[AnyRef], AvroValue[AnyRef]]#Context

    override def setup(context: Context) {
      getMapper(context.getConfiguration).begin()
    }

    override def map(wrappedInput: AvroKey[AnyRef], nullWritable: NullWritable, context: Context) {
      getMapper(context.getConfiguration) match {
        // map engine: context.write the output of each call
        case mapEngine: PFAMapEngine[_, _] =>
          val result = mapEngine.action(wrappedInput.datum)
          val key = result.asInstanceOf[AnyPFARecord].get("key")
          val value = result.asInstanceOf[AnyPFARecord].get("value")
          context.write(new AvroKey(key), new AvroValue(value))

        // emit engine: pass context.write as a callback
        case emitEngine: PFAEmitEngine[_, _] =>
          // reassign the emit function because the context object might change
          emitEngine.emit = {result: AnyRef =>
            val key = result.asInstanceOf[AnyPFARecord].get("key")
            val value = result.asInstanceOf[AnyPFARecord].get("value")
            context.write(new AvroKey[AnyRef](key), new AvroValue[AnyRef](value))
          }
          emitEngine.action(wrappedInput.datum)

        // fold engines: not allowed in the mapper
      }
    }
  }

  //////////////////////////////////////////////////////////// scoring in the reducer stage
  class ScoreReducer extends Reducer[AvroKey[AnyRef], AvroValue[AnyRef], AvroKey[AnyRef], NullWritable] {
    type Context = Reducer[AvroKey[AnyRef], AvroValue[AnyRef], AvroKey[AnyRef], NullWritable]#Context

    var pair: GenericRecord = null

    override def setup(context: Context) {
      pair = new org.apache.avro.generic.GenericData.Record(getReducer(context.getConfiguration).inputType.schema)
    }

    override def reduce(wrappedKey: AvroKey[AnyRef], wrappedValues: java.lang.Iterable[AvroValue[AnyRef]], context: Context) {
      pair.put("key", wrappedKey.datum)

      val engine = getReducer(context.getConfiguration)
      engine.revert()  // always revert at the beginning of a reduce to clear any accumulated state from the previous key

      engine match {
        // map engine: context.write the output of each call
        case mapEngine: PFAMapEngine[_, _] =>
          engine.begin()
          for (wrappedValue: AvroValue[AnyRef] <- wrappedValues) {
            pair.put("value", wrappedValue.datum)
            val translated = engine.fromGenericAvroData(pair)
            val result = mapEngine.action(translated)
            context.write(new AvroKey(result), NullWritable.get)
          }
          engine.end()

        // emit engine: pass context.write as a callback
        case emitEngine: PFAEmitEngine[_, _] =>
          emitEngine.emit = {result: AnyRef => context.write(new AvroKey(result), NullWritable.get)}
          engine.begin()
          for (wrappedValue: AvroValue[AnyRef] <- wrappedValues) {
            pair.put("value", wrappedValue.datum)
            val translated = engine.fromGenericAvroData(pair)
            emitEngine.action(translated)
          }
          engine.end()

        // fold engines: handled by the next class
      }
    }
  }

  //////////////////////////////////////////////////////////// scoring in the reducer stage for folding engines
  class FoldReducer extends Reducer[AvroKey[AnyRef], AvroValue[AnyRef], AvroKey[AnyRef], AvroValue[AnyRef]] {
    type Context = Reducer[AvroKey[AnyRef], AvroValue[AnyRef], AvroKey[AnyRef], AvroValue[AnyRef]]#Context

    var pair: GenericRecord = null

    override def setup(context: Context) {
      pair = new org.apache.avro.generic.GenericData.Record(getReducer(context.getConfiguration).inputType.schema)
    }

    override def reduce(wrappedKey: AvroKey[AnyRef], wrappedValues: java.lang.Iterable[AvroValue[AnyRef]], context: Context) {
      pair.put("key", wrappedKey.datum)
      
      val groupby = wrappedKey.datum match {
        case x: String => x
        case x: GenericRecord => x.get("groupby")
      }

      val engine = getReducer(context.getConfiguration)
      engine.revert()  // always revert at the beginning of a reduce to clear any accumulated state from the previous key
      
      // fold engines: accumulate and only output one result per key
      var sawAny = false
      var lastResult: AnyRef = null
      engine.begin()
      for (wrappedValue: AvroValue[AnyRef] <- wrappedValues) {
        pair.put("value", wrappedValue.datum)
        val translated = engine.fromGenericAvroData(pair)
        sawAny = true
        lastResult = engine.action(translated)
      }
      engine.end()
      if (sawAny)
        context.write(new AvroKey(groupby), new AvroValue(lastResult))
    }
  }

  //////////////////////////////////////////////////////////// reducer for snapshots
  class SnapshotReducer extends Reducer[AvroKey[AnyRef], AvroValue[AnyRef], AvroKey[AnyRef], AvroValue[AnyRef]] {
    type Context = Reducer[AvroKey[AnyRef], AvroValue[AnyRef], AvroKey[AnyRef], AvroValue[AnyRef]]#Context

    var pair: GenericRecord = null

    override def setup(context: Context) {
      pair = new org.apache.avro.generic.GenericData.Record(getReducer(context.getConfiguration).inputType.schema)
    }

    override def reduce(wrappedKey: AvroKey[AnyRef], wrappedValues: java.lang.Iterable[AvroValue[AnyRef]], context: Context) {
      pair.put("key", wrappedKey.datum)
      
      val groupby = wrappedKey.datum match {
        case x: String => x
        case x: GenericRecord => x.get("groupby")
      }

      val engine = getReducer(context.getConfiguration)
      engine.revert()  // always revert at the beginning of a reduce to clear any accumulated state from the previous key

      val cellPoolName = context.getConfiguration.get("pfa.snapshot")

      engine.begin()
      for (wrappedValue: AvroValue[AnyRef] <- wrappedValues) {
        pair.put("value", wrappedValue.datum)
        val translated = engine.fromGenericAvroData(pair)
        engine.action(translated)
      }
      engine.end()

      context.getConfiguration.get("pfa.snapshot.type") match {
        case "cell" =>
          val result = engine.snapshotCell(cellPoolName)
          context.write(new AvroKey(groupby), new AvroValue(result))

        case "pool" =>
          val poolKeySchema = SnapshotReducer.poolKeyType.schema
          engine.snapshotPool(cellPoolName) foreach {case (k, v) =>
            val poolKey = new org.apache.avro.generic.GenericData.Record(poolKeySchema)
            poolKey.put("groupby", groupby)
            poolKey.put("poolitem", k)
            context.write(new AvroKey(poolKey), new AvroValue(v))
          }
      }
    }
  }
  object SnapshotReducer {
    def poolKeyType: AvroType = AvroRecord(Seq(AvroField("groupby", AvroString()), AvroField("poolitem", AvroString())), "PoolKey")
  }

  //////////////////////////////////////////////////////////// identity classes
  class AvroIdentityMapper extends Mapper[AvroKey[AnyRef], NullWritable, AvroKey[AnyRef], AvroValue[AnyRef]] {
    // don't override anything!
  }
  class AvroIdentityReducer extends Reducer[AvroKey[AnyRef], AvroValue[AnyRef], AvroKey[AnyRef], AvroValue[AnyRef]] {
    // don't override anything!
  }

  //////////////////////////////////////////////////////////// custom partitioning and grouping for the case in which the key is a record
  class RecordKeyPartitioner[V] extends Partitioner[AvroKey[GenericRecord], AvroValue[V]] {
    override def getPartition(key: AvroKey[GenericRecord], value: AvroValue[V], numPartitions: Int): Int =
      Math.abs(key.datum.get("groupby").hashCode()) % numPartitions
  }

  class RecordKeyGrouper extends Configured with RawComparator[AvroKey[GenericRecord]] {
    val avroType = AvroRecord(Seq(AvroField("groupby", AvroString())), "Key")
    override def compare(b1: Array[Byte], s1: Int, l1: Int, b2: Array[Byte], s2: Int, l2: Int) = {
      val b1sliced =
        if (s1 == 0  &&  b1.size == l1)
          b1
        else
          java.util.Arrays.copyOfRange(b1, s1, s1 + l1)
      val b2sliced =
        if (s2 == 0  &&  b2.size == l2)
          b2
        else
          java.util.Arrays.copyOfRange(b2, s2, s2 + l2)
      val obj1 = fromAvro(b1sliced, avroType).asInstanceOf[GenericRecord]
      val obj2 = fromAvro(b2sliced, avroType).asInstanceOf[GenericRecord]
      obj1.get("groupby").toString.compareTo(obj2.get("groupby").toString)
    }
    def compare(obj1: AvroKey[GenericRecord], obj2: AvroKey[GenericRecord]) =
      obj1.datum.get("groupby").toString.compareTo(obj2.datum.get("groupby").toString)
  }

  //////////////////////////////////////////////////////////// overridden classes to load data efficiently
  class MapperKeyRecordReader[K](readerSchema: Schema, configuration: Configuration) extends AvroKeyRecordReader[K](readerSchema) {
    import org.apache.avro.file.SeekableInput
    import org.apache.avro.file.DataFileReader
    import org.apache.avro.io.DatumReader
    override def createAvroFileReader(input: SeekableInput, datumReader: DatumReader[K]): DataFileReader[K] = {
      // override the generic DatumReader
      val pfaReader = getMapper(configuration).datumReader.asInstanceOf[DatumReader[K]]
      pfaReader.setSchema(readerSchema)
      new DataFileReader[K](input, pfaReader)
    }
  }

  class MapperKeyInputFormat[K] extends AvroKeyInputFormat[K] {
    import org.apache.hadoop.mapreduce.InputSplit
    import org.apache.hadoop.mapreduce.TaskAttemptContext
    import org.apache.hadoop.mapreduce.RecordReader
    override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[AvroKey[K], NullWritable] = {
      // return a subclass of AvroKeyRecordReader to override the generic DatumReader
      val configuration = context.getConfiguration
      val readerSchema = AvroJob.getInputKeySchema(configuration)
      new MapperKeyRecordReader[K](readerSchema, configuration)
    }
  }

  //////////////////////////////////////////////////////////// configures and executes a scoring job
  case class ScoreOptions(input: String = null, output: String = null, mapper: String = null, reducer: Option[String] = None, snapshot: Option[String] = None, valid: Boolean = true, numReducers: Option[Int] = None)

  class ScoreJob extends Configured with Tool {
    override def run(args: Array[String]): Int = {
      val optionParser = new scopt.OptionParser[ScoreOptions]("hadoop jar hadrian-mr.jar") {
        arg[String]("input")
          .minOccurs(1)
          .maxOccurs(1)
          .action((x, c) => c.copy(input = x))
          .text("""input path specification""")

        arg[String]("output")
          .minOccurs(1)
          .maxOccurs(1)
          .action((x, c) => c.copy(output = x))
          .text("""output directory, must not yet exist""")

        opt[String]('m', "mapper")
          .minOccurs(1)
          .maxOccurs(1)
          .validate({x =>
            val name = x.toLowerCase
            if (name.endsWith(".pfa")  ||  name.endsWith(".json")  ||  name.endsWith(".yaml")  ||  name.endsWith(".yml")  ||  name.endsWith(".py"))
              success
            else
              failure(x + " does not end with .pfa|json|yaml|yml|py")
          })
          .action((x, c) => c.copy(mapper = x))
          .text("""location of mapper PFA""")

        opt[String]('r', "reducer")
          .minOccurs(0)
          .maxOccurs(1)
          .validate({x =>
            val name = x.toLowerCase
            if (name.endsWith(".pfa")  ||  name.endsWith(".json")  ||  name.endsWith(".yaml")  ||  name.endsWith(".yml")  ||  name.endsWith(".py"))
              success
            else
              failure(x + " does not end with .pfa|json|yaml|yml|py")
          })
          .action({(x, c) =>
            if (c.reducer.isEmpty)
              c.copy(reducer = Some(x))
            else {
              reportError("you cannot use both --reducer (-r) and --identity-reducer (-i)")
              c.copy(valid = false)
            }
          })
          .text("""location of reducer PFA""")

        opt[Unit]('i', "identity-reducer")
          .minOccurs(0)
          .maxOccurs(1)
          .action({(x, c) =>
            if (c.reducer.isEmpty)
              c.copy(reducer = Some(""))
            else {
              reportError("you cannot use both --reducer (-r) and --identity-reducer (-i)")
              c.copy(valid = false)
            }
          })
          .text("""use an identity reducer (key-grouping and possibly secondary sort, but no reducer action)""")

        opt[Int]('n', "num-reducers")
          .minOccurs(0)
          .maxOccurs(1)
          .action((x, c) => c.copy(numReducers = Some(x)))
          .text("""number of reducers (must be at least 1 if --reducer or --identity-reducer is used)""")

        opt[String]('s', "snapshot")
          .minOccurs(0)
          .maxOccurs(1)
          .action((x, c) => c.copy(snapshot = Some(x)))
          .text("""output a snapshot of a reducer cell/pool after processing each key, rather than the reducer engine's output (pools take precedence over cells in case of name conflicts)""")

        help("help").text("print this help message")

        note("""
Hadrian-MR in "score" mode runs a PFA-encoded scoring engine as a
mapper and a PFA-encoded scoring engine as a reducer.

The output type of the mapper must be a record with two fields: "key"
and "value".  The key must either be a string or a record containing a
string-valued "groupby" field.  If the key is a string, that string
will be used for grouping with no secondary sort.  If the key is a
record, its groupby field is used for grouping and the whole record is
used for secondary sort (according to the normal record-sorting Avro
rules).

The input type of the reducer must be a record with the same structure
as the mapper output.
""")
      }

      optionParser.parse(args, ScoreOptions()) match {
        case Some(scoreOptions) if (scoreOptions.valid) =>
          scoreOptions.reducer match {
            // this is a map-only scoring job
            case None =>
              scoreOptions.numReducers foreach {x =>
                if (x != 0)
                  throw new IllegalArgumentException("no reducer is specified, so --num-reducers cannot be anything but 0")
              }

              configuration.set("mapreduce.task.classpath.user.precedence", "true")
              configuration.set("mapreduce.job.user.classpath.first", "true")

              configuration.set("pfa.mapper", scoreOptions.mapper)
              configuration.set("mapred.reduce.tasks", "0")

              val job = new Job(configuration, "hadrian-mr-maponly")
              FileInputFormat.setInputPaths(job, new Path(scoreOptions.input))
              FileOutputFormat.setOutputPath(job, new Path(scoreOptions.output))

              val mapper = getMapper(configuration)

              job.setMapperClass(classOf[MapOnlyMapper])

              job.setInputFormatClass(classOf[MapperKeyInputFormat[_]])
              AvroJob.setInputKeySchema(job, mapper.inputType.schema)

              job.setOutputFormatClass(classOf[AvroKeyOutputFormat[_]])
              AvroJob.setMapOutputKeySchema(job, mapper.outputType.schema)

              job.setJarByClass(classOf[ScoreJob])
              job.waitForCompletion(true)
              0

            // this is a map-reduce scoring job
            case Some(reducerFileName) if (reducerFileName != "") =>
              scoreOptions.numReducers foreach {x =>
                if (x < 1)
                  throw new IllegalArgumentException("--reducer is specified, so --num-reducers must be at least 1")
                else
                  configuration.set("mapred.reduce.tasks", x.toString)
              }

              configuration.set("mapreduce.task.classpath.user.precedence", "true")
              configuration.set("mapreduce.job.user.classpath.first", "true")

              configuration.set("pfa.mapper", scoreOptions.mapper)
              configuration.set("pfa.reducer", reducerFileName)

              // load the models to ensure that they are valid before submitting a job
              val mapper = getMapper(configuration)
              val reducer = getReducer(configuration)

              // PFA semantics checks for the map-reduce workflow
              val (mapperKey, mapperValue) = mapper.outputType match {
                case AvroRecord(Seq(AvroField("key", k, _, _, _, _), AvroField("value", v, _, _, _, _)), _, _, _, _) => (k, v)
                case AvroRecord(Seq(AvroField("value", v, _, _, _, _), AvroField("key", k, _, _, _, _)), _, _, _, _) => (k, v)
                case x => throw new IllegalArgumentException("mapper must output a (key, value) record, not\n%s".format(x))
              }
              val (reducerKey, reducerValue) = reducer.inputType match {
                case AvroRecord(Seq(AvroField("key", k, _, _, _, _), AvroField("value", v, _, _, _, _)), _, _, _, _) => (k, v)
                case AvroRecord(Seq(AvroField("value", v, _, _, _, _), AvroField("key", k, _, _, _, _)), _, _, _, _) => (k, v)
                case x => throw new IllegalArgumentException("reducer must input a (key, value) record, not\n%s".format(x))
              }
              if (!mapperKey.accepts(reducerKey, checkNames = false)  ||  !reducerKey.accepts(mapperKey, checkNames = false))
                throw new IllegalArgumentException("mapper output key is %s but reducer input key is %s".format(mapperKey, reducerKey))
              if (!mapperValue.accepts(reducerValue, checkNames = false)  ||  !reducerValue.accepts(mapperValue, checkNames = false))
                throw new IllegalArgumentException("mapper output value is %s but reducer input value is %s".format(mapperValue, reducerValue))
              if (mapper.method == Method.FOLD)
                throw new IllegalArgumentException("mapper may only be method: map or method: emit, not method: fold")

              // special handling of reducer output type if it is a snapshot engine
              val (reducerKeyOutput, reducerValueOutput) = scoreOptions.snapshot match {
                case Some(cellPoolName) =>
                  if (reducer.isInstanceOf[JythonEngine])
                    throw new IllegalArgumentException("Jython engines can't be used with --snapshot")

                  reducer.config.pools.get(cellPoolName) orElse reducer.config.cells.get(cellPoolName) match {
                    case Some(x: Cell) =>
                      configuration.set("pfa.snapshot", cellPoolName)
                      configuration.set("pfa.snapshot.type", "cell")
                      (AvroString(), Some(x.avroType))

                    case Some(x: Pool) =>
                      configuration.set("pfa.snapshot", cellPoolName)
                      configuration.set("pfa.snapshot.type", "pool")
                      (SnapshotReducer.poolKeyType, Some(x.avroType))

                    case _ => throw new IllegalArgumentException(s"--snapshot (-s) requested $cellPoolName, but no such cell or pool exists in the reducer engine")
                  }
                case None => (reducer.outputType, None)
              }

              // all configuration variables must be set before instantiating a Job
              val job = new Job(configuration, "hadrian-mr-mapreduce")

              // set input and output file paths
              FileInputFormat.setInputPaths(job, new Path(scoreOptions.input))
              FileOutputFormat.setOutputPath(job, new Path(scoreOptions.output))

              // configure the inputs and outputs at each step
              job.setMapperClass(classOf[ScoreMapper])
              job.setInputFormatClass(classOf[MapperKeyInputFormat[_]])
              AvroJob.setInputKeySchema(job, mapper.inputType.schema)
              AvroJob.setMapOutputKeySchema(job, mapperKey.schema)
              AvroJob.setMapOutputValueSchema(job, mapperValue.schema)

              reducerValueOutput match {
                // scoring engine
                case None =>
                  if (reducer.method == Method.FOLD) {
                    job.setReducerClass(classOf[FoldReducer])
                    job.setOutputFormatClass(classOf[AvroKeyValueOutputFormat[_, _]])
                    AvroJob.setOutputKeySchema(job, AvroString().schema)
                    AvroJob.setOutputValueSchema(job, reducer.outputType.schema)
                  }
                  else {
                    job.setReducerClass(classOf[ScoreReducer])
                    job.setOutputFormatClass(classOf[AvroKeyOutputFormat[_]])
                    AvroJob.setOutputKeySchema(job, reducer.outputType.schema)
                  }

                // snapshot engine
                case Some(cellPoolType) =>
                  job.setReducerClass(classOf[SnapshotReducer])
                  job.setOutputFormatClass(classOf[AvroKeyValueOutputFormat[_, _]])
                  AvroJob.setOutputKeySchema(job, reducerKeyOutput.schema)
                  AvroJob.setOutputValueSchema(job, cellPoolType.schema)
              }

              // possibly set secondary sort
              mapperKey match {
                case AvroString() =>
                  // for string-valued keys, the defaults are correct: groups by key with no secondary sorting

                case AvroRecord(fields, _, _, _, _) if (fields exists {x => x.name == "groupby"  &&  x.avroType.isInstanceOf[AvroString]}) =>
                  // for composite keys, the default sorting is correct but the grouping needs to be limited to the "groupby" field only
                  // NOTE: calls to AvroJob override these settings, so they must be placed AFTER all AvroJob calls!
                  job.setPartitionerClass(classOf[RecordKeyPartitioner[_]])
                  job.setGroupingComparatorClass(classOf[RecordKeyGrouper])

                case _ =>
                  throw new IllegalArgumentException("mapper output key is %s but must be a string or a record containing a string-valued field named \"groupby\"".format(mapperValue))
              }

              // run!
              job.setJarByClass(classOf[ScoreJob])
              job.waitForCompletion(true)
              0

            // this is a mapper resulting in key-value pairs that pass through an identity reducer (they will be grouped; they may be secondary sorted)
            case Some(reducerFileName) if (reducerFileName == "") =>
              scoreOptions.numReducers foreach {x =>
                if (x < 1)
                  throw new IllegalArgumentException("--identity-reducer is specified, so --num-reducers must be at least 1")
                else
                  configuration.set("mapred.reduce.tasks", x.toString)
              }

              configuration.set("mapreduce.task.classpath.user.precedence", "true")
              configuration.set("mapreduce.job.user.classpath.first", "true")

              configuration.set("pfa.mapper", scoreOptions.mapper)

              // all configuration variables must be set before instantiating a Job
              val job = new Job(configuration, "hadrian-mr-mapidentity")

              // load the models to ensure that they are valid before submitting a job
              val mapper = getMapper(configuration)

              // set input and output file paths
              FileInputFormat.setInputPaths(job, new Path(scoreOptions.input))
              FileOutputFormat.setOutputPath(job, new Path(scoreOptions.output))

              // PFA semantics checks for the map-reduce workflow
              val (mapperKey, mapperValue) = mapper.outputType match {
                case AvroRecord(Seq(AvroField("key", k, _, _, _, _), AvroField("value", v, _, _, _, _)), _, _, _, _) => (k, v)
                case AvroRecord(Seq(AvroField("value", v, _, _, _, _), AvroField("key", k, _, _, _, _)), _, _, _, _) => (k, v)
                case x => throw new IllegalArgumentException("mapper must output a (key, value) record, not\n%s".format(x))
              }
              if (mapper.method == Method.FOLD)
                throw new IllegalArgumentException("mapper may only be method: map or method: emit, not method: fold")

              // configure the inputs and outputs at each step
              job.setMapperClass(classOf[ScoreMapper])
              job.setReducerClass(classOf[AvroIdentityReducer])

              job.setInputFormatClass(classOf[MapperKeyInputFormat[_]])
              AvroJob.setInputKeySchema(job, mapper.inputType.schema)

              AvroJob.setMapOutputKeySchema(job, mapperKey.schema)
              AvroJob.setMapOutputValueSchema(job, mapperValue.schema)

              job.setOutputFormatClass(classOf[AvroKeyValueOutputFormat[_, _]])
              AvroJob.setOutputKeySchema(job, mapperKey.schema)
              AvroJob.setOutputValueSchema(job, mapperValue.schema)

              // possibly set secondary sort
              mapperKey match {
                case AvroString() =>
                  // for string-valued keys, the defaults are correct: groups by key with no secondary sorting

                case AvroRecord(fields, _, _, _, _) if (fields exists {x => x.name == "groupby"  &&  x.avroType.isInstanceOf[AvroString]}) =>
                  // for composite keys, the default sorting is correct but the grouping needs to be limited to the "groupby" field only
                  // NOTE: calls to AvroJob override these settings, so they must be placed AFTER all AvroJob calls!
                  job.setPartitionerClass(classOf[RecordKeyPartitioner[_]])
                  job.setGroupingComparatorClass(classOf[RecordKeyGrouper])

                case _ =>
                  throw new IllegalArgumentException("mapper output key is %s but must be a string or a record containing a string-valued field named \"groupby\"".format(mapperValue))
              }

              // run!
              job.setJarByClass(classOf[ScoreJob])
              job.waitForCompletion(true)
              0
          }

        case None => -1   // arguments could not be parsed; error message has already been displayed

        case _ =>   // arguments could not be parsed; error message has not been displayed
          optionParser.showUsageAsError
          -1
      }
    }
  }

  object Main {
    //////////////////////////////////////////////////////////// process Java and Hadoop options first, then pass the rest on to ScoreJob
    def main(args: Array[String]) {
      val otherArgs: Array[String] = new GenericOptionsParser(configuration, args).getRemainingArgs
      ToolRunner.run(new ScoreJob, otherArgs)
    }
  }
}
