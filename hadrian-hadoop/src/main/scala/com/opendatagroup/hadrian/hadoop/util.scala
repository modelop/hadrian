package com.opendatagroup.hadrian

import scala.collection.JavaConversions._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.fs.PathFilter
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer

import org.apache.avro.file.DataFileStream
import org.apache.avro.generic.GenericDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapred.AvroValue
import org.apache.avro.mapreduce.AvroJob
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.avro.mapreduce.AvroKeyOutputFormat
import org.apache.avro.mapreduce.AvroKeyRecordReader
import org.apache.avro.mapreduce.AvroKeyValueOutputFormat
import org.apache.avro.Schema
import org.apache.hadoop.fs.FileSystem

import com.opendatagroup.hadrian.ast.EngineConfig
import com.opendatagroup.hadrian.data.PFADatumReader
import com.opendatagroup.hadrian.data.PFARecord
import com.opendatagroup.hadrian.data.toJson
import com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine

package object hadoop {
  var conf: Configuration = null

  def loadEngine(location: String, configuration: Configuration): PFAEngine[AnyRef, AnyRef] = {
    val fs = org.apache.hadoop.fs.FileSystem.get(configuration)

    System.err.println(s"Loading engine file -> "+location)
    
    val inputStream = fs.open(new Path(location))

    System.err.println(s"Found engine.")

    if (location.endsWith(".yaml"))
      PFAEngine.fromYaml(new java.util.Scanner(inputStream).useDelimiter("\\Z").next()).head
    else
      PFAEngine.fromJson(inputStream).head
  }

  private var _mapperEngine: PFAEngine[AnyRef, AnyRef] = null
  def mapperEngine(conf: Configuration): PFAEngine[AnyRef, AnyRef] = {
    if (_mapperEngine == null)
      _mapperEngine = loadEngine(conf.get("pfa.mapper"), conf)
    _mapperEngine
  }

  private var _reducerEngine: PFAEngine[AnyRef, AnyRef] = null
  def reducerEngine(conf: Configuration): PFAEngine[AnyRef, AnyRef] = {
    if (_reducerEngine == null)
      _reducerEngine = loadEngine(conf.get("pfa.reducer"), conf)
    _reducerEngine
  }


  def inputPaths(inputData: String, fs: FileSystem): String =
    if (inputData.contains(","))
      inputData
    else
      recursiveFiles(new Path(inputData), fs, {path: Path => path.getName.endsWith(".avro")}).mkString(",")


  def recursiveFiles(path: Path, fileSystem: FileSystem, filter: Path => Boolean): Seq[Path] = {
    val pathFilter = new PathFilter {
      def accept(p: Path): Boolean = fileSystem.isDirectory(p)  ||  filter(p)
    }
    if (fileSystem.isDirectory(path))
      fileSystem.listStatus(path, pathFilter) flatMap {fileStatus =>
        if (fileStatus.isDirectory)
          recursiveFiles(fileStatus.getPath, fileSystem, filter)
        else
          Vector(fileStatus.getPath)
      }
    else {
      if (filter(path))
        Vector(path)
      else
        Vector[Path]()
    }
  }

  def collectIntoMergedModel(fs: FileSystem, reducerAST: EngineConfig, segmentationPool: String, segmentationSchema: Schema, outputParameters: String, mergedModelPath: Path) {
    val mapBuilder = Map.newBuilder[String, String]

    val cwd = fs.getWorkingDirectory
    fs.setWorkingDirectory(new Path(outputParameters))
    val fileList = fs.listStatus(new Path("."), new PathFilter {
      def accept(path: Path): Boolean = path.getName.startsWith("part-")
    })
    for (fileStatus <- fileList if (fileStatus.isFile)) {
      val datumReader = new GenericDatumReader[GenericRecord]
      val dataFileStream = new DataFileStream[GenericRecord](fs.open(fileStatus.getPath), datumReader)
      for (poolItem <- asScalaIterator(dataFileStream))
        mapBuilder += (poolItem.get("key").toString -> toJson(poolItem.get("value"), segmentationSchema))
    }
    fs.setWorkingDirectory(cwd)

    val newPoolInit = mapBuilder.result
    val oldPoolInit = reducerAST.pools(segmentationPool).init

    val newPool = reducerAST.pools(segmentationPool).copy(init = oldPoolInit ++ newPoolInit)
    val newPools = reducerAST.pools.updated(segmentationPool, newPool)
    val newConfig = reducerAST.copy(pools = newPools)

    val outputModelWriter = new java.io.PrintWriter(fs.create(mergedModelPath))
    outputModelWriter.println(newConfig.toJson(true))
    outputModelWriter.close()
  }

  //////////////////////////////////////////////////////////// mapper used for scoring and training
  class ScoreMapper extends Mapper[AvroKey[AnyRef], NullWritable, AvroKey[AnyRef], AvroValue[AnyRef]] {
    type Context = Mapper[AvroKey[AnyRef], NullWritable, AvroKey[AnyRef], AvroValue[AnyRef]]#Context

    override def setup(context: Context) {
      mapperEngine(context.getConfiguration).begin()
    }

    override def map(wrappedInput: AvroKey[AnyRef], nullWritable: NullWritable, context: Context) {
      mapperEngine(context.getConfiguration) match {
        case emitEngine: PFAEmitEngine[_, _] =>
          // (might be a different context each time)
          emitEngine.emit = {result: AnyRef =>
            val key = result.asInstanceOf[PFARecord].get("key")
            val value = result.asInstanceOf[PFARecord].get("value")
            context.write(new AvroKey[AnyRef](key), new AvroValue[AnyRef](value))
          }
          emitEngine.action(wrappedInput.datum)

        case otherEngine =>
          val result = otherEngine.action(wrappedInput.datum)
          val key = result.asInstanceOf[PFARecord].get("key")
          val value = result.asInstanceOf[PFARecord].get("value")
          context.write(new AvroKey(key), new AvroValue(value))
      }
    }
  }

  //////////////////////////////////////////////////////////// overridden classes to load data into mapper efficiently
  class PFAKeyRecordReader[T](readerSchema: Schema, conf: Configuration) extends AvroKeyRecordReader[T](readerSchema) {
    import org.apache.avro.file.SeekableInput
    import org.apache.avro.file.DataFileReader
    import org.apache.avro.io.DatumReader
    override def createAvroFileReader(input: SeekableInput, datumReader: DatumReader[T]): DataFileReader[T] = {
      // override the generic DatumReader
      val pfaReader = new PFADatumReader[T](mapperEngine(conf).specificData)
      pfaReader.setSchema(readerSchema)
      new DataFileReader[T](input, pfaReader)
    }
  }

  class PFAKeyInputFormat[T] extends AvroKeyInputFormat[T] {
    import org.apache.hadoop.mapreduce.InputSplit
    import org.apache.hadoop.mapreduce.TaskAttemptContext
    import org.apache.hadoop.mapreduce.RecordReader
    override def createRecordReader(split: InputSplit, context: TaskAttemptContext): RecordReader[AvroKey[T], NullWritable] = {
      // return a subclass of AvroKeyRecordReader to override the generic DatumReader
      val conf = context.getConfiguration
      val readerSchema = AvroJob.getInputKeySchema(conf)
      new PFAKeyRecordReader[T](readerSchema, conf)
    }
  }
}
