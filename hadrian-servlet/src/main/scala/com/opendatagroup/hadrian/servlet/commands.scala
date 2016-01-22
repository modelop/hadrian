package com.opendatagroup.hadrian.servlet

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.postfixOps

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.ArrayNode
import org.codehaus.jackson.node.BooleanNode
import org.codehaus.jackson.node.IntNode
import org.codehaus.jackson.node.LongNode
import org.codehaus.jackson.node.JsonNodeFactory
import org.codehaus.jackson.node.ObjectNode
import org.codehaus.jackson.node.TextNode
import org.codehaus.jackson.node.NullNode
import org.codehaus.jackson.node.NumericNode

import org.yaml.snakeyaml.error.YAMLException
import org.codehaus.jackson.JsonParseException
import org.apache.avro.SchemaParseException
import org.apache.avro.AvroTypeException
import com.opendatagroup.hadrian.errors.PFASyntaxException
import com.opendatagroup.hadrian.errors.PFASemanticException
import com.opendatagroup.hadrian.errors.PFAInitializationException

import com.opendatagroup.hadrian.ast
import com.opendatagroup.hadrian.ast.EngineConfig
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine
import com.opendatagroup.hadrian.yaml.yamlToJson
import com.opendatagroup.hadrian.memory.EnginesReport

import com.opendatagroup.hadrian.servlet.engine._
import com.opendatagroup.hadrian.servlet.datastream.filestream

package object commands {
  // for working with JSON
  private[servlet] val jsonNodeFactory = JsonNodeFactory.instance
  private[servlet] val objectMapper = new ObjectMapper
}

package commands {
  abstract class Command extends HttpServlet {
    override def doOptions(request: HttpServletRequest, response: HttpServletResponse) {
      super.doOptions(request, response)
      // Allow requests from anywhere, POST only.
      response.setHeader("Access-Control-Allow-Origin", "*")
      response.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
      response.setHeader("Access-Control-Allow-Methods", "POST")
    }

    def command: String

    def data(request: HttpServletRequest) =
      try {
        Some(new java.util.Scanner(request.getInputStream).useDelimiter("\\A").next)
      }
      catch {
        case _: java.util.NoSuchElementException => None
      }

    def logInfo(argument: Option[String], message: String) {
      logger.info(command + argument.map(": " + _).mkString + " -> " + message)
    }

    def logError(argument: Option[String], message: String, traceback: String) {
      logger.error(command + argument.map(": " + _).mkString + " -> " + message + (if (traceback.isEmpty) "" else "\n") + traceback)
    }

    def reportInfo(argument: Option[String], message: String, outputJsonNode: ObjectNode) {
      outputJsonNode.put("result", "success")
      outputJsonNode.put("success", message)
      logInfo(argument, message)
    }

    def reportError(argument: Option[String], message: String, error: Option[Exception], outputJsonNode: ObjectNode) {
      outputJsonNode.put("result", "error")
      outputJsonNode.put("error", message)
      error match {
        case Some(err) =>
          val traceback = exceptionTraceback(err)
          outputJsonNode.put("exception", err.toString)
          outputJsonNode.put("traceback", traceback)
          logError(argument, message, traceback)
        case None =>
          logError(argument, message, "")
      }
    }

    def setHeaders(response: HttpServletResponse) {
      response.setHeader("Access-Control-Allow-Origin", "*")
      response.setContentType("application/json")
    }

    protected[Command] def status = {
      val outputJsonNode = jsonNodeFactory.objectNode

      outputJsonNode.put("numberOfInstancesRunning", new IntNode(engineThreads.size))

      val instances = jsonNodeFactory.arrayNode
      engineThreads foreach {engineThread =>
        instances.add(new IntNode(engineThread.eng.instance))
      }
      outputJsonNode.put("instancesRunning", instances)

      val ops = jsonNodeFactory.objectNode
      options foreach {case (k, v) => ops.put(k, v)}
      outputJsonNode.put("serverOptions", ops)

      outputJsonNode.put("pfaVersion", pfaVersion)
      
      engineConfig match {
        case Some(c) =>
          val pfaDocumentNode = jsonNodeFactory.objectNode

          pfaDocumentNode.put("name", new TextNode(c.name))
          pfaDocumentNode.put("method", new TextNode(c.method.toString.toLowerCase))
          pfaDocumentNode.put("input", c.input.jsonNode(mutable.Set[String]()))
          pfaDocumentNode.put("output", c.output.jsonNode(mutable.Set[String]()))
          pfaDocumentNode.put("numberOfFcns", new IntNode(c.fcns.size))
          pfaDocumentNode.put("numberOfCells", new IntNode(c.cells.size))
          pfaDocumentNode.put("numberOfPools", new IntNode(c.pools.size))
          pfaDocumentNode.put("randseed", c.randseed match {case None => null; case Some(x) => new LongNode(x)})
          pfaDocumentNode.put("doc", c.doc match {case None => null; case Some(x) => new TextNode(x)})
          pfaDocumentNode.put("version", c.version match {case None => null; case Some(x) => new IntNode(x)})

          val metadata = jsonNodeFactory.objectNode
          c.metadata foreach {case (k, v) => metadata.put(k, new TextNode(v))}
          pfaDocumentNode.put("metadata", metadata)

          val ops = jsonNodeFactory.objectNode
          c.options foreach {case (k, v) => ops.put(k, v)}
          pfaDocumentNode.put("options", ops)

          outputJsonNode.put("pfaDocument", pfaDocumentNode)

        case None =>
          outputJsonNode.put("pfaDocument", NullNode.getInstance)
      }

      dataQualityMetric match {
        case Some(q) =>
          val dataQualityNode = jsonNodeFactory.objectNode

          dataQualityNode.put("name", q.name)

          val truePath = jsonNodeFactory.arrayNode
          q.truePath foreach {
            case Left(i) => truePath.add(new IntNode(i))
            case Right(s) => truePath.add(new TextNode(s))
          }
          dataQualityNode.put("truePath", truePath)

          val predictedPath = jsonNodeFactory.arrayNode
          q.predictedPath foreach {
            case Left(i) => predictedPath.add(new IntNode(i))
            case Right(s) => predictedPath.add(new TextNode(s))
          }
          dataQualityNode.put("predictedPath", predictedPath)

          dataQualityNode.put("applicable", if (dataQualityMetricApplicable) BooleanNode.getTrue else BooleanNode.getFalse)

          outputJsonNode.put("dataQualityMetric", dataQualityNode)

        case None =>
          outputJsonNode.put("dataQualityMetric", NullNode.getInstance)
      }

      dataInputStreamConfig match {
        case Some(x) =>
          val inputStreamNode = jsonNodeFactory.objectNode
          inputStreamNode.put("config", x);
          inputStreamNode.put("loop", if (dataInputStreamLoop) BooleanNode.getTrue else BooleanNode.getFalse)
          dataInputStreamCompatible match {
            case Some(true) => inputStreamNode.put("schemaCompatible", BooleanNode.getTrue)
            case Some(false) => inputStreamNode.put("schemaCompatible", BooleanNode.getFalse)
            case None => inputStreamNode.put("schemaCompatible", "unknown")
          }
          outputJsonNode.put("dataInputStream", inputStreamNode)

        case None =>
          outputJsonNode.put("dataInputStream", NullNode.getInstance)
      }

      dataOutputStreamConfig match {
        case Some(x) =>
          val outputStreamNode = jsonNodeFactory.objectNode
          outputStreamNode.put("config", x)
          outputJsonNode.put("dataOutputStream", outputStreamNode)

        case None =>
          outputJsonNode.put("dataOutputStream", NullNode.getInstance)
      }

      outputJsonNode
    }
  }

  class Status extends Command {
    val command = "status"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val result = objectMapper.writeValueAsString(status)
      reportInfo(None, result, jsonNodeFactory.objectNode)
      setHeaders(response)
      response.getWriter.println(result)
    }
  }

  // general case: the PFA is in JSON format
  class LoadPfa extends Command {
    val command = "load-pfa"

    protected[LoadPfa] def identical(engineAST: EngineConfig) = {
      // determine if all the engines are (and will always be) identical, so that snapshots are only a single PFA
      val unsharedCells = engineAST.cells filter {case (name, cell) => !cell.shared} keySet
      val unsharedPools = engineAST.pools filter {case (name, pool) => !pool.shared} keySet
      val modifiedUnsharedCells = engineAST collect {case ast.CellTo(name, _, _, _) if unsharedCells contains name => name}
      val modifiedUnsharedPools = engineAST collect {case ast.PoolTo(name, _, _, _, _) if unsharedPools contains name => name}
      modifiedUnsharedCells.isEmpty  &&  modifiedUnsharedPools.isEmpty
    }

    protected[LoadPfa] def load(inputStream: java.io.InputStream, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode

      try {
        val newEngineFactory = PFAEngine.factoryFromJson(inputStream, options, pfaVersion)
        val numberOfEngines = if (engineThreads.isEmpty) 1 else engineThreads.size
        val newEngines = Vector.fill(numberOfEngines)(newEngineFactory())
        val t = newEngines.map(EngineThread(_))
        val i = identical(newEngines.head.config)
        val n = newEngines.head.config.name
        engineThreads.synchronized {
          throughputTotalCount = 0L
          throughputTotalNanosecs = 0L
          engineFactory = Some(newEngineFactory)
          engineConfig = Some(newEngines.head.config)
          engineThreads = t
          enginesIdentical = i
          dataQualityMetric foreach {metric =>
            dataQualityMetricApplicable = metric.applicable(engineConfig.get)
            metric.reset()
          }
        }
        outputJsonNode.put("result", "success")
        outputJsonNode.put("success", s"""Compiled PFA and created $numberOfEngines new engine${if (numberOfEngines > 1) "s" else ""}""")
        reportInfo(None, "successfully compiled " + n, outputJsonNode)
      }
      catch {
        case err: JsonParseException =>
          reportError(None, "The document is not valid JSON", Some(err), outputJsonNode)
        case err: SchemaParseException =>
          reportError(None, "One of the type specifications in the PFA document is not a valid Avro schema", Some(err), outputJsonNode)
        case err: AvroTypeException =>
          reportError(None, "An embedded JSON value does not match its Avro schema", Some(err), outputJsonNode)
        case err: PFASyntaxException =>
          reportError(None, "The document is not valid PFA; it has a syntax error", Some(err), outputJsonNode)
        case err: PFASemanticException =>
          reportError(None, "The document is not valid PFA; it has a semantic error", Some(err), outputJsonNode)
        case err: PFAInitializationException =>
          reportError(None, "The document is not valid PFA; it has an initialization error", Some(err), outputJsonNode)
        case err: Exception =>
          reportError(None, "Unexpected error", Some(err), outputJsonNode)
      }

      outputJsonNode.put("status", status)

      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      load(request.getInputStream, response)
    }
  }

  // special case: the PFA is in YAML format
  class LoadPfaFromYaml extends LoadPfa {
    override val command = "load-pfa-as-yaml"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      try {
        load(new java.io.ByteArrayInputStream(yamlToJson(request.getInputStream).getBytes), response)
      }
      catch {
        case err: YAMLException =>
          val outputJsonNode = jsonNodeFactory.objectNode
          reportError(None, "The document is not valid YAML", Some(err), outputJsonNode)
          setHeaders(response)
          response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))

        // other error cases are handled by loadPfa.load
      }
    }
  }

  class ChangeNumberOfInstances extends Command {
    val command = "change-number-of-instances"

    // NOTE: must be called within engines.synchronized
    protected[ChangeNumberOfInstances] def change(number: Int, newEngine: () => PFAEngine[AnyRef, AnyRef]) {
      if (number > engineThreads.size) {
        // put new engines at the end of the vector
        engineThreads = engineThreads ++ Vector.fill(number - engineThreads.size)(EngineThread(newEngine()))
      }
      else if (number < engineThreads.size) {
        // stop and drop old engines from the beginning of the vector
        val numberToDrop = engineThreads.size - number
        engineThreads.take(numberToDrop) map {_.mailbox.stop = true}
        engineThreads = engineThreads.drop(numberToDrop)
      }
    }

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      engineThreads.synchronized {
        val outputJsonNode = jsonNodeFactory.objectNode

        val argument = data(request)

        (engineFactory, argument) match {
          case (Some(newEngine), Some(arg)) =>
            try {
              val number = arg.toInt
              if (number < 0)
                reportError(argument, "The number provided is negative", None, outputJsonNode)
              else {
                change(number, newEngine)
                reportInfo(argument, s"Number of instances is now ${engineThreads.size}", outputJsonNode)
              }
            }
            catch {
              case err: java.lang.NumberFormatException =>
                reportError(argument, "The text provided is not an integer", Some(err), outputJsonNode)
            }
          case (Some(_), None) =>
            reportError(argument, "Number of desired instances has not been provided", None, outputJsonNode)
          case (None, _) =>
            reportError(argument, "No PFA engine has been loaded yet", None, outputJsonNode)
        }

        outputJsonNode.put("status", status)

        setHeaders(response)
        response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
      }
    }
  }

  class IncreaseNumberOfInstances extends ChangeNumberOfInstances {
    override val command = "increase-number-of-instances"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      engineThreads.synchronized {
        val outputJsonNode = jsonNodeFactory.objectNode

        engineFactory match {
          case Some(newEngine) =>
            change(engineThreads.size + 1, newEngine)
            reportInfo(None, s"Number of instances is now ${engineThreads.size}", outputJsonNode)
          case None =>
            reportError(None, "No PFA engine has been loaded yet", None, outputJsonNode)
        }

        outputJsonNode.put("status", status)

        setHeaders(response)
        response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
      }
    }
  }

  class DecreaseNumberOfInstances extends ChangeNumberOfInstances {
    override val command = "decrease-number-of-instances"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      engineThreads.synchronized {
        val outputJsonNode = jsonNodeFactory.objectNode

        engineFactory match {
          case Some(newEngine) =>
            val number = engineThreads.size - 1
            if (number < 0)
              reportError(None, "Cannot decrease number of instances when it is zero", None, outputJsonNode)
            else {
              change(number, newEngine)
              reportInfo(None, s"Number of instances is now ${engineThreads.size}", outputJsonNode)
            }
          case None =>
            reportError(None, "No PFA engine has been loaded yet", None, outputJsonNode)
        }

        outputJsonNode.put("status", status)

        setHeaders(response)
        response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
      }
    }
  }

  class AttachFileInput extends Command {
    val command = "attach-file-input"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode
      val argument = data(request)

      argument match {
        case Some(fileName) =>
          try {
            dataInputStream.synchronized {
              val file = new java.io.File(fileName)
              dataInputStream = Some(filestream.jsonFileInputDataStream(file))

              val tmp = jsonNodeFactory.objectNode
              tmp.put("type", "json-file")
              tmp.put("fileName", file.getAbsolutePath)
              dataInputStreamConfig = Some(tmp)

              (engineConfig, dataInputStream.get.avroType) match {
                case (Some(ec), Some(t)) =>
                  dataInputStreamCompatible = Some(ec.input.accepts(t))
                case _ =>
                  dataInputStreamCompatible = None
              }
            }
            reportInfo(argument, s"""Attached file \"$fileName\" as input data source""", outputJsonNode)
          }
          catch {
            case err: java.io.FileNotFoundException =>
              reportError(argument, s"""Path to \"$fileName\" not found on server""", Some(err), outputJsonNode)
            case err: Exception =>
              reportError(argument, "Unexpected error", Some(err), outputJsonNode)
          }
        case None =>
          reportError(argument, "File name must be specified", None, outputJsonNode)
      }

      outputJsonNode.put("status", status)

      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class AttachFileOutput extends Command {
    val command = "attach-file-output"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode
      val argument = data(request)

      argument match {
        case Some(fileName) =>
          try {
            dataOutputStream.synchronized {
              val file = new java.io.File(fileName)
              dataOutputStream = Some(filestream.jsonFileOutputDataStream(file))

              val tmp = jsonNodeFactory.objectNode
              tmp.put("type", "json-file")
              tmp.put("fileName", file.getAbsolutePath)
              dataOutputStreamConfig = Some(tmp)
            }
            reportInfo(argument, s"""Attached file \"$fileName\" as output data sink""", outputJsonNode)
          }
          catch {
            case err: java.io.FileNotFoundException =>
              reportError(argument, s"""Path for \"$fileName\" not found or permission denied on server""", Some(err), outputJsonNode)
            case err: Exception =>
              reportError(argument, "Unexpected error", Some(err), outputJsonNode)
          }
        case None =>
          reportError(argument, "File name must be specified", None, outputJsonNode)
      }

      outputJsonNode.put("status", status)

      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class InputLoopOn extends Command {
    val command = "input-loop-on"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode
      dataInputStreamLoop = true
      reportInfo(None, "Turned input data loop on", outputJsonNode)
      outputJsonNode.put("status", status)
      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class InputLoopOff extends Command {
    val command = "input-loop-off"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode
      dataInputStreamLoop = false
      reportInfo(None, "Turned input data loop off", outputJsonNode)
      outputJsonNode.put("status", status)
      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class SetTimeout extends Command {
    val command = "set-default-timeout"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode
      val argument = data(request)

      argument match {
        case Some(arg) =>
          try {
            val timeout = arg.toInt
            options = options.updated("timeout", new IntNode(timeout))
            reportInfo(argument, s"Set PFA timeout to $timeout milliseconds for the next engine that will be loaded", outputJsonNode)
          }
          catch {
            case err: java.lang.NumberFormatException =>
              reportError(argument, "Timeout must be a number in milliseconds", Some(err), outputJsonNode)
            case err: Exception =>
              reportError(argument, "Unexpected error", Some(err), outputJsonNode)
          }
        case None =>
          reportError(argument, "Timeout must be specified", None, outputJsonNode)
      }

      outputJsonNode.put("status", status)

      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class SetPfaVersion extends Command {
    val command = "set-default=pfa-version"
    val PFAVersionPattern = """([0-9]+\.[0-9]+\.[0-9]+)""".r

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode
      val argument = data(request)

      argument match {
        case Some(PFAVersionPattern(x)) =>
          pfaVersion = x
          reportInfo(argument, s"Set PFA version to $pfaVersion for the next engine that will be loaded", outputJsonNode)
        case _ =>
          reportError(argument, "PFA version must be specified as xx.yy.zz where xx, yy, and zz are integers", None, outputJsonNode)
      }

      outputJsonNode.put("status", status)

      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class Snapshot extends Command {
    val command = "snapshot"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode

      val enginesToSnapshot =
        if (engineThreads.isEmpty)
          Map[String, PFAEngine[AnyRef, AnyRef]]()
        else if (enginesIdentical)
          Map("shared" -> engineThreads.head.eng)
        else
          engineThreads.map(x => x.eng.instance.toString -> x.eng).toMap

      // do them all quickly (no serialization in this step)
      val snapshots = enginesToSnapshot map {case (k, v) => k -> v.snapshot}

      if (enginesToSnapshot.keySet == Set[String]())
        reportError(None, "No PFA engines are running, so no snapshot can be taken", None, outputJsonNode)

      else if (enginesToSnapshot.keySet == Set[String]("shared")) {
        reportInfo(None, "Current PFA engines are identical, so only one snapshot returned", outputJsonNode)
        outputJsonNode.put("snapshot", snapshots("shared").jsonNode(true, mutable.Set[String]()))
      }

      else {
        reportInfo(None, "Current PFA engines have non-shared mutable state, so a map from instance number to snapshots returned", outputJsonNode)
        val snapshotsJson = jsonNodeFactory.objectNode
        snapshots foreach {case (k, v) => snapshotsJson.put(k, v.jsonNode(true, mutable.Set[String]()))}
        outputJsonNode.put("snapshot", snapshotsJson)
      }

      outputJsonNode.put("status", status)

      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class StreamLogfile extends Command {
    val command = "stream-logfile"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val argument = data(request)
      response.setHeader("Access-Control-Allow-Origin", "*")
      response.setContentType("text/plain")
      val writer = response.getWriter

      argument match {
        case Some(arg) =>
          try {
            val timeout = arg.toInt
            writer.println(s"Streaming PFA log output for %d milliseconds...")
            logPFAToWebUser = Some({x => writer.println(x); writer.flush()})
            Thread.sleep(timeout)
            logPFAToWebUser = None
          }
          catch {
            case err: java.lang.NumberFormatException =>
              writer.println("ERROR: timeout must be a number in milliseconds")
          }
        case None =>
          writer.println("ERROR: timeout not provided")
      }
    }
  }

  class GetPerformanceStatistics extends Command {
    val command = "get-performance-statistics"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode

      outputJsonNode.put("throughputTotalCount", new LongNode(throughputTotalCount))
      outputJsonNode.put("throughputTotalNanosecs", new LongNode(throughputTotalNanosecs))

      val instancesNode = jsonNodeFactory.objectNode
      engineThreads foreach {engineThread =>
        instancesNode.put(engineThread.eng.instance.toString, engineThread.performance)
      }
      outputJsonNode.put("instances", instancesNode)

      logInfo(None, objectMapper.writeValueAsString(outputJsonNode))

      outputJsonNode.put("status", status)

      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class ResetPerformanceStatistics extends Command {
    val command = "reset-performance-statistics"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode

      throughputTotalCount = 0L
      throughputTotalNanosecs = 0L
      engineThreads foreach {engineThread =>
        engineThread.resetPerformance()
      }

      reportInfo(None, "Reset all performance counters to zero", outputJsonNode)
      outputJsonNode.put("status", status)
      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class GetMemoryUsage extends Command {
    val command = "get-memory-usage"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode

      if (engineThreads.isEmpty)
        reportError(None, "No PFA engines are running, so a memory usage report cannot be generated", None, outputJsonNode)
      else {
        val enginesReport = EnginesReport(engineThreads.map(_.eng))

        val sharedCells = jsonNodeFactory.arrayNode
        enginesReport.sharedCells.values.toSeq.sortBy(_.name) foreach {cellReport =>
          val cellNode = jsonNodeFactory.objectNode
          cellNode.put("name", cellReport.name)
          cellNode.put("usage", cellReport.usage.toString)
          cellNode.put("bytes", new LongNode(cellReport.usage.bytes))
          cellNode.put("exact", if (cellReport.usage.exact) BooleanNode.getTrue else BooleanNode.getFalse)
          sharedCells.add(cellNode)
        }
        outputJsonNode.put("sharedCells", sharedCells)

        val sharedPools = jsonNodeFactory.arrayNode
        enginesReport.sharedPools.values.toSeq.sortBy(_.name) foreach {poolReport =>
          val poolNode = jsonNodeFactory.objectNode
          poolNode.put("name", poolReport.name)
          poolNode.put("usage", poolReport.usage.toString)
          poolNode.put("bytes", new LongNode(poolReport.usage.bytes))
          poolNode.put("exact", if (poolReport.usage.exact) BooleanNode.getTrue else BooleanNode.getFalse)
          poolNode.put("items", new LongNode(poolReport.items))
          sharedPools.add(poolNode)
        }
        outputJsonNode.put("sharedPools", sharedPools)

        val unsharedCells = jsonNodeFactory.objectNode
        enginesReport.unsharedCells.toSeq.sortBy(_._1) foreach {case (instance, cellReports) =>
          val cellsNode = jsonNodeFactory.arrayNode
          cellReports foreach {case (_, cellReport) =>
            val cellNode = jsonNodeFactory.objectNode
            cellNode.put("name", cellReport.name)
            cellNode.put("usage", cellReport.usage.toString)
            cellNode.put("bytes", new LongNode(cellReport.usage.bytes))
            cellNode.put("exact", if (cellReport.usage.exact) BooleanNode.getTrue else BooleanNode.getFalse)
            cellsNode.add(cellNode)
          }
          unsharedCells.put(instance.toString, cellsNode)
        }
        outputJsonNode.put("unsharedCells", unsharedCells)

        val unsharedPools = jsonNodeFactory.objectNode
        enginesReport.unsharedPools.toSeq.sortBy(_._1) foreach {case (instance, poolReports) =>
          val poolsNode = jsonNodeFactory.arrayNode
          poolReports foreach {case (_, poolReport) =>
            val poolNode = jsonNodeFactory.objectNode
            poolNode.put("name", poolReport.name)
            poolNode.put("usage", poolReport.usage.toString)
            poolNode.put("bytes", new LongNode(poolReport.usage.bytes))
            poolNode.put("exact", if (poolReport.usage.exact) BooleanNode.getTrue else BooleanNode.getFalse)
            poolNode.put("items", new LongNode(poolReport.items))
            poolsNode.add(poolNode)
          }
          unsharedPools.put(instance.toString, poolsNode)
        }
        outputJsonNode.put("unsharedPools", unsharedPools)

        outputJsonNode.put("totalUsage", enginesReport.total.toString)
        outputJsonNode.put("totalBytes", new LongNode(enginesReport.total.bytes))

        logInfo(None, objectMapper.writeValueAsString(outputJsonNode))
      }

      outputJsonNode.put("status", status)

      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class GetSamplingProfiler extends Command {
    val command = "get-sampling-profiler"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode
      val argument = data(request)

      argument match {
        case Some(arg) =>
          try {
            val maxItems = arg.toInt
            getSamplingProfiler(outputJsonNode, maxItems)
            logInfo(None, "Returning sampling profiler results")
          }
          catch {
            case err: java.lang.NumberFormatException =>
              reportError(argument, "Maximum number of items must be an integer", Some(err), outputJsonNode)
            case err: Exception =>
              reportError(argument, "Unexpected error", Some(err), outputJsonNode)
          }
        case None =>
          reportError(argument, "Maximum number of items must be specified", None, outputJsonNode)
      }

      outputJsonNode.put("status", status)

      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class ResetSamplingProfiler extends Command {
    val command = "reset-sampling-profiler"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode

      resetSamplingProfiler()

      reportInfo(None, "Reset sampling profiler", outputJsonNode)
      outputJsonNode.put("status", status)
      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class DataQualityMetricConfigurationException(message: String) extends Exception(message)

  abstract class InstallDataQualityMetric extends Command {
    def getPath(rootNode: JsonNode, fieldName: String) = rootNode.get(fieldName) match {
      case null => throw new DataQualityMetricConfigurationException(s"""missing "$fieldName"""")
      case arrayNode: ArrayNode => (0 until arrayNode.size).toList map {i =>
        arrayNode.get(i) match {
          case numericNode: NumericNode => Left(numericNode.asInt)
          case textNode: TextNode => Right(textNode.getTextValue)
          case _ => throw new DataQualityMetricConfigurationException(s""""$fieldName" element $i has the wrong JSON type""")
        }
      }
    }

    def pathString(path: Path) = path map {
      case Left(i) => i.toString
      case Right(s) => "\"" + s + "\""
    } mkString(", ")
  }

  class InstallResiduals extends InstallDataQualityMetric {
    val command = "install-residuals"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode
      val argument = data(request)

      try {
        argument match {
          case Some(arg) =>
            val rootNode = objectMapper.readTree(arg)
            val truePath: Path = getPath(rootNode, "truePath")
            val predictedPath: Path = getPath(rootNode, "predictedPath")

            dataQualityMetric = Some(Residuals(truePath, predictedPath))

            engineConfig match {
              case Some(ec) =>
                dataQualityMetricApplicable = dataQualityMetric.get.applicable(ec)
              case None =>
                dataQualityMetricApplicable = false
            }

            reportInfo(argument, s"Installed residuals with truePath [${pathString(truePath)}] and predictedPath [${pathString(predictedPath)}]", outputJsonNode)

          case _ =>
            reportError(None, """A request must be provided to specify the truePath and predictedPath""", None, outputJsonNode)
        }
      }
      catch {
        case err: JsonParseException =>
          reportError(None, """The request must be formatted as {"truePath": [...], "predictedPath": [...]} where [...] is an array of integers and strings""", Some(err), outputJsonNode)
      }

      outputJsonNode.put("status", status)

      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class InstallConfusionMatrix extends InstallDataQualityMetric {
    val command = "install-confusion-matrix"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode
      val argument = data(request)

      try {
        argument match {
          case Some(arg) =>
            val rootNode = objectMapper.readTree(arg)
            val truePath: Path = getPath(rootNode, "truePath")
            val predictedPath: Path = getPath(rootNode, "predictedPath")

            dataQualityMetric = Some(ConfusionMatrix(truePath, predictedPath))

            engineConfig match {
              case Some(ec) =>
                dataQualityMetricApplicable = dataQualityMetric.get.applicable(ec)
              case None =>
                dataQualityMetricApplicable = false
            }

            reportInfo(argument, s"Installed confusion matrix with truePath [${pathString(truePath)}] and predictedPath [${pathString(predictedPath)}]", outputJsonNode)

          case _ =>
            reportError(None, """A request must be provided to specify the truePath and predictedPath""", None, outputJsonNode)
        }
      }
      catch {
        case err: JsonParseException =>
          reportError(None, """The request must be formatted as {"truePath": [...], "predictedPath": [...]} where [...] is an array of integers and strings""", Some(err), outputJsonNode)
      }

      outputJsonNode.put("status", status)

      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class GetDataQuality extends Command {
    val command = "get-data-quality"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode

      dataQualityMetric match {
        case Some(dqm) if (dataQualityMetricApplicable) =>
          outputJsonNode.put("result", "success")
          outputJsonNode.put("success", dqm.get)
          logInfo(None, objectMapper.writeValueAsString(outputJsonNode))

        case Some(dqm) =>
          reportError(None, "The data quality metric is not applicable to this PFA engine", None, outputJsonNode)

        case None =>
          reportError(None, "No data quality metric has been installed", None, outputJsonNode)
      }

      outputJsonNode.put("status", status)

      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }

  class ResetDataQuality extends Command {
    val command = "reset-data-quality"

    override def doPost(request: HttpServletRequest, response: HttpServletResponse) {
      val outputJsonNode = jsonNodeFactory.objectNode

      dataQualityMetric match {
        case Some(dqm) =>
          dqm.reset()
          reportInfo(None, "Data quality metric has been reset", outputJsonNode)
        case None =>
          reportError(None, "No data quality metric has been installed", None, outputJsonNode)
      }

      outputJsonNode.put("status", status)

      setHeaders(response)
      response.getWriter.println(objectMapper.writeValueAsString(outputJsonNode))
    }
  }
}
