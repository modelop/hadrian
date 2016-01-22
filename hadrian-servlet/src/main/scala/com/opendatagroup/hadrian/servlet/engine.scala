package com.opendatagroup.hadrian.servlet

import java.lang.StackTraceElement

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.concurrent
import scala.language.postfixOps
import scala.annotation.tailrec

import org.apache.logging.log4j.LogManager

import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.DoubleNode
import org.codehaus.jackson.node.IntNode
import org.codehaus.jackson.node.LongNode
import org.codehaus.jackson.node.ObjectNode

import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.errors.PFAUserException
import com.opendatagroup.hadrian.errors.PFATimeoutException

import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.datatype.AvroNumber
import com.opendatagroup.hadrian.datatype.AvroRecord
import com.opendatagroup.hadrian.datatype.AvroArray
import com.opendatagroup.hadrian.datatype.AvroMap
import com.opendatagroup.hadrian.datatype.AvroString

import com.opendatagroup.hadrian.ast.EngineConfig
import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAMap
import com.opendatagroup.hadrian.data.PFARecord
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.errors.PFATimeoutException
import com.opendatagroup.hadrian.errors.PFAUserException
import com.opendatagroup.hadrian.jvmcompiler.defaultPFAVersion
import com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine

package object engine {
  // PFA options, configurable by REST
  var options = Map("timeout" -> new IntNode(1000))
  var pfaVersion = defaultPFAVersion

  // logger
  val logger = LogManager.getLogger(getClass())

  // current PFA factory and engines, configurable by REST
  var engineFactory: Option[() => PFAEngine[AnyRef, AnyRef]] = None
  var engineConfig: Option[EngineConfig] = None
  var engineThreads: Vector[EngineThread] = Vector()
  var enginesIdentical = false

  // log splitter
  var logPFAToWebUser: Option[String => Unit] = None
  def logPFA(x: String, ns: Option[String]) {
    val out = ns.map("[" + _ + "] ").mkString + x
    logger.info("PFA: " + out)
    logPFAToWebUser.foreach(_(out))
  }

  // input and output streams, configurable by REST
  type Deserializer = PFAEngine[AnyRef, AnyRef] => AnyRef
  abstract class InputDataStream extends Iterator[Deserializer] {
    def avroType: Option[AvroType]
    def restart()
  }
  type OutputDataStream = (PFAEngine[AnyRef, AnyRef], AnyRef) => Unit

  var dataInputStream: Option[InputDataStream] = None
  var dataInputStreamConfig: Option[JsonNode] = None
  var dataInputStreamCompatible: Option[Boolean] = None
  var dataInputStreamLoop = false

  var dataOutputStream: Option[OutputDataStream] = None
  var dataOutputStreamConfig: Option[JsonNode] = None

  // Mailbox is a simple two-thread actor system: only one thread puts to the mailbox, 
  // while another gets (blocking call) until a datum is available.
  // 
  // If you can figure out how to get the same functionality from .wait() and .notify(),
  // that would be an improvement (no polling). Losses due to polling can be seen in the
  // difference between
  // 
  //     sum_i totalNanosecsDeserializing_i + totalNanosecsInAction_i + totalNanosecsSerializing_i
  //
  // for all engines i and
  // 
  //     throughputTotalNanosecs * engineThreads.size
  // 
  // because the engine performance counters do not include the mailbox wait and the total
  // throughput does.
  // 
  // Note: now the dataQualityMonitor is also included in the total throughput but not
  // totalNanosecsDeserializing_i + totalNanosecsInAction_i + totalNanosecsSerializing_i
  // so be sure to turn off data quality monitors when doing this test.
  // 
  class Mailbox[X] {
    private var content: Option[X] = None
    def isEmpty = content.isEmpty
    var stop = false
    def get = {
      while (isEmpty  &&  !stop) Thread.sleep(1)
      content
    }
    // caller should only put if isEmpty
    def put(x: X) {
      content = Some(x)
    }
    def reset() {
      content = None
    }
  }

  def exceptionTraceback(err: java.lang.Exception) = {
    val stringWriter = new java.io.StringWriter
    err.printStackTrace(new java.io.PrintWriter(stringWriter))
    stringWriter.toString
  }

  type PathItem = Either[Int, String]
  type Path = List[PathItem]

  def extractedType(avroType: AvroType, path: Path): Option[AvroType] = (avroType, path) match {
    case (x: AvroRecord, Left(i) :: rest) if (i < x.fields.size) => extractedType(x.fields(i).avroType, rest)
    case (x: AvroRecord, Right(s) :: rest) if (x.fieldNames contains s) => extractedType(x.field(s).avroType, rest)
    case (x: AvroArray, Left(i) :: rest) => extractedType(x.items, rest)
    case (x: AvroMap, Right(s) :: rest) => extractedType(x.values, rest)
    case (x, Nil) => Some(x)
    case _ => None
  }

  def box(value: Any): AnyRef = value match {
    case x: Boolean => java.lang.Boolean.valueOf(x)
    case x: Int => java.lang.Integer.valueOf(x)
    case x: Long => java.lang.Long.valueOf(x)
    case x: Float => java.lang.Float.valueOf(x)
    case x: Double => java.lang.Double.valueOf(x)
    case x: AnyRef => x
  }

  def extract(obj: AnyRef, path: Path): Option[AnyRef] =
    try {
      (obj, path) match {
        case (x: PFARecord, Left(i) :: rest) => extract(x.get(i), rest)
        case (x: PFARecord, Right(s) :: rest) => extract(x.get(s), rest)
        case (x: PFAArray[_], Left(i) :: rest) => extract(box(x.toVector.apply(i)), rest)
        case (x: PFAMap[_], Right(s) :: rest) => extract(x.toMap.apply(s), rest)
        case (x, Nil) => Some(x)
        case _ => None
      }
    }
    catch {
      case _: java.lang.IndexOutOfBoundsException => None
      case _: java.util.NoSuchElementException => None
    }

  var dataQualityMetric: Option[DataQualityMetric] = None
  var dataQualityMetricApplicable = false

  def dataQuality(inputDatum: AnyRef, outputDatum: AnyRef) {
    if (dataQualityMetricApplicable)
      dataQualityMetric.foreach(_.add(inputDatum, outputDatum))
  }

  trait DataQualityMetric {
    def name: String
    def truePath: Path
    def predictedPath: Path
    def applicable(engineConfig: EngineConfig): Boolean
    def add(inputDatum: AnyRef, outputDatum: AnyRef): Unit
    def get: JsonNode
    def reset(): Unit
  }

  object Residuals {
    case class Stats(sum: Double = 0.0, sum2: Double = 0.0, abssum: Double = 0.0, min: Double = 0.0, max: Double = 0.0, num: Long = 0L, missing: Long = 0L) {
      def +(residual: Double) =
        if (num == 0L)
          Stats(sum + residual, sum2 + residual*residual, abssum + Math.abs(residual), residual, residual, num + 1L, missing)
        else
          Stats(sum + residual, sum2 + residual*residual, abssum + Math.abs(residual), if (residual < min) residual else min, if (residual > max) residual else max, num + 1L, missing)
    }
  }
  case class Residuals(truePath: Path, predictedPath: Path) extends DataQualityMetric {
    val name = "residuals"

    private var active = Residuals.Stats()

    def applicable(engineConfig: EngineConfig) =
      (extractedType(engineConfig.input, truePath), extractedType(engineConfig.output, predictedPath)) match {
        case (Some(AvroNumber()), Some(AvroNumber())) => true
        case _ => false
      }

    def add(inputDatum: AnyRef, outputDatum: AnyRef) {
      (extract(inputDatum, truePath), extract(outputDatum, predictedPath)) match {
        case (Some(truth: java.lang.Number), Some(predicted: java.lang.Number)) =>
          val residual = predicted.doubleValue - truth.doubleValue
          active.synchronized {
            active = active + residual
          }
        case _ =>
          active.synchronized {
            active = active.copy(missing = active.missing + 1)
          }
      }
    }

    def get = {
      val stats = active.synchronized { active.copy() }
      val outputJsonNode = commands.jsonNodeFactory.objectNode
      outputJsonNode.put("sum", new DoubleNode(stats.sum))
      outputJsonNode.put("sum2", new DoubleNode(stats.sum2))
      outputJsonNode.put("abssum", new DoubleNode(stats.abssum))
      outputJsonNode.put("min", new DoubleNode(stats.min))
      outputJsonNode.put("max", new DoubleNode(stats.max))
      outputJsonNode.put("num", new LongNode(stats.num))
      outputJsonNode.put("missing", new LongNode(stats.missing))
      outputJsonNode
    }

    def reset() {
      active = Residuals.Stats()
    }
  }

  case class ConfusionMatrix(truePath: Path, predictedPath: Path) extends DataQualityMetric {
    val name = "confusion-matrix"

    private var active = concurrent.TrieMap[Option[(String, String)], Long]()

    def applicable(engineConfig: EngineConfig) =
      (extractedType(engineConfig.input, truePath), extractedType(engineConfig.output, predictedPath)) match {
        case (Some(AvroString()), Some(AvroString())) => true
        case _ => false
      }

    def add(inputDatum: AnyRef, outputDatum: AnyRef) {
      val pair = (extract(inputDatum, truePath), extract(outputDatum, predictedPath)) match {
        case (Some(truth: String), Some(predicted: String)) =>
          Some((truth -> predicted))
        case _ =>
          None
      }

      active.putIfAbsent(pair, 0L)
      var done = false
      while (!done) {
        val old = active(pair)
        if (active.replace(pair, old, old + 1L))
          done = true
      }
    }

    def get = {
      val matrix = active.readOnlySnapshot

      val classes = matrix.keys flatMap {case Some((truth, predicted)) => truth :: predicted :: Nil; case None => Nil} toSet

      val classesNode = commands.jsonNodeFactory.objectNode
      classes foreach {cls =>
        val classNode = commands.jsonNodeFactory.objectNode

        val truePositives = matrix collect {case (Some((truth, predicted)), count)
          if (truth == cls  &&  predicted == cls) => count} sum

        val falseNegatives = matrix collect {case (Some((truth, predicted)), count)
          if (truth == cls  &&  predicted != cls) => count} sum

        val falsePositives = matrix collect {case (Some((truth, predicted)), count)
          if (truth != cls  &&  predicted == cls) => count} sum

        val trueNegatives = matrix collect {case (Some((truth, predicted)), count)
          if (truth != cls  &&  predicted != cls) => count} sum

        classNode.put("tp", new LongNode(truePositives))
        classNode.put("fn", new LongNode(falseNegatives))
        classNode.put("fp", new LongNode(falsePositives))
        classNode.put("tn", new LongNode(trueNegatives))
        classesNode.put(cls, classNode)
      }

      val outputJsonNode = commands.jsonNodeFactory.objectNode
      outputJsonNode.put("classes", classesNode)
      outputJsonNode.put("missing", new LongNode(matrix.getOrElse(None, 0L)))
      outputJsonNode
    }

    def reset() {
      active = new concurrent.TrieMap[Option[(String, String)], Long]()
    }
  }

  // Wraps a PFAEngine to get one-engine-per-thread, with a mailbox for getting new data
  case class EngineThread(eng: PFAEngine[AnyRef, AnyRef], mailbox: Mailbox[Deserializer] = new Mailbox()) {
    var inputsReceived = 0L
    var actionsAttempted = 0L
    var actionsSuccessful = 0L
    var totalNanosecsDeserializing = 0L
    var totalNanosecsInAction = 0L
    var totalNanosecsSerializing = 0L
    val deserializationErrors = mutable.HashMap[String, Long]()
    val actionErrors = mutable.HashMap[String, Long]()
    val serializationErrors = mutable.HashMap[String, Long]()

    sealed trait Phase
    case object Deserialization extends Phase
    case object Action extends Phase
    case object DataQuality extends Phase
    case object Serialization extends Phase

    def performance = {
      val outputJsonNode = commands.jsonNodeFactory.objectNode
      outputJsonNode.put("inputsReceived", new LongNode(inputsReceived))
      outputJsonNode.put("actionsAttempted", new LongNode(actionsAttempted))
      outputJsonNode.put("actionsSuccessful", new LongNode(actionsSuccessful))
      outputJsonNode.put("totalNanosecsDeserializing", new LongNode(totalNanosecsDeserializing))
      outputJsonNode.put("totalNanosecsInAction", new LongNode(totalNanosecsInAction))
      outputJsonNode.put("totalNanosecsSerializing", new LongNode(totalNanosecsSerializing))

      val deserializationErrorsNode = commands.jsonNodeFactory.objectNode
      deserializationErrors foreach {case (k, v) => deserializationErrorsNode.put(k, new LongNode(v))}
      outputJsonNode.put("deserializationErrors", deserializationErrorsNode)

      val actionErrorsNode = commands.jsonNodeFactory.objectNode
      actionErrors foreach {case (k, v) => actionErrorsNode.put(k, new LongNode(v))}
      outputJsonNode.put("actionErrors", actionErrorsNode)

      val serializationErrorsNode = commands.jsonNodeFactory.objectNode
      serializationErrors foreach {case (k, v) => serializationErrorsNode.put(k, new LongNode(v))}
      outputJsonNode.put("serializationErrors", serializationErrorsNode)

      outputJsonNode
    }

    def resetPerformance() {
      inputsReceived = 0L
      actionsAttempted = 0L
      actionsSuccessful = 0L
      totalNanosecsDeserializing = 0L
      totalNanosecsInAction = 0L
      totalNanosecsSerializing = 0L
      deserializationErrors.clear()
      actionErrors.clear()
      serializationErrors.clear()
    }

    eng.log = logPFA

    val thread = new Thread(new Runnable {
      def run() {
        // before anything, run engine.begin()
        eng.begin()

        var done = false
        while (!done)
          // mailbox.get blocks until it gets data or is stopped
          mailbox.get match {
            case None =>
              done = true
            case Some(deserialize) =>
              var phase: Phase = Deserialization
              try {
                inputsReceived += 1L

                // deserialize in the EngineThread (to parallelize this part)
                val nanosecsBeforeDeserialize = System.nanoTime
                val inputDatum = deserialize(eng)
                totalNanosecsDeserializing += System.nanoTime - nanosecsBeforeDeserialize
                actionsAttempted += 1L

                phase = Action
                  (eng, dataOutputStream) match {
                  // emitter engine; make the emit function do the serialization
                  case (emitEngine: PFAEmitEngine[AnyRef, AnyRef], Some(serialize)) =>
                    var nanosecsSerializing = 0L
                    emitEngine.emit = {outputDatum: AnyRef =>
                      phase = DataQuality
                      dataQuality(inputDatum, outputDatum)

                      phase = Serialization
                      val nanosecsBeforeSerialize = System.nanoTime
                      serialize(eng, outputDatum)
                      nanosecsSerializing += System.nanoTime - nanosecsBeforeSerialize
                    }

                    val nanosecsBeforeAction = System.nanoTime
                    eng.action(inputDatum)
                    // for emit engines, the serialization takes place during the (single threaded) action
                    // so subtract that out and put it in the right variable
                    totalNanosecsInAction += System.nanoTime - nanosecsBeforeAction - nanosecsSerializing
                    totalNanosecsSerializing += nanosecsSerializing
                    actionsSuccessful += 1L

                  // non-emitter engine; serialize outside of action
                  case (_, Some(serialize)) =>
                    val nanosecsBeforeAction = System.nanoTime
                    val outputDatum = eng.action(inputDatum)
                    totalNanosecsInAction += System.nanoTime - nanosecsBeforeAction
                    actionsSuccessful += 1L

                    phase = DataQuality
                    dataQuality(inputDatum, outputDatum)

                    phase = Serialization
                    val nanosecsBeforeSerialize = System.nanoTime
                    serialize(eng, outputDatum)
                    totalNanosecsSerializing += System.nanoTime - nanosecsBeforeSerialize

                  case _ =>
                    throw new Exception   // shouldn't get here because input waits for output to be set up
                }
              }
              catch {
                case err: Exception =>
                  val id = err match {
                    case _: PFARuntimeException | _: PFAUserException | _: PFATimeoutException =>
                      err.toString              // PFA errors contain all relevant information in the message
                    case _ =>
                      exceptionTraceback(err)   // other (unexpected) errors need a full traceback to diagnose
                  }
                  phase match {
                    case Deserialization =>
                      deserializationErrors(id) = deserializationErrors.getOrElse(id, 0L) + 1L
                    case Action =>
                      actionErrors(id) = actionErrors.getOrElse(id, 0L) + 1L
                    case DataQuality =>
                      throw err // an exception in the DataQuality phase is a servlet programming error; they should go to the same log as other servlet programming errors
                    case Serialization =>
                      serializationErrors(id) = serializationErrors.getOrElse(id, 0L) + 1L
                  }
              }
              mailbox.reset()
          }

        // after everything, run engine.end()
        eng.end()
      }
    }, s"Hadrian-servlet ${eng.config.name} instance ${eng.instance}")
    thread.setDaemon(true)
    thread.start()
  }
  
  var throughputTotalCount = 0L
  var throughputTotalNanosecs = 0L

  // input thread, fills engines round robin with work when available
  val inputThread = new Thread(new Runnable {
    def run() {
      while (true) dataInputStream match {
        case Some(iterator) if (iterator.hasNext  &&  dataInputStreamCompatible != Some(false)) =>
          // don't let the set of engines change while you're looking for a free one to send work to
          engineThreads.synchronized {
            // poll with a 100 ms wait if an engine hasn't been loaded or an output stream hasn't been attached yet
            // (long wait time because we're waiting for the user; this does not affect performance)
            if (engineThreads.isEmpty  ||  dataOutputStream.isEmpty)
              Thread.sleep(100)
            else {
              // start the stopwatch here: you know that an engine and an output stream has been configured
              val nanosecsBeforeInput = System.nanoTime

              // the engines are loaded; even if they're busy, one will eventually be ready to take this data, so read it in
              val deserializer = iterator.next()

              // search for an idle engine and assign it some work
              var assigned = false
              var index = 0
              while (!assigned  &&  !engineThreads.isEmpty) {
                val engineThread = engineThreads(index)
                if (engineThread.mailbox.isEmpty) {
                  engineThread.mailbox.put(deserializer)
                  assigned = true
                }
                index = (index + 1) % engineThreads.size
              }

              // stop the stopwatch here: this includes any waiting due to busy engines
              if (assigned) {
                throughputTotalCount += 1L
                throughputTotalNanosecs += System.nanoTime - nanosecsBeforeInput
              }
            }
          }

        case Some(iterator) if (dataInputStreamLoop) => iterator.restart()

        // poll with a 100 ms wait if an input stream hasn't been attached yet (or if the current input stream runs out)
        // (long wait time because we're waiting for the user; this does not affect performance)
        case _ => Thread.sleep(100)
      }
    }
  }, "Hadrian-servlet Input Loop")
  inputThread.setDaemon(true)
  inputThread.start()

  type StackTraceMap = mutable.Map[String, (Long, mutable.Map[Int, Long], String)]
  val engineThreadTop = mutable.Map[String, (Long, mutable.Map[Int, Long], String)]()
  val engineThreadTopHadrian = mutable.Map[String, (Long, mutable.Map[Int, Long], String)]()
  val engineThreadTopHadrianLib = mutable.Map[String, (Long, mutable.Map[Int, Long], String)]()
  
  def resetSamplingProfiler() {
    engineThreadTop.clear()
    engineThreadTopHadrian.clear()
    engineThreadTopHadrianLib.clear()
  }

  def getSamplingProfiler(outputJsonNode: ObjectNode, maxItems: Int) {
    outputJsonNode.put("engineThreadTop", getSamplingProfilerNode(engineThreadTop, maxItems))
    outputJsonNode.put("engineThreadTopHadrian", getSamplingProfilerNode(engineThreadTopHadrian, maxItems))
    outputJsonNode.put("engineThreadTopHadrianLib", getSamplingProfilerNode(engineThreadTopHadrianLib, maxItems))
  }

  def getSamplingProfilerNode(stackTraceMap: StackTraceMap, maxItems: Int) = {
    val outputJsonNode = commands.jsonNodeFactory.arrayNode

    stackTraceMap.toSeq.sortBy(-_._2._1).take(maxItems) foreach {case (key, (count, lineCounter, fileName)) =>
      val itemNode = commands.jsonNodeFactory.objectNode
      itemNode.put("classMethod", key)
      itemNode.put("totalCount", new LongNode(count))
      itemNode.put("fileName", fileName)

      val lineNumbersNode = commands.jsonNodeFactory.objectNode

      lineCounter.toSeq.sortBy(_._2) foreach {case (lineNumber, lineNumberCount) =>
        lineNumbersNode.put(lineNumber.toString, new LongNode(lineNumberCount))
      }

      itemNode.put("lineNumberCounts", lineNumbersNode)
      outputJsonNode.add(itemNode)
    }

    outputJsonNode
  }

  val samplingProfilerThread = new Thread(new Runnable {
    def increment(stackTraceMap: StackTraceMap, key: String, lineNumber: Int, fileName: String) {
      val (count, lineCounter, _) = stackTraceMap.getOrElse(key, (0L, mutable.Map[Int, Long](), ""))

      lineCounter(lineNumber) = lineCounter.getOrElse(lineNumber, 0L) + 1L

      stackTraceMap(key) = (count + 1L, lineCounter, fileName)
    }

    def run() {
      while (true) {
        if (engineThreads exists {!_.mailbox.isEmpty})
          Thread.getAllStackTraces.filter(_._1.getName.startsWith("Hadrian-servlet ")) foreach {case (thread, stackTrace) =>
            val top = stackTrace.headOption
            val topHadrian = stackTrace.find(_.getClassName.startsWith("com.opendatagroup.hadrian."))
            val topHadrianLib = stackTrace.find(_.getClassName.startsWith("com.opendatagroup.hadrian.lib."))

            if (thread.getName == "Hadrian-servlet StackTrace Sampler") {}
            else if (thread.getName == "Hadrian-servlet Input Loop") {}
            else {
              top collect {case x: StackTraceElement => increment(engineThreadTop, x.getClassName + "." + x.getMethodName, x.getLineNumber, x.getFileName)}
              topHadrian collect {case x: StackTraceElement => increment(engineThreadTopHadrian, x.getClassName + "." + x.getMethodName, x.getLineNumber, x.getFileName)}
              topHadrianLib collect {case x: StackTraceElement => increment(engineThreadTopHadrianLib, x.getClassName + "." + x.getMethodName, x.getLineNumber, x.getFileName)}
            }

            Thread.sleep(100)
          }
        else
          Thread.sleep(1)
      }
    }
  }, "Hadrian-servlet StackTrace Sampler")
  samplingProfilerThread.setDaemon(true)
  samplingProfilerThread.start()
}
