package com.opendatagroup.hadrian.actors

import java.lang.management.ManagementFactory
import java.lang.ProcessBuilder
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.concurrent.ExecutionContext
import scala.concurrent.Future
import scala.language.postfixOps
import scala.reflect.ClassTag

import com.opendatagroup.hadrian.data.AnyPFARecord
import com.opendatagroup.hadrian.data.avroInputIterator
import com.opendatagroup.hadrian.data.avroOutputDataStream
import com.opendatagroup.hadrian.data.fromJson
import com.opendatagroup.hadrian.data.jsonInputIterator
import com.opendatagroup.hadrian.data.jsonOutputDataStream
import com.opendatagroup.hadrian.data.OutputDataStream
import com.opendatagroup.hadrian.data.ScalaDataTranslator
import com.opendatagroup.hadrian.data.toJson
import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.errors.PFAInitializationException
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.errors.PFATimeoutException
import com.opendatagroup.hadrian.errors.PFAUserException
import com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine

package object workflow {
  val GB_PER_BYTE = 1.0 / 1024.0 / 1024.0 / 1024.0

  val monitor = new Monitor()
  var avoidMemoryCrash: AvoidMemoryCrash = null
  val sources = mutable.Map[String, Seq[SimpleActor]]()
  val engines = mutable.Map[String, Seq[SimpleActor]]()
  val queues = mutable.ListBuffer[SimpleActor]()

  def now = System.currentTimeMillis

  def createActors(cfg: config.Topology): Unit = {
    // create nodes
    for ((name, node) <- cfg.nodes) {
      node match {
        case cfg: config.FileSource =>
          sources(name) = Seq(new FileSource(monitor, name, cfg))

        case cfg: config.ExtendedSource[_] =>
          sources(name) = Seq(new ExtendedSource(monitor, name, cfg)(cfg.stateType))

        case cfg: config.Engine =>
          val factory = PFAEngine.factoryFromAst(cfg.engineConfig, cfg.options)
          engines(name) =
            for (i <- 0 until cfg.multiplicity) yield
              new Engine(monitor, name, cfg, factory())

        case cfg: config.JarEngine =>
          val classLoader = new java.net.URLClassLoader(Array(cfg.url))
          val clazz = classLoader.loadClass(cfg.className)
          engines(name) =
            for (i <- 0 until cfg.multiplicity) yield
              new JarEngine(monitor, name, cfg, clazz)

        case cfg: config.ShellEngine =>
          engines(name) =
            for (i <- 0 until cfg.multiplicity) yield
              new ShellEngine(monitor, name, cfg)

        case cfg: config.ExtendedProcessor[_] =>
          engines(name) =
            for (i <- 0 until cfg.multiplicity) yield
              new ExtendedProcessor(monitor, name, cfg)(cfg.stateType)
      }
    }

    // attach queues
    for ((name, node) <- cfg.nodes;  destination <- node.destinations) destination match {
      case cfg: config.Queue =>
        val queue =
          if (cfg.hashKeys.isEmpty)
            new Queue(monitor, s"$name -> ${cfg.to}", cfg)
          else
            new HashQueue(monitor, s"$name -> ${cfg.to}", cfg)
        queues.add(queue)

        val sourceOrEngineByName = (sources.get(name) ++ engines.get(name)).head
        val sourceOrEngineByCfgTo = (sources.get(cfg.to) ++ engines.get(cfg.to)).head

        sourceOrEngineByName foreach {_.receive(AttachDestination(queue), EmptyActor)}
        sourceOrEngineByCfgTo foreach {_.receive(AttachSource(queue), EmptyActor)}

        queue.receive(AttachNodes(sourceOrEngineByName, sourceOrEngineByCfgTo), EmptyActor)

      case _ =>
    }
  }

  def start(monitorFreqSeconds: Double, queueMemoryLimit: Double): Unit = {
    for (refs <- engines.values; ref <- refs)
      ref.receive(Start(), EmptyActor)

    for (ref <- queues)
      ref.receive(Start(), EmptyActor)

    val executionContext = ExecutionContext.fromExecutor(
      java.util.concurrent.Executors.newFixedThreadPool(sources.size))

    for (refs <- sources.values; ref <- refs)
      ref.receive(StartSource(executionContext), EmptyActor)

    monitor.receive(StartMonitor(monitorFreqSeconds), EmptyActor)

    avoidMemoryCrash = new AvoidMemoryCrash(queueMemoryLimit)
    for (ref <- queues)
      avoidMemoryCrash.receive(AttachQueue(ref), EmptyActor)

    avoidMemoryCrash.receive(Start(), EmptyActor)
  }

  def closeFiles(): Unit =
    for (refs <- sources.values ++ engines.values; ref <- refs)
      ref.receive(CloseFiles(), EmptyActor)

  java.lang.Runtime.getRuntime.addShutdownHook(new Thread(new Runnable {
    def run() {
      closeFiles()
    }
  }))

  class FileHandle(val file: java.io.File, writer: OutputDataStream, closeCondition: (Double, Long) => Boolean) {
    private var startTime = -1L
    private var recordCount = 0L
    private var closed = false

    def append(x: AnyRef): Unit = {
      if (!closed) {
        if (startTime < 0L)
          startTime = now
        val timeDiff = 0.001 * (now - startTime)

        if (closeCondition(timeDiff, recordCount)) {
          writer.close()
          closed = true
        }
        else
          writer.append(x)

        recordCount += 1L
      }
    }

    def close(): Unit =
      if (!closed) {
        writer.close()
        closed = true
      }
  }

  def fileDestinations(destinations: Seq[config.Destination], outputType: AvroType): Seq[FileHandle] = destinations collect {
      case config.FileDestination(file, format, limitSeconds, limitRecords) =>
        val writer = format match {
          case config.Format.AVRO => avroOutputDataStream(file, outputType)
          case config.Format.JSON => jsonOutputDataStream(file, outputType, false)
          case config.Format.JSONSCHEMA => jsonOutputDataStream(file, outputType, true)
        }

        val closeCondition =
          if (limitSeconds == None  &&  limitRecords == None)
            {(timeDiff: Double, recordCount: Long) => false}
          else if (limitSeconds != None  &&  limitRecords == None)
            {(timeDiff: Double, recordCount: Long) => timeDiff > limitSeconds.get}
          else if (limitSeconds == None  &&  limitRecords != None)
            {(timeDiff: Double, recordCount: Long) => recordCount > limitRecords.get}
          else
            {(timeDiff: Double, recordCount: Long) => timeDiff > limitSeconds.get  ||  recordCount > limitRecords.get}

        new FileHandle(file, writer, closeCondition)
    }

  def sendToFiles(files: Seq[FileHandle], value: AnyRef, monitor: Monitor, name: String, sender: SimpleActor): Unit =
    files foreach {f =>
      try {
        f.append(value)
      }
      catch {
        case exception: Exception =>
          f.close()
          main.Main.logger.error(s"$exception (stop writing to ${f.file})")
          monitor.receive(MonitorError(name), sender)
      }
    }

  def extendedDestinations(destinations: Seq[config.Destination], outputType: AvroType): Seq[config.HasSend] =
    destinations collect {
      case x: config.ExtendedDestination[_] => x.initialize(outputType)
    }

  def sendToExtended(hasSends: Seq[config.HasSend], value: AnyRef, monitor: Monitor, name: String, sender: SimpleActor): Unit =
    hasSends foreach {f =>
      try {
        f.send(value)
      }
      catch {
        case exception: Exception =>
          main.Main.logger.error(exception.toString)
          monitor.receive(MonitorError(name), sender)
      }
    }

  def closeExtended(hasSends: Seq[config.HasSend]): Unit =
    hasSends foreach {_ match {
      case x: config.HasClose => x.close()
      case _ =>
    }}
}

package workflow {
  trait Message
  trait SimpleActor {
    def receive(x: Message, sender: SimpleActor): Unit
  }

  object EmptyActor extends SimpleActor {
    def receive(x: Message, sender: SimpleActor) { }
  }

  case class AttachSource(queue: SimpleActor) extends Message
  case class AttachDestination(queue: SimpleActor) extends Message
  case class AttachNodes(from: Seq[SimpleActor], to: Seq[SimpleActor]) extends Message
  case class AttachQueue(q: SimpleActor) extends Message

  case class Start() extends Message
  case class StartSource(executionContext: ExecutionContext) extends Message
  case class StartMonitor(monitorFreqSeconds: Double) extends Message
  case class ReadyForData() extends Message
  case class Datum(value: AnyRef) extends Message
  case class CloseFiles() extends Message

  case class DropAllData(really: Boolean) extends Message

  case class MonitorSource(name: String) extends Message
  case class MonitorEngineAnnounce(name: String, shellEngine: Boolean, multiplicity: Int) extends Message
  case class MonitorTimeInEngine(name: String, diff: Long) extends Message
  case class MonitorError(name: String) extends Message
  case class MonitorQueueDepth(name: String, depth: Int) extends Message
  case class MonitorHashQueueDepth(name: String, depths: Seq[Int]) extends Message
  case class MonitorQueueDropped(name: String) extends Message

  class FileSource(monitor: Monitor, name: String, cfg: config.FileSource) extends SimpleActor {
    private val inputIterator = cfg.format match {
      case config.Format.AVRO => avroInputIterator(new java.io.FileInputStream(cfg.file), cfg.outputType)
      case config.Format.JSON => jsonInputIterator(new java.io.FileInputStream(cfg.file), cfg.outputType)
    }
    private var destinations = List[SimpleActor]()

    private val outputFiles = fileDestinations(cfg.destinations, cfg.outputType)
    private val outputExtended = extendedDestinations(cfg.destinations, cfg.outputType)
    
    override def receive(x: Message, sender: SimpleActor) = x match {
      case AttachDestination(queue) =>
        this.synchronized {
          destinations = queue :: destinations
        }

      case StartSource(executionContext: ExecutionContext) =>
        implicit val ec = executionContext
        Future {
          while (inputIterator.hasNext) {
            val datum = Datum(inputIterator.next())
            monitor.receive(MonitorSource(name), this)
            destinations foreach {_.receive(datum, this)}
            sendToFiles(outputFiles, datum.value, monitor, name, this)
            sendToExtended(outputExtended, datum.value, monitor, name, this)
          }
        }

      case CloseFiles() =>
        outputFiles foreach {_.close()}
        closeExtended(outputExtended)

      case _ =>
    }
  }

  class ExtendedSource[STATE <: scala.Iterator[AnyRef]](monitor: Monitor, name: String, cfg: config.ExtendedSource[STATE])(implicit stateType: ClassTag[STATE]) extends SimpleActor {
    private val state = cfg.initialize

    private var destinations = List[SimpleActor]()

    private val outputFiles = fileDestinations(cfg.destinations, cfg.outputType)
    private val outputExtended = extendedDestinations(cfg.destinations, cfg.outputType)

    override def receive(x: Message, sender: SimpleActor) = x match {
      case AttachDestination(queue) =>
        this.synchronized {
          destinations = queue :: destinations
        }

      case StartSource(executionContext: ExecutionContext) =>
        implicit val ec = executionContext
        Future {
          while (state.hasNext) {
            val datum = Datum(state.next())
            monitor.receive(MonitorSource(name), this)
            destinations foreach {_.receive(datum, this)}
            sendToFiles(outputFiles, datum.value, monitor, name, this)
            sendToExtended(outputExtended, datum.value, monitor, name, this)
          }
        }

      case CloseFiles() =>
        outputFiles foreach {_.close()}
        closeExtended(outputExtended)

      case _ =>
    }
  }

  class Engine(monitor: Monitor, name: String, cfg: config.Engine, engine: PFAEngine[AnyRef, AnyRef]) extends SimpleActor {
    private var sources = List[SimpleActor]()
    private var destinations = List[SimpleActor]()

    private val outputFiles = fileDestinations(cfg.destinations, cfg.outputType)
    private val outputExtended = extendedDestinations(cfg.destinations, cfg.outputType)
    
    private val saveStateThread =
      cfg.saveState map {saveStateCfg =>
        val out = new Thread(new Runnable {
          def writeStateToFile(reason: String): Unit = {
            val fileName =
              if (cfg.multiplicity > 1) {
                val digits = Math.floor(Math.log10(cfg.multiplicity)).toInt
                val formatter = "%0" + digits.toString + "d"
                saveStateCfg.baseName + name + "_" + formatter.format(engine.instance) + "_" + reason + ".pfa"
              }
              else
                saveStateCfg.baseName + name + "_" + reason + ".pfa"

            val out = new java.io.PrintWriter(new java.io.File(fileName))
            out.print(engine.snapshot.toString)
            out.close()
          }

          def run(): Unit = {
            saveStateCfg.freqSeconds foreach {secs =>
              while (true) {
                Thread.sleep(secs * 1000L)
                val timestamp = new java.sql.Timestamp((new java.util.Date).getTime).toString.take(19).replace(" ", "T")
                writeStateToFile(timestamp)
              }
            }
          }
        })
        out.setDaemon(true)
        out
      }

    engine.log = {(x: String, ns: Option[String]) =>
      if (main.Main.logger.isDebugEnabled)
        main.Main.logger.debug(s"""$name ${engine.config.name} ${engine.instance}${if (ns != None) " " + ns.get else ""}: $x""")
    }

    private val exec: Function1[AnyRef, Unit] =
      engine match {
        case emitEngine: PFAEmitEngine[_, _] =>
          emitEngine.emit = {result: AnyRef =>
            destinations foreach {_.receive(Datum(result), this)}
            sendToFiles(outputFiles, result, monitor, name, this)
            sendToExtended(outputExtended, result, monitor, name, this)
          }

          {value =>
            engine.action(engine.fromPFAData(value))}

        case otherEngine =>
          {value =>
            val result = engine.action(engine.fromPFAData(value))
            destinations foreach {_.receive(Datum(result), this)}
            sendToFiles(outputFiles, result, monitor, name, this)
            sendToExtended(outputExtended, result, monitor, name, this)
          }
      }

    val errorsSeen = mutable.Map[(String, String), Long]()

    def maybeWriteError(errType: String, err: String) {
      val count = errorsSeen.get((errType, err)) match {
        case None => 1
        case Some(c) => c
      }

      if (count < 10)
        main.Main.logger.error(err)
      else if (Math.log10(count) % 1 == 0)
        main.Main.logger.error(err + s" ($count times)")

      errorsSeen((errType, err)) = count + 1
    }

    // since the thread pool has exactly one thread, it's always available to this engine instance
    // and no more than one engine instance can ever run in a Future (so "synchronized" is not needed)
    implicit val executionContext = ExecutionContext.fromExecutor(java.util.concurrent.Executors.newFixedThreadPool(1))

    override def receive(x: Message, sender: SimpleActor) = x match {
      case AttachSource(queue) =>
        this.synchronized {
          sources = queue :: sources
        }

      case AttachDestination(queue) =>
        this.synchronized {
          destinations = queue :: destinations
        }

      case Start() =>
        Future {
          monitor.receive(MonitorEngineAnnounce(name, false, cfg.multiplicity), this)
          engine.begin()
          sources foreach {_.receive(ReadyForData(), this)}
          saveStateThread foreach {_.start()}
        }

      case Datum(value) =>
        Future {
          val before = now
          try {
            exec(value)
          }
          catch {
            case exception: PFAInitializationException =>
              maybeWriteError(exception.getClass.getName, exception.toString)
              monitor.receive(MonitorError(name), this)

            case exception: PFARuntimeException =>
              maybeWriteError(exception.getClass.getName, exception.toString)
              monitor.receive(MonitorError(name), this)

            case exception: PFATimeoutException =>
              maybeWriteError(exception.getClass.getName, exception.toString)
              monitor.receive(MonitorError(name), this)

            case exception: PFAUserException =>
              maybeWriteError(exception.getClass.getName, exception.toString)
              monitor.receive(MonitorError(name), this)

            case exception: Exception =>
              val writer = new java.io.StringWriter
              val printWriter = new java.io.PrintWriter(writer)
              exception.printStackTrace(printWriter)
              writer.flush()
              main.Main.logger.error(writer.toString)

          }
          val after = now

          monitor.receive(MonitorTimeInEngine(name, after - before), this)
          sender.receive(ReadyForData(), this)
        }

      case CloseFiles() =>
        engine.end()
        outputFiles foreach {_.close()}
        closeExtended(outputExtended)

      case _ =>
    }
  }

  class JarEngine[INPUT, OUTPUT](monitor: Monitor, name: String, cfg: config.JarEngine, clazz: java.lang.Class[_]) extends SimpleActor {
    private var sources = List[SimpleActor]()
    private var destinations = List[SimpleActor]()

    private val outputFiles = fileDestinations(cfg.destinations, cfg.outputType)
    private val outputExtended = extendedDestinations(cfg.destinations, cfg.outputType)

    private val inputTranslator = new ScalaDataTranslator[INPUT](cfg.inputType, clazz.getClassLoader)
    private val outputTranslator = new ScalaDataTranslator[OUTPUT](cfg.outputType, clazz.getClassLoader)
    private val engine = clazz.getConstructor().newInstance().asInstanceOf[Function1[INPUT, Seq[OUTPUT]]]

    try {
      val declaredType = clazz.getDeclaredMethod("input").invoke(engine).asInstanceOf[AvroType]
      if (!declaredType.accepts(cfg.inputType)  ||  !cfg.inputType.accepts(declaredType))
        throw new IllegalArgumentException(s"Hadrian-Actors expects ${clazz.getName} input type to be\n    ${cfg.inputType}\nbut ${clazz.getName} declares it to be\n    $declaredType")
    }
    catch {
      case exception: java.lang.NoSuchMethodException => main.Main.logger.warn(s"class ${clazz.getName} has no input() method; skipping type check")
    }

    try {
      val declaredType = clazz.getDeclaredMethod("output").invoke(engine).asInstanceOf[AvroType]
      if (!declaredType.accepts(cfg.outputType)  ||  !cfg.outputType.accepts(declaredType))
        throw new IllegalArgumentException(s"Hadrian-Actors expects ${clazz.getName} output type to be\n    ${cfg.outputType}\nbut ${clazz.getName} declares it to be\n    $declaredType")
    }
    catch {
      case exception: java.lang.NoSuchMethodException => main.Main.logger.warn(s"class ${clazz.getName} has no output() method; skipping type check")
    }

    // since the thread pool has exactly one thread, it's always available to this engine instance
    // and no more than one engine instance can ever run in a Future (so "synchronized" is not needed)
    implicit val executionContext = ExecutionContext.fromExecutor(java.util.concurrent.Executors.newFixedThreadPool(1))

    override def receive(x: Message, sender: SimpleActor) = x match {
      case AttachSource(queue) =>
        this.synchronized {
          sources = queue :: sources
        }

      case AttachDestination(queue) =>
        this.synchronized {
          destinations = queue :: destinations
        }

      case Start() =>
        Future {
          monitor.receive(MonitorEngineAnnounce(name, false, cfg.multiplicity), this)
          sources foreach {_.receive(ReadyForData(), this)}
        }

      case Datum(value) =>
        Future {
          val before = now
          val inputData = inputTranslator.toScala(value)
          val results =
            try {
              engine(inputData)
            }
          catch {
            case exception: Exception =>
              val writer = new java.io.StringWriter
              val printWriter = new java.io.PrintWriter(writer)
              exception.printStackTrace(printWriter)
              writer.flush()
              main.Main.logger.error(writer.toString)
              monitor.receive(MonitorError(name), this)
              Seq[AnyRef]()
          }
          for (result <- results) {
            val outputData = outputTranslator.fromScala(result)
            destinations foreach {_.receive(Datum(outputData), this)}
            sendToFiles(outputFiles, outputData, monitor, name, this)
            sendToExtended(outputExtended, outputData, monitor, name, this)
          }
          val after = now

          monitor.receive(MonitorTimeInEngine(name, after - before), this)
          sender.receive(ReadyForData(), this)
        }

      case CloseFiles() =>
        outputFiles foreach {_.close()}
        closeExtended(outputExtended)

      case _ =>
    }
  }

  class ShellEngine(monitor: Monitor, name: String, cfg: config.ShellEngine) extends SimpleActor {
    private var sources = List[SimpleActor]()
    private var destinations = List[SimpleActor]()

    private val outputFiles = fileDestinations(cfg.destinations, cfg.outputType)
    private val outputExtended = extendedDestinations(cfg.destinations, cfg.outputType)

    private val processBuilder = new ProcessBuilder(cfg.cmd)
    cfg.dir foreach {x => processBuilder.directory(new java.io.File(x))}
    cfg.env foreach {case (k, v) => processBuilder.environment.put(k, v)}
    private val process = processBuilder.start()
    private val processInput = new java.io.PrintWriter(process.getOutputStream)
    private val processOutput = new java.util.Scanner(process.getInputStream)
    private val processErrors = new java.util.Scanner(process.getErrorStream)

    class OutputRunnable(sender: SimpleActor) extends Runnable {
      def run(): Unit = {
        while (true) {
          while (processOutput.hasNextLine) {
            try {
              // note: nextLine() blocks until a line is available (possibly depends on how often stderr is flushed)
              val str = processOutput.nextLine()
              val result = fromJson(str, cfg.outputType)
              destinations foreach {_.receive(Datum(result), sender)}
              sendToFiles(outputFiles, result, monitor, name, sender)
              sendToExtended(outputExtended, result, monitor, name, sender)
            }
            catch {
              case exception: Exception =>
                val writer = new java.io.StringWriter
                val printWriter = new java.io.PrintWriter(writer)
                exception.printStackTrace(printWriter)
                writer.flush()
                main.Main.logger.error(writer.toString)
                monitor.receive(MonitorError(name), sender)
                Seq[AnyRef]()
            }
          }
          Thread.sleep(10)
        }
      }}
    private val outputThread = new Thread(new OutputRunnable(this))
    outputThread.setDaemon(true)

    class ErrorRunnable(sender: SimpleActor) extends Runnable {
      def run(): Unit = {
        while (true) {
          main.Main.logger.error(processErrors.nextLine())
          monitor.receive(MonitorError(name), sender)
        }
      }
    }
    private val errorThread = new Thread(new ErrorRunnable(this))
    errorThread.setDaemon(true)

    // since the thread pool has exactly one thread, it's always available to this engine instance
    // and no more than one engine instance can ever run in a Future (so "synchronized" is not needed)
    implicit val executionContext = ExecutionContext.fromExecutor(java.util.concurrent.Executors.newFixedThreadPool(1))

    override def receive(x: Message, sender: SimpleActor) = x match {
      case AttachSource(queue) =>
        this.synchronized {
          sources = queue :: sources
        }

      case AttachDestination(queue) =>
        this.synchronized {
          destinations = queue :: destinations
        }

      case Start() =>
        Future {
          monitor.receive(MonitorEngineAnnounce(name, true, cfg.multiplicity), this)
          outputThread.start()
          errorThread.start()
          sources foreach {_.receive(ReadyForData(), this)}
        }

      case Datum(value) =>
        // send a line of JSON to the process and let the output/error threads handle the results
        Future {
          processInput.println(toJson(value, cfg.inputType))
          sender.receive(ReadyForData(), this)
        }

      case CloseFiles() =>
        outputFiles foreach {_.close()}
        closeExtended(outputExtended)

      case _ =>
    }
  }

  class ExtendedProcessor[STATE <: config.HasAction](monitor: Monitor, name: String, cfg: config.ExtendedProcessor[STATE])(implicit stateType: ClassTag[STATE]) extends SimpleActor {
    private val state: STATE = cfg.initialize

    private var sources = List[SimpleActor]()
    private var destinations = List[SimpleActor]()

    private val outputFiles = fileDestinations(cfg.destinations, cfg.outputType)
    private val outputExtended = extendedDestinations(cfg.destinations, cfg.outputType)

    // since the thread pool has exactly one thread, it's always available to this engine instance
    // and no more than one engine instance can ever run in a Future (so "synchronized" is not needed)
    implicit val executionContext = ExecutionContext.fromExecutor(java.util.concurrent.Executors.newFixedThreadPool(1))

    override def receive(x: Message, sender: SimpleActor) = x match {
      case AttachSource(queue) =>
        this.synchronized {
          sources = queue :: sources
        }

      case AttachDestination(queue) =>
        this.synchronized {
          destinations = queue :: destinations
        }

      case Start() =>
        Future {
          monitor.receive(MonitorEngineAnnounce(name, false, cfg.multiplicity), this)
          state match {
            case x: config.HasBegin => x.begin()
            case _ =>
          }
          sources foreach {_.receive(ReadyForData(), this)}
        }

      case Datum(value) =>
        Future {
          val before = now
          val results =
            try {
              state.action(value)
            }
            catch {
              case exception: Exception =>
                val writer = new java.io.StringWriter
                val printWriter = new java.io.PrintWriter(writer)
                exception.printStackTrace(printWriter)
                writer.flush()
                main.Main.logger.error(writer.toString)
                monitor.receive(MonitorError(name), this)
                Seq[AnyRef]()
            }
          for (result <- results) {
            destinations foreach {_.receive(Datum(result), this)}
            sendToFiles(outputFiles, result, monitor, name, this)
            sendToExtended(outputExtended, result, monitor, name, this)
          }
          val after = now

          monitor.receive(MonitorTimeInEngine(name, after - before), this)
          sender.receive(ReadyForData(), this)
        }

      case CloseFiles() =>
        state match {
          case x: config.HasEnd => x.end()
          case _ =>
        }
        outputFiles foreach {_.close()}
        closeExtended(outputExtended)

      case _ =>
    }
  }

  class Queue(monitor: Monitor, name: String, cfg: config.Queue) extends SimpleActor {
    private var from = Seq[SimpleActor]()
    private var to = Seq[SimpleActor]()

    private val data = new ConcurrentLinkedQueue[Datum]
    private val waiting = new ConcurrentLinkedQueue[SimpleActor]
    private var dataSize = new AtomicInteger

    private var dropAllData = false

    override def receive(x: Message, sender: SimpleActor) = x match {
      case AttachNodes(from, to) =>
        this.from = from
        this.to = to

      case Start() =>
        monitor.receive(MonitorQueueDepth(name, 0), this)

      case x: ReadyForData =>
        val datum = data.poll()
        if (datum == null)
          waiting.add(sender)
        else {
          sender.receive(datum, this)
          dataSize.getAndDecrement()
        }

        monitor.receive(MonitorQueueDepth(name, dataSize.get), this)

      case x: Datum =>
        val waiter = waiting.poll()
        if (waiter == null) {
          val ds = dataSize.incrementAndGet()
          if (dropAllData  ||  (cfg.limitBuffer != None  &&  ds > cfg.limitBuffer.get)) {
            monitor.receive(MonitorQueueDropped(name), this)
            dataSize.getAndDecrement()
          }
          else
            data.add(x)
        }
        else
          waiter.receive(x, this)

        monitor.receive(MonitorQueueDepth(name, dataSize.get), this)

      case DropAllData(really) =>
        dropAllData = really

      case _ =>
    }
  }

  class HashQueue(monitor: Monitor, name: String, cfg: config.Queue) extends SimpleActor {
    private var from = Seq[SimpleActor]()
    private var to = Seq[SimpleActor]()

    private val fieldNameSets = cfg.hashKeys map {_.trim.split("""\.""").toList} toSeq

    private def descend(value: AnyRef, fieldNames: List[String]): AnyRef = fieldNames match {
      case Nil => value
      case x :: xs => descend(value.asInstanceOf[AnyPFARecord].get(x), xs)
    }

    private val data = mutable.Map[SimpleActor, ConcurrentLinkedQueue[Datum]]()
    private val dataSize = mutable.Map[SimpleActor, AtomicInteger]()
    private val waiting = mutable.Map[SimpleActor, AtomicInteger]()

    private var dropAllData = false

    private def keysToSimpleActor(keys: Seq[AnyRef]): SimpleActor =
      to((to.size * (keys.hashCode.toLong - java.lang.Integer.MIN_VALUE.toLong) /
        (java.lang.Integer.MAX_VALUE.toLong - java.lang.Integer.MIN_VALUE.toLong)).toInt)

    override def receive(x: Message, sender: SimpleActor) = x match {
      case AttachNodes(from, to) =>
        this.from = from
        this.to = to.toIndexedSeq  // make sure it has quick index-based access

        this.synchronized {
          for (actorRef <- to) {
            data(actorRef) = new ConcurrentLinkedQueue[Datum]()
            dataSize(actorRef) = new AtomicInteger()
            waiting(actorRef) = new AtomicInteger
          }
        }

      case Start() =>
        monitor.receive(MonitorHashQueueDepth(name, Seq.fill(this.to.size)(0)), this)

      case x: ReadyForData =>
        val q = data(sender)

        val datum = q.poll()
        if (datum == null)
          waiting(sender).getAndIncrement()
        else {
          sender.receive(datum, this)
          dataSize(sender).getAndDecrement()
        }

        monitor.receive(MonitorHashQueueDepth(name, this.to map {actorRef => dataSize(actorRef).get}), this)

      case x: Datum =>
        val keys = fieldNameSets map {fieldNames => descend(x.value, fieldNames)}
        val actorRef = keysToSimpleActor(keys)
        val q = data(actorRef)

        if (waiting(actorRef).get == 0) {
          val ds = dataSize(actorRef).incrementAndGet()
          if (dropAllData  ||  (cfg.limitBuffer != None  &&  ds > cfg.limitBuffer.get / to.size)) {
            monitor.receive(MonitorQueueDropped(name), this)
            dataSize(actorRef).getAndDecrement()
          }
          else
            q.add(x)
        }
        else {
          waiting(actorRef).getAndDecrement()
          actorRef.receive(x, this)
        }

        monitor.receive(MonitorHashQueueDepth(name, this.to map {actorRef => dataSize(actorRef).get}), this)

      case DropAllData(really) =>
        dropAllData = really

      case _ =>
    }
  }

  class AvoidMemoryCrash(queueMemoryLimit: Double) extends SimpleActor {
    private var queues = List[SimpleActor]()

    class WatcherRunnable(sender: SimpleActor) extends Runnable {
      def run() {
        while (true) {
          Thread.sleep(1000)
          val usedMemBytes = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed
          val maxMemBytes = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getMax
          val frac = usedMemBytes.toDouble / maxMemBytes.toDouble

          if (frac > queueMemoryLimit) {
            queues foreach {_.receive(DropAllData(true), sender)}
            main.Main.logger.error(s"memory used ${usedMemBytes * GB_PER_BYTE} GB / max ${maxMemBytes * GB_PER_BYTE} GB = $frac > $queueMemoryLimit, so all queues drop data for 1 sec")
          }
          else
            queues foreach {_.receive(DropAllData(false), sender)}
        }
      }
    }
    val watcherThread = new Thread(new WatcherRunnable(this))
    watcherThread.setDaemon(false)

    override def receive(x: Message, sender: SimpleActor) = x match {
      case AttachQueue(q) =>
        queues = q :: queues

      case Start() =>
        watcherThread.start()

      case _ =>
    }
  }

  case class EngineData(shellEngine: Boolean, count: AtomicInteger, sum: AtomicLong, var max: Long, errors: AtomicInteger, multiplicity: Int) {
    def reset(): Unit = {
      count.set(0)
      sum.set(0)
      max = 0L
    }

    def updateTime(diff: Long): Unit = {
      count.getAndIncrement()
      sum.getAndAdd(diff)
      if (diff > max) max = diff
    }

    def updateErrors(): Unit =
      errors.getAndIncrement()

    def averageTime: Double = {
      val c = count.get
      val s = sum.get
      if (c > 0)
        s.toDouble / c.toDouble
      else
        0.0
    }
  }

  class Monitor extends SimpleActor {
    val sourceCount           = new java.util.HashMap[String, AtomicLong]
    val sourceCumulativeCount = new java.util.HashMap[String, AtomicLong]
    val engineData            = new java.util.HashMap[String, EngineData]
    val queueDepths           = new java.util.HashMap[String, Int]
    val hashQueueDepths       = new java.util.HashMap[String, Seq[Int]]
    val queueDropped          = new java.util.HashMap[String, AtomicInteger]

    override def receive(x: Message, sender: SimpleActor) = x match {
      case StartMonitor(monitorFreqSeconds) =>
        this.synchronized {
          freqSeconds = monitorFreqSeconds
          printThread.start()
        }

      case MonitorSource(name) => {
          try {
            sourceCount.get(name).getAndIncrement()
          }
          catch {
            case _: NullPointerException =>
              sourceCount.put(name, new AtomicLong)
              sourceCount.get(name).getAndIncrement()
          }

          try {
            sourceCumulativeCount.get(name).getAndIncrement()
          }
          catch {
            case err: NullPointerException =>
              sourceCumulativeCount.put(name, new AtomicLong)
              sourceCumulativeCount.get(name).getAndIncrement()
          }
      }

      case MonitorEngineAnnounce(name, shellEngine, multiplicity) =>
        engineData.synchronized {
          engineData.put(name, EngineData(shellEngine, new AtomicInteger, new AtomicLong, 0L, new AtomicInteger, multiplicity))
        }

      case MonitorTimeInEngine(name, diff) =>
        engineData.get(name).updateTime(diff)

      case MonitorError(name) =>
        engineData.get(name).updateErrors()

      case MonitorQueueDepth(name, depth) =>
        queueDepths.put(name, depth)

      case MonitorHashQueueDepth(name, depths) =>
        hashQueueDepths.put(name, depths)

      case MonitorQueueDropped(name) =>
        try {
          queueDropped.get(name).getAndIncrement()
        }
        catch {
          case _: NullPointerException =>
            queueDropped.put(name, new AtomicInteger)
            queueDropped.get(name).getAndIncrement()
        }

      case _ =>
    }

    var freqSeconds = 1.0
    val printThread = new Thread(new Runnable {
      def run(): Unit = {
        var lastTime = now
        val startTime = lastTime
        var oldTimeInGC = 0L
        while (true) {
          Thread.sleep(Math.round(freqSeconds * 1000.0))

          val usedMemBytes = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed
          val maxMemBytes = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getMax

          val currentTime = now
          val timeDifference = currentTime - lastTime

          val totalTimeInGC = ManagementFactory.getGarbageCollectorMXBeans.map(_.getCollectionTime).sum
          val timeInGC = totalTimeInGC - oldTimeInGC
          oldTimeInGC = totalTimeInGC

          main.Main.logger.info(f"""total time:               ${(currentTime - startTime) / 1000.0}%9.3fs    since last: ${timeDifference / 1000.0}%6.3fs""")
          main.Main.logger.info(f"""total garbage collections: ${java.lang.management.ManagementFactory.getGarbageCollectorMXBeans.map(_.getCollectionCount).sum}%4d         time in GC: ${100.0 * timeInGC / (currentTime - startTime)}%4.1f%%""")
          main.Main.logger.info(f"""heap memory ceiling:         ${maxMemBytes * GB_PER_BYTE}%.2fGB""")
          main.Main.logger.info(f"""heap memory used:            ${usedMemBytes * GB_PER_BYTE}%.2fGB      fraction: ${100.0 * usedMemBytes / maxMemBytes}%4.1f%%""")
          main.Main.logger.info(f"""number of threads:         ${ManagementFactory.getThreadMXBean.getThreadCount}%4d""")

          sourceCount.keySet.toSeq.sorted foreach {key =>
            val cumulative =
              try {
                sourceCumulativeCount.get(key).get
              }
              catch {
                case _: NullPointerException => 0
              }
            main.Main.logger.info(f"""$key%-15s rate: ${sourceCount.get(key).get * 1000.0 / timeDifference}%9.3f/s cumulative: ${cumulative}%5d""")
          }
          sourceCount.keySet foreach {key => sourceCount.get(key).set(0)}

          engineData.keySet.toSeq.sorted foreach {key =>
            val edata = engineData.get(key)
            if (!edata.shellEngine)
              main.Main.logger.info(f"""$key%-15s rate: ${edata.count.get * 1000.0 / timeDifference}%9.3f/s average: ${edata.averageTime}%7.3fms max: ${edata.max}%4dms busyCPUs: ${edata.sum.get.toDouble / timeDifference}%5.2f/${edata.multiplicity} errors: ${edata.errors.get}%4d""")
            else
              main.Main.logger.info(f"""$key%-15s                                                      errors: ${edata.errors.get}%4d""")
          }
          engineData.values foreach {_.reset()}

          queueDepths.keySet.toSeq.sorted foreach {key =>
            val dropped =
              try {
                queueDropped.get(key).get
              }
              catch {
                case _: NullPointerException => 0
              }
            if (dropped > 0)
              main.Main.logger.info(f"""$key%-24s depth: ${queueDepths.get(key)}%5d                           dropped: $dropped%5d""")
            else
              main.Main.logger.debug(f"""$key%-24s depth: ${queueDepths.get(key)}%5d                           dropped: $dropped%5d""")
          }
          hashQueueDepths.keySet.toSeq.sorted foreach {key =>
            val dropped =
              try {
                queueDropped.get(key).get
              }
              catch {
                case _: NullPointerException => 0
              }
            if (dropped > 0)
              main.Main.logger.info(f"""$key%-24s depths: ${hashQueueDepths.get(key).mkString(" ")}%-30s dropped: ${dropped}%5d""")
            else
              main.Main.logger.debug(f"""$key%-24s depths: ${hashQueueDepths.get(key).mkString(" ")}%-30s dropped: ${dropped}%5d""")
          }
          main.Main.logger.info("")

          lastTime = currentTime
        }
      }
    })
    printThread.setDaemon(true)
  }

}
