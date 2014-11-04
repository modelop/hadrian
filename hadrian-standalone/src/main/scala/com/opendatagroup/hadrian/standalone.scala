package com.opendatagroup.hadrian.standalone

import java.util.logging
import java.io.File

import scala.collection.JavaConversions._
import scala.language.postfixOps

import org.codehaus.jackson.node.ArrayNode
import org.codehaus.jackson.node.ObjectNode

import com.opendatagroup.hadrian.datatype.AvroConversions._
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine
import com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine
import com.opendatagroup.hadrian.util.convertFromJson

object Main {
  case class SaveState(baseName: String = "state_", freqSeconds: Int = 600)

  case class Config(numberOfEngines: Int = 1, queueSizeLimit: Int = 100, engine: File = new File(""), output: File = new File(""), messages: Option[File] = None, inputs: Seq[File] = Nil, flush: Boolean = false, json: Boolean = false, saveState: Option[SaveState] = None)

  class MessageException(msg: String) extends Exception(msg)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("java -jar hadrian-standalone.jar") {
      head(" ")

      opt[Int]('n', "numberOfEngines")
        .validate(x => if (x > 0) success else failure("numberOfEngines must be greater than 0"))
        .action((x, c) => c.copy(numberOfEngines = x))
        .text("number of engines to run (default: 1)")

      opt[Int]('q', "queueSizeLimit")
        .validate(x => if (x > 0) success else failure("queueSizeLimit must be greater than 0"))
        .action((x, c) => c.copy(queueSizeLimit = x))
        .text("maximum size of the input and output queues before applying backpressure (default: 100)")

      opt[File]('e', "engineFileName")
        .required
        .valueName("engineFileName.pfa|json|yaml")
        .validate({x => 
          val y = x.getName;
          if (y.endsWith(".pfa")  ||  y.endsWith(".json")  ||  y.endsWith(".yaml"))
            if (x.exists)
              success
            else
              failure(x.getAbsolutePath + " does not exist")
          else
            failure("engineFileName must end with .pfa, .json, or .yaml")})
        .action((x, c) => c.copy(engine = x))
        .text("scoring engine encoded in PFA")

      opt[File]('o', "output")
        .required
        .valueName("output.avro|json")
        .validate({x => 
          if (x.getName.endsWith(".json")  ||  x.getName.endsWith(".avro"))
            success
          else
            failure("output file name must end with .json or .avro")})
        .action((x, c) => c.copy(output = x))
        .text("file name for output (may be a named pipe to avoid disk access with streams)")

      opt[File]('m', "messages")
        .optional
        .valueName("messages.json")
        .validate({x => 
          if (x.getName.endsWith(".json"))
            if (x.exists)
              success
            else
              failure(x.getAbsolutePath + " does not exist")
          else
            failure("messages file name must end with .json")})
        .action((x, c) => c.copy(messages = Some(x)))
        .text("file name for messages input (may be a named pipe to avoid disk access with streams)")

      opt[Unit]('f', "flush")
        .optional
        .action((_, c) => c.copy(flush = true))
        .text("flush output buffer after every result")

      opt[Unit]('j', "json")
        .optional
        .action((_, c) => c.copy(json = true))
        .text("write output as JSON, rather than Avro")

      opt[String]('s', "saveState")
        .optional
        .valueName("baseNameOrPath")
        .validate({x =>
          val y = new File(x.split("/").init.mkString("/"))
          if (y.exists)
            success
          else
            failure(y.getAbsolutePath + " does not exist")})
        .action((x, c) => c.saveState match {
          case Some(y) => c.copy(saveState = Some(y.copy(baseName = x)))
          case None => c.copy(saveState = Some(SaveState(baseName = x)))
        })
        .text("base file name for saving the scoring engine state (a directory, file name prefix, or both)")

      opt[Int]('S', "saveFreq")
        .optional
        .valueName("frequencySeconds")
        .validate(x => if (x > 0) success else failure("number of seconds must be positive"))
        .action((x, c) => c.saveState match {
          case Some(y) => c.copy(saveState = Some(y.copy(freqSeconds = x)))
          case None => c.copy(saveState = Some(SaveState(freqSeconds = x)))
        })
        .text("number of seconds to wait between attempts to save the scoring engine state state")

      help("help") text("print this help message")

      arg[File]("input1.avro[, input2.avro[, ...]]")
        .minOccurs(1)
        .maxOccurs(1024)
        .validate({x =>
          if (x.getName.endsWith(".avro"))
            if (x.exists)
              success
            else
              failure(x.getAbsolutePath + " does not exist")
          else
            failure("input file names must end with .avro")})
        .action((x, c) => c.copy(inputs = c.inputs :+ x))
        .text("input file names (may be named pipes to avoid disk access with streams)")

      note("""
Hadrian standalone builds a multithreaded PFA workflow within a single
process.  This workflow monitors all input streams and collates them
on a concurrent queue, while instances of the PFA scoring engine
execute them and push results to the output stream.  An optional
messages input accepts requests to change model parameters while
the engines are running.
""")
    }
    parser.parse(args, Config()) map {config =>
      val logger = logging.Logger.getLogger(getClass.getName)
      logger.setLevel(logging.Level.INFO)
      logger.getParent.getHandlers.head.setFormatter(new logging.Formatter {
        override def format(record: logging.LogRecord): String =
          new java.util.Date + " " + record.getLevel + " " + record.getMessage + "\n"
      })

      logger.log(logging.Level.INFO, s"Loading ${config.numberOfEngines} engines")

      val before = System.currentTimeMillis()

      val engines =
        if (config.engine.getName.endsWith(".pfa")  ||  config.engine.getName.endsWith(".json"))
          PFAEngine.fromJson(config.engine, multiplicity = config.numberOfEngines)
        else if (config.engine.getName.endsWith(".yaml"))
          PFAEngine.fromYaml(new java.util.Scanner(config.engine).useDelimiter("\\Z").next(), multiplicity = config.numberOfEngines)
        else
          throw new RuntimeException(s"unrecognized file name extension for engine file ${config.engine.getName}")

      val after = System.currentTimeMillis()

      logger.log(logging.Level.INFO, "Engines took %g seconds to load".format((after - before) / 1000.0))

      val inputQueue = new java.util.concurrent.ConcurrentLinkedQueue[AnyRef]
      val outputQueue = new java.util.concurrent.ConcurrentLinkedQueue[AnyRef]
      val inputQueueSize = new java.util.concurrent.atomic.AtomicInteger
      val inputQueueIndex = new java.util.concurrent.atomic.AtomicLong
      val outputQueueSize = new java.util.concurrent.atomic.AtomicInteger
      val outputQueueIndex = new java.util.concurrent.atomic.AtomicLong
      val enginesRunning = new java.util.concurrent.atomic.AtomicInteger

      logger.log(logging.Level.INFO, "Setting up engine threads")

      var engineThreads: List[Thread] = Nil
      for ((engine, index) <- engines.zipWithIndex) {
        val newThread = engine match {
          case emitEngine: PFAEmitEngine[_, _] =>
            new Thread(new Runnable {
              def run(): Unit = {
                emitEngine.emit = {x: AnyRef =>
                  outputQueue.add(x);
                  outputQueueSize.getAndIncrement()
                  outputQueueIndex.getAndIncrement()
                }
                while (true) {
                  val datum = inputQueue.poll()
                  if (datum != null) {
                    inputQueueSize.getAndDecrement()
                    enginesRunning.getAndIncrement()
                    engine.action(datum)
                    enginesRunning.getAndDecrement()
                  }
                  else
                    Thread.sleep(1)
                }
              }
            }, "Hadrian-engine-%02d".format(index))
          case _ =>
            new Thread(new Runnable {
              def run(): Unit =
                while (true) {
                  val datum = inputQueue.poll()
                  if (datum != null) {
                    inputQueueSize.getAndDecrement()
                    enginesRunning.getAndIncrement()
                    outputQueue.add(engine.action(datum))
                    enginesRunning.getAndDecrement()
                    outputQueueSize.getAndIncrement()
                    outputQueueIndex.getAndIncrement()
                  }
                  else
                    Thread.sleep(1)
                }
            }, "Hadrian-engine-%02d".format(index))
        }
        newThread.setDaemon(true)
        engineThreads = newThread :: engineThreads
      }

      val messagesThread = config.messages map {messagesInput =>
        logger.log(logging.Level.INFO, "Setting up the messages thread")

        val out = new Thread(new Runnable {
          def run(): Unit = {
            val scanner = new java.util.Scanner(new java.io.FileInputStream(messagesInput.getAbsolutePath))
            while (scanner.hasNextLine)
              try {
                val jsonText = scanner.nextLine()
                convertFromJson(jsonText) match {
                  case jsonNode: ObjectNode if (jsonNode.size == 1) => {
                    val functionName = jsonNode.getFieldNames.next()

                    val fcnDef = engines.head.config.fcns.get(functionName) match {
                      case Some(x) => x
                      case None => throw new MessageException(s"""function "${functionName}" not found in model""")
                    }

                    val paramTypes = fcnDef.params map {case (n, t) => t} toVector

                    jsonNode.get(functionName) match {
                      case argumentsNode: ArrayNode => {
                        if (paramTypes.size != argumentsNode.size)
                          throw new MessageException(s"""function "$functionName" takes ${paramTypes.size} arguments, but ${argumentsNode.size} have been provided""")

                        val arguments =
                          for (i <- 0 until argumentsNode.size) yield
                            try {
                              engines.head.fromJson(argumentsNode.get(i).toString, paramTypes(i))
                            }
                            catch {
                              case x: org.apache.avro.AvroTypeException => throw new MessageException(s"""argument $i (${fcnDef.paramNames(i)}) of function "$functionName" should have type ${paramTypes(i)}: ${x.getMessage}""")
                            }
                        
                        logger.log(logging.Level.INFO, s"Calling function: ${jsonText}")

                        engines foreach {engine =>
                          arguments.size match {
                            case 0 => engine.fcn0(functionName).apply()
                            case 1 => engine.fcn1(functionName).apply(arguments(0))
                            case 2 => engine.fcn2(functionName).apply(arguments(0), arguments(1))
                            case 3 => engine.fcn3(functionName).apply(arguments(0), arguments(1), arguments(2))
                            case 4 => engine.fcn4(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3))
                            case 5 => engine.fcn5(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4))
                            case 6 => engine.fcn6(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5))
                            case 7 => engine.fcn7(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6))
                            case 8 => engine.fcn8(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7))
                            case 9 => engine.fcn9(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8))
                            case 10 => engine.fcn10(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8), arguments(9))
                            case 11 => engine.fcn11(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8), arguments(9), arguments(10))
                            case 12 => engine.fcn12(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8), arguments(9), arguments(10), arguments(11))
                            case 13 => engine.fcn13(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8), arguments(9), arguments(10), arguments(11), arguments(12))
                            case 14 => engine.fcn14(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8), arguments(9), arguments(10), arguments(11), arguments(12), arguments(13))
                            case 15 => engine.fcn15(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8), arguments(9), arguments(10), arguments(11), arguments(12), arguments(13), arguments(14))
                            case 16 => engine.fcn16(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8), arguments(9), arguments(10), arguments(11), arguments(12), arguments(13), arguments(14), arguments(15))
                            case 17 => engine.fcn17(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8), arguments(9), arguments(10), arguments(11), arguments(12), arguments(13), arguments(14), arguments(15), arguments(16))
                            case 18 => engine.fcn18(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8), arguments(9), arguments(10), arguments(11), arguments(12), arguments(13), arguments(14), arguments(15), arguments(16), arguments(17))
                            case 19 => engine.fcn19(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8), arguments(9), arguments(10), arguments(11), arguments(12), arguments(13), arguments(14), arguments(15), arguments(16), arguments(17), arguments(18))
                            case 20 => engine.fcn20(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8), arguments(9), arguments(10), arguments(11), arguments(12), arguments(13), arguments(14), arguments(15), arguments(16), arguments(17), arguments(18), arguments(19))
                            case 21 => engine.fcn21(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8), arguments(9), arguments(10), arguments(11), arguments(12), arguments(13), arguments(14), arguments(15), arguments(16), arguments(17), arguments(18), arguments(19), arguments(20))
                            case 22 => engine.fcn22(functionName).apply(arguments(0), arguments(1), arguments(2), arguments(3), arguments(4), arguments(5), arguments(6), arguments(7), arguments(8), arguments(9), arguments(10), arguments(11), arguments(12), arguments(13), arguments(14), arguments(15), arguments(16), arguments(17), arguments(18), arguments(19), arguments(20), arguments(21))
                            case _ => throw new MessageException("functions can't have more than 22 arguments")
                          }
                        }

                      }
                      case x => throw new MessageException(s"""must have the form {"functionName": [arg1, arg2, ...]}, not ${x.toString}""")
                    }
                  }
                  case x => throw new MessageException(s"""must have the form {"functionName": [arg1, arg2, ...]}, not ${x.toString}""")
                }
              }
              catch {
                case x: MessageException => logger.log(logging.Level.WARNING, "Message error: " + x.getMessage)
                case x: Exception => logger.log(logging.Level.WARNING, "Message error (" + x.getClass.getName + "): " + x.getMessage)
              }
          }
        }, "Hadrian-messages")
        out.setDaemon(true)
        out
      }

      val saveStateThread = config.saveState map {saveState =>
        logger.log(logging.Level.INFO, "Setting up save-state thread")

        new Thread(new Runnable {
          def run(): Unit = {
            println("waiting")

            Thread.sleep(saveState.freqSeconds * 1000L)

            println("running")

            val timestamp = new java.sql.Timestamp((new java.util.Date).getTime).toString.take(19).replace(" ", "T")
            val snapshots = engines map {_.snapshot}

            for ((engine, index) <- engines.zipWithIndex) {
              val fileName =
                if (engines.size > 1) {
                  val digits = Math.floor(Math.log10(engines.size)).toInt
                  val formatter = "%0" + digits.toString + "d"
                  saveState.baseName + "engine" + formatter.format(index) + "_" + timestamp + ".pfa"
                }
                else
                  saveState.baseName + timestamp + ".pfa"

              println("fileName", fileName)

              val out = new java.io.PrintWriter(new File(fileName))
              out.print(snapshots(index).toString)
            }
          }
        }, "Hadrian-saveState")
      }
      saveStateThread foreach {_.setDaemon(true)}

      logger.log(logging.Level.INFO, "Setting up input threads")

      var inputThreads: List[Thread] = Nil
      for (pipe <- config.inputs)
        inputThreads = new Thread(new Runnable {
          def run(): Unit = {
            val namedPipe = new java.io.FileInputStream(pipe.getAbsolutePath)
            val inputIterator = engines.head.avroInputIterator[AnyRef](namedPipe)
            while (inputIterator.hasNext) {
              while (inputQueueSize.get > config.queueSizeLimit  ||  outputQueueSize.get > config.queueSizeLimit)
                Thread.sleep(1)

              inputQueue.add(inputIterator.next())
              inputQueueSize.getAndIncrement()
              inputQueueIndex.getAndIncrement()
            }
          }
        }, s"Hadrian-input(${pipe.getAbsolutePath})") :: inputThreads

      logger.log(logging.Level.INFO, "Setting up output thread")
      val outputFile =
        if (config.json)
          Left(new java.io.PrintWriter(config.output.getAbsolutePath))
        else
          Right(engines.head.avroOutputDataFileWriter(config.output.getAbsolutePath))

      val outputThread = new Thread(new Runnable {
        def run(): Unit = {
          while (outputQueueSize.get > 0  ||  inputThreads.forall(_.isAlive)) {
            val out = outputQueue.poll()
            if (out != null) {
              outputQueueSize.getAndDecrement()
              outputFile match {
                case Left(x) => x.println(engines.head.jsonOutput(out)); if (config.flush) x.flush()
                case Right(x) => x.append(out); if (config.flush) x.flush()
              }
            }
            else
              Thread.sleep(1)
          }
          outputFile match {
            case Left(x) => x.close()
            case Right(x) => x.close()
          }
        }
      }, "Hadrian-output")

      logger.log(logging.Level.INFO, "Setting up monitor thread")
      val monitorThread = new Thread(new Runnable {
        def run(): Unit = {
          val samples = 100
          var inputQueueSum = 0.0
          var outputQueueSum = 0.0
          var enginesRunningSum = 0.0
          var heapMemoryUsedSum = 0.0
          val gbPerByte = 1.0 / 1024.0 / 1024.0 / 1024.0
          var cumulativeTimeInGC = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans.map(_.getCollectionTime).filter(_ > 0).sum
          var lastTime = System.currentTimeMillis()
          var lastInputIndex = 0L
          var lastOutputIndex = 0L
          while (true) {
            inputQueueSum = 0.0
            outputQueueSum = 0.0
            enginesRunningSum = 0.0
            heapMemoryUsedSum = 0.0
            for (i <- 0 until samples) {
              inputQueueSum += inputQueueSize.doubleValue
              outputQueueSum += outputQueueSize.doubleValue
              enginesRunningSum += enginesRunning.doubleValue
              heapMemoryUsedSum += java.lang.management.ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed.toDouble
              Thread.sleep(Math.ceil(1000 / samples).toInt)
            }
            val inputQueueAve = inputQueueSum/samples
            val outputQueueAve = outputQueueSum/samples
            val enginesRunningAve = enginesRunningSum/samples
            val heapUsedAve = heapMemoryUsedSum/samples*gbPerByte
            val heapMax = java.lang.management.ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getMax*gbPerByte

            val newTotalTimeInGC = java.lang.management.ManagementFactory.getGarbageCollectorMXBeans.map(_.getCollectionTime).filter(_ > 0).sum

            val currentTime = System.currentTimeMillis()
            val inputIndex = inputQueueIndex.get
            val outputIndex = outputQueueIndex.get

            val timeDifference = currentTime - lastTime
            val timeInGC = Math.ceil(100.0 * (newTotalTimeInGC - cumulativeTimeInGC) / timeDifference).toInt
            cumulativeTimeInGC = newTotalTimeInGC
            lastTime = currentTime

            val inputDifference = inputIndex - lastInputIndex
            val outputDifference = outputIndex - lastOutputIndex
            val inputRate = 1000.0 * inputDifference / timeDifference
            val outputRate = 1000.0 * outputDifference / timeDifference
            lastInputIndex = inputIndex
            lastOutputIndex = outputIndex

            logger.log(logging.Level.INFO, f"""{"inputQ": $inputQueueAve%.2f, "in/s": $inputRate%.2f, "outputQ": $outputQueueAve%.2f, "out/s": $outputRate%.2f, "engines#": $enginesRunningAve%.2f, "GC%%": $timeInGC%d, "heapGB,max": [$heapUsedAve%.2f, $heapMax%.2f]}""")
          }
        }
      }, "Hadrian-monitor")
      monitorThread.setDaemon(true)

      logger.log(logging.Level.INFO, "Ready to score")

      engineThreads.foreach(_.start())
      inputThreads.foreach(_.start())
      messagesThread.foreach(_.start())
      saveStateThread.foreach(_.start())
      outputThread.start()
      monitorThread.start()
    }
  }
}
