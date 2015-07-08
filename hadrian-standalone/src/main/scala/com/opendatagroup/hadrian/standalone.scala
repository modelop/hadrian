package com.opendatagroup.hadrian.standalone

import java.io.File
import java.io.InputStream
import java.io.FileInputStream

import scala.collection.JavaConversions._
import scala.language.postfixOps

import org.codehaus.jackson.node.ArrayNode
import org.codehaus.jackson.node.ObjectNode

import com.opendatagroup.hadrian.datatype.AvroConversions._
import com.opendatagroup.hadrian.jvmcompiler.PFAEngine
import com.opendatagroup.hadrian.jvmcompiler.PFAEmitEngine
import com.opendatagroup.hadrian.util.convertFromJson

import com.opendatagroup.antinous.engine.JythonEngine

object Main {
  object Format extends Enumeration {
    val AVRO, JSON, JSONSCHEMA = Value
  }

  case class Config(numberOfEngines: Int = 1,
                    engine: File = new File(""),
                    inputFiles: Seq[File] = Seq[File](),
                    inputFormat: Format.Value = Format.AVRO,
                    outputFormat: Format.Value = Format.AVRO,
                    saveState: Option[String] = None,
                    printTime: Boolean = false,
                    debug: Boolean = false)

  def main(args: Array[String]): Unit = {
    val parser = new scopt.OptionParser[Config]("java -jar hadrian-standalone.jar") {
      head(" ")

      opt[Int]('n', "numberOfEngines")
        .optional
        .validate(x => if (x > 0) success else failure("numberOfEngines must be greater than 0"))
        .action((x, c) => c.copy(numberOfEngines = x))
        .text("number of engines to run (default: 1)")

      opt[String]('i', "inputFormat")
        .optional
        .validate(x => if (x == "avro"  ||  x == "json") success else failure("inputFormat must be \"avro\" or \"json\""))
        .action((x, c) => c.copy(inputFormat = if (x == "avro") Format.AVRO else Format.JSON))
        .text("input format: \"avro\" (default) or \"json\"")

      opt[String]('o', "outputFormat")
        .optional
        .validate(x => if (x == "avro"  ||  x == "json"  ||  x == "json+schema") success else failure("outputFormat must be \"avro\", \"json\", or \"json+schema\""))
        .action((x, c) => c.copy(outputFormat = x match {
          case "avro" => Format.AVRO
          case "json" => Format.JSON
          case "json+schema" => Format.JSONSCHEMA
        }))
        .text("output format: \"avro\" (default), \"json\", or \"json+schema\"")

      opt[String]('s', "saveState")
        .optional
        .valueName("baseNameOrPath")
        .validate({x =>
          val y = new File(x.split("/").init.mkString("/") + "/")
          if (y.exists)
            success
          else
            failure(y.getAbsolutePath + " does not exist")
        })
        .action((x, c) => c.copy(saveState = Some(x)))
        .text("base file name for saving the scoring engine state (a directory, file name prefix, or both)")

      opt[Unit]("printTime")
        .optional
        .action((_, c) => c.copy(printTime = true))
        .text("print average time of action to standard error (approximately every 10 seconds, does not include data input/output)")

      opt[Unit]("debug")
        .action((x, c) => c.copy(debug = true))
        .text("print out auto-generated Java code for PFA engine")

      arg[File]("engineFileName.pfa|json|yaml|yml|py")
        .required
        .validate({x => 
          if (x.exists) {
            val name = x.getName.toLowerCase
            if (name.endsWith(".pfa")  ||  name.endsWith(".json")  ||  name.endsWith(".yaml")  ||  name.endsWith(".yml")  ||  name.endsWith(".py"))
              success
            else
              failure(x.getName + " does not end with .pfa|json|yaml|yml|py")
          }
          else
            failure(x.getAbsolutePath + " does not exist")
        })
        .action((x, c) => c.copy(engine = x))
        .text("scoring engine encoded in PFA (pfa|json|yaml|yml) or Python (py)")

      arg[File]("inputFile.json|avro")
        .minOccurs(0)
        .maxOccurs(1024)
        .validate({x => 
          if (x.exists)
            success
          else
            failure(x.getAbsolutePath + " does not exist")})
        .action((x, c) => c.copy(inputFiles = c.inputFiles :+ x))
        .text("optional input files (if omitted, data are taken from standard input)")

      help("help") text("print this help message")

      note("""
Hadrin standalone runs a PFA-encoded scoring engine as a standard
input to standard output process.  If multiple engines are specified,
these engines run in parallel and may share state.  If a --saveState
option is provided, the final state is written to a file at the end of
input.
""")
    }
    parser.parse(args, Config()) map {config =>
      val normalizedName = config.engine.getName.toLowerCase
      val engines =
        if (normalizedName.endsWith(".pfa")  ||  normalizedName.endsWith(".json"))
          PFAEngine.fromJson(config.engine, multiplicity = config.numberOfEngines, debug = config.debug)
        else if (normalizedName.endsWith(".yaml")  ||  normalizedName.endsWith(".yml"))
          PFAEngine.fromYaml(new java.util.Scanner(config.engine).useDelimiter("\\Z").next(), multiplicity = config.numberOfEngines, debug = config.debug)
        else
          JythonEngine.fromPython(config.engine, config.engine.getName, multiplicity = config.numberOfEngines)

      // every engine input bucket is a fermion: it can only take zero or one items; if full, System.in blocks
      val inputBuckets = Array.fill[Option[AnyRef]](config.numberOfEngines)(None)

      // the one and only output queue is a boson: it takes as many items as is necessary; no blocking, but maybe run out of memory
      val outputQueue = new java.util.concurrent.ConcurrentLinkedQueue[Option[AnyRef]]

      // keeps going while there's input
      class InputRunnable extends Runnable {
        def runOne(inputStream: InputStream) {
          val inputIterator = config.inputFormat match {
            case Format.AVRO => engines.head.avroInputIterator[AnyRef](inputStream)
            case Format.JSON => engines.head.jsonInputIterator[AnyRef](inputStream)
          }

          while (inputIterator.hasNext) {
            inputBuckets.indexOf(None) match {
              case -1 =>
                Thread.sleep(1)
              case i =>
                inputBuckets(i) = Some(inputIterator.next())
            }
          }
        }
        def run(): Unit =
          if (config.inputFiles.isEmpty)
            runOne(System.in)
          else
            config.inputFiles foreach {x => runOne(new FileInputStream(x))}
      }
      val inputThread = new Thread(new InputRunnable, "Hadrian-input")

      // keeps going while inputThread is alive or there's work to be done
      class EngineRunnable(engine: PFAEngine[_, _]) extends Runnable {
        private var lastPrintout = 0L
        private var timeInAction = 0L
        private var callsToAction = 0.0

        def action(engine: PFAEngine[_, _], input: AnyRef): AnyRef = {
          val startTime = System.currentTimeMillis
          val out = engine.asInstanceOf[PFAEngine[AnyRef, AnyRef]].action(input)
          val endTime = System.currentTimeMillis

          timeInAction += endTime - startTime
          callsToAction += 1.0

          if (endTime - lastPrintout > 10000  ||  lastPrintout == 0L) {
            if (config.printTime  &&  lastPrintout != 0L)
              System.err.println(timeInAction / callsToAction)
            timeInAction = 0L
            callsToAction = 0.0
            lastPrintout = endTime
          }

          out
        }

        def run() {
          val processInput =
            engine match {
              case emitEngine: PFAEmitEngine[_, _] =>
                emitEngine.emit = {fx: AnyRef => outputQueue.add(Some(fx))}
                {x: AnyRef => action(emitEngine, x)}

              case _ =>
                {x: AnyRef => outputQueue.add(Some(action(engine, x)))}
            }

          val index = engine.instance
          engine.begin()
          while (inputThread.isAlive  ||  inputBuckets(index) != None) {
            inputBuckets(index) match {
              case None =>
                Thread.sleep(1)  // no work to be done; skip a millisecond
              case Some(x) =>
                inputBuckets(index) = None
                processInput(x)
            }
          }
          engine.end()
        }
      }
      val engineThreads = engines map {x => new Thread(new EngineRunnable(x), s"Hadrian-engine-${x.instance}")}

      // keeps going while any engines are or there's work to be done
      class OutputRunnable extends Runnable {
        def run() {
          val outputFile = config.outputFormat match {
            case Format.AVRO => engines.head.avroOutputDataStream(System.out)
            case Format.JSON => engines.head.jsonOutputDataStream(System.out, false)
            case Format.JSONSCHEMA => engines.head.jsonOutputDataStream(System.out, true)
          }

          while (engineThreads.exists(_.isAlive)  ||  !outputQueue.isEmpty) {
            val out = outputQueue.poll()
            if (out == null)
              Thread.sleep(1)  // no work to be done; skip a millisecond
            else
              outputFile.append(out.get)
          }

          outputFile.close()

          // if the user wants a snapshot, do it at the end of processing
          config.saveState foreach {baseName =>
            engines foreach {engine =>
              val fileName =
                if (engines.size > 1) {
                  val digits = Math.ceil(Math.log10(engines.size)).toInt
                  val formatter = "%0" + digits.toString + "d"
                  baseName + formatter.format(engine.instance) + ".pfa"
                }
                else
                  baseName + ".pfa"

              val out = new java.io.PrintWriter(new java.io.File(fileName))
              out.print(engine.snapshot)
              out.close()
            }
          }
        }
      }
      val outputThread = new Thread(new OutputRunnable, "Hadrian-output")

      inputThread.start()
      engineThreads foreach {_.start()}
      outputThread.start()
    }
  }
}
