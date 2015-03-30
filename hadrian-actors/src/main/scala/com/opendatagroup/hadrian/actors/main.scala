package com.opendatagroup.hadrian.actors

import scala.collection.JavaConversions._

import org.apache.log4j

import com.opendatagroup.hadrian.actors.config.stringToURL

package main {
  object Main {
    val logger = log4j.Logger.getLogger("Hadrian-Actors")

    case class CommandLine(logConfig: Option[java.net.URL] = None, monitorFreqSeconds: Double = 1.0, topologyConfig: java.net.URL = null, queueMemoryLimit: Double = 0.9)

    def main(args: Array[String]): Unit = {
      val parser = new scopt.OptionParser[CommandLine]("java -jar hadrian-actors.jar") {
        head(" ")

        opt[String]("log")
          .minOccurs(0)
          .maxOccurs(1)
          .valueName("log4j.xml")
          .action((x, c) => c.copy(logConfig = Some(stringToURL(x))))
          .text("URL to configure log4j (default is in 'jar -xf hadrian-actors.jar resources/log4j.xml')")

        opt[Double]("monitorFreqSeconds")
          .minOccurs(0)
          .maxOccurs(1)
          .valueName("1.0")
          .action((x, c) => c.copy(monitorFreqSeconds = x))
          .text("number of seconds between monitor INFO messages")

        opt[Double]("queueMemoryLimit")
          .minOccurs(0)
          .maxOccurs(1)
          .action((x, c) => c.copy(queueMemoryLimit = x))
          .text("used/max memory above which data will be dropped from queues")

        arg[String]("topology.json|yaml")
          .minOccurs(1)
          .maxOccurs(1)
          .action((x, c) => c.copy(topologyConfig = stringToURL(x)))
          .text("topology description file")

        note("""
Hadrian-Actors builds an actor-based workflow within a single process.
The workflow can be as general as a directed acyclic graph (DAG), can
contain PFA engines (inline or from external files), executable
functions in external JAR files, and executable scripts, provided by a
shell command.  At any stage, results can be saved to a file or named
pipe.
""")
      }

      val serviceLoader = java.util.ServiceLoader.load(classOf[config.ExtensionsLoader])
      for (loader <- serviceLoader)
        config.Extensions.load(loader)

      parser.parse(args, CommandLine()) map {commandLine =>
        commandLine.logConfig match {
          case Some(x) => log4j.xml.DOMConfigurator.configure(x)
          case None =>
            val log4jconfig = new java.util.Scanner(getClass.getResourceAsStream("/resources/log4j.xml")).useDelimiter("\\Z").next()
            println(s"Configuring log4j with this file (from 'jar -xf hadrian-actors.jar resources/log4j.xml'):\n\n$log4jconfig\n")
            log4j.xml.DOMConfigurator.configure(getClass.getResource("/resources/log4j.xml"))
        }

        val topologyFileData = new java.util.Scanner(commandLine.topologyConfig.openStream).useDelimiter("\\Z").next()
        val topology =
          try {
            config.jsonToTopology(topologyFileData)
          }
          catch {
            case exception: Exception =>
              config.yamlToTopology(topologyFileData)
          }

        workflow.createActors(topology)
        workflow.start(commandLine.monitorFreqSeconds, commandLine.queueMemoryLimit)
        try {
          while (true)
            Thread.sleep(1000)
        }
        catch {
          case exception: Exception =>
            workflow.closeFiles()
            throw exception
        }
      }
    }
  }
}
