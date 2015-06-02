package com.opendatagroup.hadrian.actors

import java.io.FileFilter

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.postfixOps
import scala.reflect.ClassTag

import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.ArrayNode
import org.codehaus.jackson.node.IntNode
import org.codehaus.jackson.node.NumericNode
import org.codehaus.jackson.node.ObjectNode
import org.codehaus.jackson.node.TextNode
import org.codehaus.jackson.node.BooleanNode

import com.opendatagroup.hadrian.ast.EngineConfig
import com.opendatagroup.hadrian.reader.jsonToAst
import com.opendatagroup.hadrian.yaml.yamlToAst
import com.opendatagroup.hadrian.datatype._

package object config {
  val objectMapper = new ObjectMapper

  def stringToURL(x: String): java.net.URL =
    try {
      new java.net.URL(x)
    }
    catch {
      case exception: java.net.MalformedURLException =>
        new java.net.URL("file", "", -1, x)
    }

  def readAvroType(objectNode: ObjectNode, name: String, paramName: String): AvroType = objectNode.get("type") match {
    case null => throw new ConfigurationException(s"""$name parameter "$paramName" is missing""")
    case textNode: TextNode =>
      try {
        AvroConversions.stringToAvroType(objectMapper.writeValueAsString(textNode.getTextValue))
      }
      catch {
        case exception: Exception =>
          val location = textNode.getTextValue
          val src = new java.util.Scanner(stringToURL(location).openStream).useDelimiter("\\A").next()
          try {
            AvroConversions.stringToAvroType(src)
          }
          catch {
            case exception: Exception => AvroConversions.stringToAvroType(yamlToTopology.yamlToJson(src))
          }
      }
    case x => AvroConversions.stringToAvroType(objectMapper.writeValueAsString(x))
  }

  def readWatchMemory(obj: JsonNode) = obj match {
    case null => None
    case BooleanNode.FALSE => None
    case BooleanNode.TRUE => Some(WatchMemory())
    case x => throw new ConfigurationException(s"""pfa-engine parameter "watchMemory" must be boolean, not ${x.toString}""")
  }

  def readDestination(jsonNode: JsonNode): Destination = jsonNode match {
    case objectNode: ObjectNode =>
      if (objectNode.get("to") != null) {
        // this destination is a queue
        val to = objectNode.get("to") match {
          case textNode: TextNode => textNode.getTextValue
          case x => throw new ConfigurationException(s"""queue-destination parameter "to" must be a string, not ${x.toString}""")
        }

        val hashKeys = objectNode.get("hashKeys") match {
          case null => Seq[String]()
          case arrayNode: ArrayNode =>
            arrayNode.getElements.toSeq map {
              case textNode: TextNode => textNode.getTextValue
              case x => throw new ConfigurationException(s"""queue-destination parameter "hashKeys" array elements must be strings, not ${x.toString}""")
            }
          case x => throw new ConfigurationException(s"""queue-destination parameter "hashKeys" must be an array of strings, not ${x.toString}""")
        }

        val limitBuffer = objectNode.get("limitBuffer") match {
          case null => None
          case intNode: IntNode if (intNode.getIntValue > 0) => Some(intNode.getIntValue)
          case textNode: TextNode if (textNode.getTextValue == "none") => None
          case x => throw new ConfigurationException(s"""queue-destination parameter "limitBuffer" must be \"none\" or a positive integer, not ${x.toString}""")
        }

        val watchMemoryOrCount = objectNode.get("watchMemory") match {
          case null => None
          case textNode: TextNode if (textNode.getTextValue == "count") => Some(new WatchMemoryOrCount)
          case x => readWatchMemory(x)
        }

        Queue(to, hashKeys, limitBuffer, watchMemoryOrCount)
      }

      else if (objectNode.get("file") != null) {
        // this destination is a file or named pipe
        val file = objectNode.get("file") match {
          case textNode: TextNode => new java.io.File(textNode.getTextValue)
          case x => throw new ConfigurationException(s"""file-destination parameter "file" must be a string, not ${x.toString}""")
        }

        val format = objectNode.get("format") match {
          case null => throw new ConfigurationException("""file-destination parameter "format" is missing""")
          case textNode: TextNode if (textNode.getTextValue.toUpperCase == "AVRO") => Format.AVRO
          case textNode: TextNode if (textNode.getTextValue.toUpperCase == "JSON") => Format.JSON
          case textNode: TextNode if (textNode.getTextValue.toUpperCase == "JSON+SCHEMA") => Format.JSONSCHEMA
          case x => throw new ConfigurationException(s"""file-destination parameter "format" must be one of "avro", "json" and "json+schema", not ${x.toString}""")
        }

        val limitSeconds = objectNode.get("limitSeconds") match {
          case null => None
          case textNode: TextNode if (textNode.getTextValue == "none") => None
          case numericNode: NumericNode if (numericNode.getDoubleValue > 0.0) => Some(numericNode.getDoubleValue)
          case x => throw new ConfigurationException(s"""file-destination parameter "limitSeconds" must be a positive floating-point number or "none", not ${x.toString}""")
        }

        val limitRecords = objectNode.get("limitRecords") match {
          case null => None
          case textNode: TextNode if (textNode.getTextValue == "none") => None
          case numericNode: NumericNode if (numericNode.getDoubleValue > 0.0) => Some(numericNode.getLongValue)
          case x => throw new ConfigurationException(s"""file-destination parameter "limitRecords" must be a positive number or "none", not ${x.toString}""")
        }

        FileDestination(file, format, limitSeconds, limitRecords)
      }
      
      else {
        var out: ExtendedDestination[_] = null
        for (key <- Extensions.destinations.keys if (objectNode.get(key) != null)) {
          if (out != null)
            throw new ConfigurationException(s"""destination with keys [${objectNode.getFieldNames.mkString(" ")}] matches multiple destination types""")
          out = Extensions.readDestination(key, objectNode)
        }
        if (out == null)
          throw new ConfigurationException(s"""destination must have one of the following fields: [${(Seq("to", "file") ++ Extensions.destinations.keys).mkString(" ")}]""")
        out
      }

    case x => throw new ConfigurationException(s"""destination must be a map from parameter names to values, not ${x.toString}""")
  }

}

package config {
  //////////////////////////////////////// configuration for standard sources, processors, and destinations

  class ConfigurationException(msg: String) extends Exception(msg)

  object Format extends Enumeration {
    type Format = Value
    val AVRO, JSON, JSONSCHEMA = Value
  }

  case class Topology(nodes: Map[String, TopologyNode])

  trait TopologyNode {
    val destinations: Seq[Destination]
    def outputType: AvroType
  }

  trait HasInput {
    def inputType: AvroType
  }

  trait Source extends TopologyNode
  trait Processor extends TopologyNode with HasInput {
    def multiplicity: Int
  }
  trait Destination

  class WatchMemoryOrCount
  case class WatchMemory() extends WatchMemoryOrCount

  case class FileSource(file: java.io.File, outputType: AvroType, format: Format.Format, destinations: Seq[Destination]) extends Source

  case class SaveState(baseName: String, freqSeconds: Option[Int])
  case class Engine(engineConfig: EngineConfig, options: Map[String, JsonNode], multiplicity: Int, saveState: Option[SaveState], watchMemory: Option[WatchMemory], destinations: Seq[Destination]) extends Processor {
    def inputType = engineConfig.input
    def outputType = engineConfig.output
  }

  case class JarEngine(url: java.net.URL, className: String, inputType: AvroType, outputType: AvroType, multiplicity: Int, destinations: Seq[Destination]) extends Processor

  case class ShellEngine(cmd: Seq[String], dir: Option[String], env: Map[String, String],
    inputType: AvroType, outputType: AvroType, multiplicity: Int, destinations: Seq[Destination])
      extends Processor

  case class Queue(to: String, hashKeys: Seq[String], limitBuffer: Option[Int], watchMemoryOrCount: Option[WatchMemoryOrCount]) extends Destination
  case class FileDestination(file: java.io.File, format: Format.Format, limitSeconds: Option[Double], limitRecords: Option[Long]) extends Destination

  //////////////////////////////////////// extension sources, processors, and destinations

  trait ExtensionsLoader {
    def loadSources(): Map[String, java.lang.Class[_ <: ExtendedSource[_]]]
    def loadProcessors(): Map[String, java.lang.Class[_ <: ExtendedProcessor[_]]]
    def loadDestinations(): Map[String, java.lang.Class[_ <: ExtendedDestination[_]]]
  }

  abstract class ExtendedSource[STATE <: scala.Iterator[AnyRef]](objectNode: ObjectNode, implicit val stateType: ClassTag[STATE]) extends Source {
    def initialize(): STATE

    def readType(objectNode: ObjectNode, name: String): AvroType = readAvroType(objectNode, name, "type")

    def readDestinations(objectNode: ObjectNode, name: String): Seq[Destination] = objectNode.get("destinations") match {
      case null => Seq[Destination]()
      case arrayNode: ArrayNode => arrayNode.getElements.toSeq.map(readDestination)
      case x => throw new ConfigurationException(s"""$name parameter "destinations" must be an array of queues or outputs, not ${x.toString}""")
    }
  }

  trait HasBegin {
    def begin(): Unit
  }
  trait HasAction {
    def action(datum: AnyRef): Seq[AnyRef]
  }
  trait HasEnd {
    def end(): Unit
  }

  abstract class ExtendedProcessor[STATE <: HasAction](objectNode: ObjectNode, implicit val stateType: ClassTag[STATE]) extends Processor {
    def initialize(): STATE

    def readInput(objectNode: ObjectNode, name: String): AvroType = readAvroType(objectNode, name, "input")
    def readOutput(objectNode: ObjectNode, name: String): AvroType = readAvroType(objectNode, name, "output")

    def readMultiplicity(objectNode: ObjectNode, name: String): Int = objectNode.get("multiplicity") match {
      case null => 1
      case intNode: IntNode if (intNode.getIntValue > 0) => intNode.getIntValue
      case x => throw new ConfigurationException(s"""$name parameter "multiplicity" must be a positive integer, not ${x.toString}""")
    }

    def readDestinations(objectNode: ObjectNode, name: String): Seq[Destination] = objectNode.get("destinations") match {
      case null => Seq[Destination]()
      case arrayNode: ArrayNode => arrayNode.getElements.toSeq.map(readDestination)
      case x => throw new ConfigurationException(s"""$name parameter "destinations" must be an array of queues or outputs, not ${x.toString}""")
    }
  }

  trait HasSend {
    def send(datum: AnyRef): Unit
  }
  trait HasClose {
    def close(): Unit
  }

  abstract class ExtendedDestination[STATE <: HasSend](objectNode: ObjectNode, implicit val stateType: ClassTag[STATE]) extends Destination {
    def initialize(outputType: AvroType): STATE
  }

  object Extensions {
    val sources = mutable.Map[String, java.lang.Class[_ <: ExtendedSource[_]]]()
    val processors = mutable.Map[String, java.lang.Class[_ <: ExtendedProcessor[_]]]()
    val destinations = mutable.Map[String, java.lang.Class[_ <: ExtendedDestination[_]]]()

    def load(extensionsLoader: ExtensionsLoader): Unit = {
      sources ++= extensionsLoader.loadSources
      processors ++= extensionsLoader.loadProcessors
      destinations ++= extensionsLoader.loadDestinations
    }

    def readSource(key: String, objectNode: ObjectNode): ExtendedSource[_] =
      sources(key).getConstructor(classOf[ObjectNode]).newInstance(objectNode)

    def readProcessor(key: String, objectNode: ObjectNode): ExtendedProcessor[_] =
      processors(key).getConstructor(classOf[ObjectNode]).newInstance(objectNode)

    def readDestination(key: String, objectNode: ObjectNode): ExtendedDestination[_] =
      destinations(key).getConstructor(classOf[ObjectNode]).newInstance(objectNode)
  }

  //////////////////////////////////////// read in with JSON
  object jsonToTopology extends Function1[String, Topology] {
    def apply(src: java.io.File): Topology = jsonNodeToTopology(objectMapper.readTree(src))
    def apply(src: java.io.InputStream): Topology = jsonNodeToTopology(objectMapper.readTree(src))
    def apply(src: String): Topology = jsonNodeToTopology(objectMapper.readTree(src))

    private def findCycle(name: String, nodes: Map[String, TopologyNode], path: List[String] = Nil): Unit = {
      if (path.contains(name))
        throw new ConfigurationException(s"""topology contains a cycle: ${path.reverse.mkString(" -> ")}""")
      for (destination <- nodes(name).destinations) destination match {
        case queue: Queue => findCycle(queue.to, nodes, name :: path)
        case _ =>
      }
    }

    private def jsonNodeToTopology(jsonNode: JsonNode): Topology = jsonNode match {
      case objectNode: ObjectNode =>
        // read JSON objects into configuration
        val out = Topology(objectNode.getFields map {mapEntry =>
          val name = mapEntry.getKey
          val node = mapEntry.getValue
          (name, readNode(node))
        } toMap)

        for ((name, node) <- out.nodes;  destination <- node.destinations) destination match {
          case q: Queue =>
            // check for missing destinations
            if (!out.nodes.containsKey(q.to))
              throw new ConfigurationException(s"""${name} is configured to send data to ${q.to}, but this node does not exist""")

            // check for mismatched types (types must be exactly equal)
            out.nodes(q.to) match {
              case x: HasInput =>
                val a = node.outputType
                val b = x.inputType
                if (!a.accepts(b)  ||  !b.accepts(a))
                  throw new ConfigurationException(s"""${name} is configured to send data to ${q.to}, but ${name} has output type\n$a\nand ${q.to} has input type\n$b""")
              case _ => throw new ConfigurationException(s"""${name} is configured to send data to ${q.to}, but this is a source""")
            }
          case _ =>
        }

        // check for cycles
        for ((name, node) <- out.nodes) node match {
          case _: Source => findCycle(name, out.nodes)
          case _ =>
        }

        out

      case x =>
        throw new ConfigurationException(s"""topology must be a map from node names to nodes, not ${x.toString}""")
    }

    private def readNode(jsonNode: JsonNode): TopologyNode = jsonNode match {
      case objectNode: ObjectNode =>
        objectNode.get("node") match {
          case null => throw new ConfigurationException("""topology node parameter "node" is missing""")
          case textNode: TextNode if (textNode.getTextValue == "file-source") => readFileSource(objectNode)
          case textNode: TextNode if (textNode.getTextValue == "pfa-engine") => readPFAEngine(objectNode)
          case textNode: TextNode if (textNode.getTextValue == "jar-engine") => readJAREngine(objectNode)
          case textNode: TextNode if (textNode.getTextValue == "shell-engine") => readShellEngine(objectNode)
          case textNode: TextNode if (Extensions.sources.contains(textNode.getTextValue)) =>
            Extensions.readSource(textNode.getTextValue, objectNode)
          case textNode: TextNode if (Extensions.processors.contains(textNode.getTextValue)) =>
            Extensions.readProcessor(textNode.getTextValue, objectNode)
          case x =>
            throw new ConfigurationException(s"""topology node parameter "node" must be one of [${(Seq("file-source", "pfa-engine", "jar-engine", "shell-engine") ++ Extensions.sources.keys ++ Extensions.processors.keys).mkString(" ")}], not ${x.toString}""")
        }
      case x =>
        throw new ConfigurationException(s"""topology node must be a map from parameter names to values, not ${x.toString}""")
    }

    private def readFileSource(objectNode: ObjectNode): FileSource = {
      val file = objectNode.get("fileName") match {
        case null => throw new ConfigurationException("""file-source parameter "fileName" is missing""")
        case textNode: TextNode => new java.io.File(textNode.getTextValue)
        case x => throw new ConfigurationException(s"""file-source parameter "fileName" must be a string, not ${x.toString}""")
      }
        
      val outputType = readAvroType(objectNode, "file-source", "type")

      val format = objectNode.get("format") match {
        case null => throw new ConfigurationException("""file-source parameter "format" is missing""")
        case textNode: TextNode if (textNode.getTextValue.toUpperCase == "AVRO") => Format.AVRO
        case textNode: TextNode if (textNode.getTextValue.toUpperCase == "JSON") => Format.JSON
        case x => throw new ConfigurationException(s"""file-source parameter "format" must be one of "avro" and "json", not ${x.toString}""")
      }

      val destinations = objectNode.get("destinations") match {
        case null => Seq[Destination]()
        case arrayNode: ArrayNode => arrayNode.getElements.toSeq.map(readDestination)
        case x => throw new ConfigurationException(s"""file-source parameter "destinations" must be an array of queues or outputs, not ${x.toString}""")
      }

      FileSource(file, outputType, format, destinations)
    }

    private def readPFAEngine(objectNode: ObjectNode): Engine = {
      val engineConfig = objectNode.get("pfa") match {
        case null => throw new ConfigurationException("""pfa-engine parameter "pfa" is missing""")

        case textNode: TextNode =>
          val location = textNode.getTextValue
          try {
            val stream = stringToURL(location).openStream
            jsonToAst(stream)
          }
          catch {
            case exception: org.codehaus.jackson.JsonProcessingException =>
              val string = new java.util.Scanner(stringToURL(location).openStream).useDelimiter("\\A").next()
              yamlToAst(string)
          }

        case x => jsonToAst(objectMapper.writeValueAsString(x))
      }

      val options = objectNode.get("options") match {
        case null => Map[String, JsonNode]()
        case x: ObjectNode => x.getFields map {y => (y.getKey, y.getValue)} toMap
        case x => throw new ConfigurationException(s"""pfa-engine parameter "options" must be a map, not ${x.toString}""")
      }

      val multiplicity = objectNode.get("multiplicity") match {
        case null => 1
        case intNode: IntNode if (intNode.getIntValue > 0) => intNode.getIntValue
        case x => throw new ConfigurationException(s"""pfa-engine parameter "multiplicity" must be a positive integer, not ${x.toString}""")
      }

      val saveState = objectNode.get("saveState") match {
        case null => None

        case saveStateOptions: ObjectNode =>
          val baseName = saveStateOptions.get("baseName") match {
            case null => throw new ConfigurationException("""pfa-engine saveState parameter "baseName" is missing""")
            case textNode: TextNode => textNode.getTextValue
            case x => throw new ConfigurationException(s"""pfa-engine saveState parameter "baseName" must be a string, not ${x.toString}""")
          }

          val freqSeconds = saveStateOptions.get("freqSeconds") match {
            case null => None
            case intNode: IntNode if (intNode.getIntValue > 0) => Some(intNode.getIntValue)
            case x => throw new ConfigurationException(s"""pfa-engine saveState parameter "freqSeconds" must be a positive integer, not ${x.toString}""")
          }

          Some(SaveState(baseName, freqSeconds))

        case x => throw new ConfigurationException(s"""pfa-engine parameter "saveState" must be a map, not ${x.toString}""")
      }

      val watchMemory = readWatchMemory(objectNode.get("watchMemory"))

      val destinations = objectNode.get("destinations") match {
        case null => Seq[Destination]()
        case arrayNode: ArrayNode => arrayNode.getElements.toSeq.map(readDestination)
        case x => throw new ConfigurationException(s"""pfa-engine parameter "destinations" must be an array of queues or outputs, not ${x.toString}""")
      }

      Engine(engineConfig, options, multiplicity, saveState, watchMemory, destinations)
    }

    private def readJAREngine(objectNode: ObjectNode): JarEngine = {
      val url = objectNode.get("jar") match {
        case null => throw new ConfigurationException("""jar-engine parameter "jar" is missing""")
        case textNode: TextNode => stringToURL(textNode.getTextValue)
        case x => throw new ConfigurationException(s"""jar-engine parameter "jar" must be a string URL, not ${x.toString}""")
      }

      val className = objectNode.get("className") match {
        case null => throw new ConfigurationException("""jar-engine parameter "className" is missing""")
        case textNode: TextNode => textNode.getTextValue
        case x => throw new ConfigurationException(s"""jar-engine parameter "className" must be a string, not ${x.toString}""")
      }

      val inputType = readAvroType(objectNode, "jar-engine", "input")
      val outputType = readAvroType(objectNode, "jar-engine", "output")

      val multiplicity = objectNode.get("multiplicity") match {
        case null => 1
        case intNode: IntNode if (intNode.getIntValue > 0) => intNode.getIntValue
        case x => throw new ConfigurationException(s"""jar-engine parameter "multiplicity" must be a positive integer, not ${x.toString}""")
      }

      val destinations = objectNode.get("destinations") match {
        case null => Seq[Destination]()
        case arrayNode: ArrayNode => arrayNode.getElements.toSeq.map(readDestination)
        case x => throw new ConfigurationException(s"""jar-engine parameter "destinations" must be an array of queues or outputs, not ${x.toString}""")
      }

      JarEngine(url, className, inputType, outputType, multiplicity, destinations)
    }

    private def readShellEngine(objectNode: ObjectNode): ShellEngine = {
      val cmd = objectNode.get("cmd") match {
        case null => throw new ConfigurationException("""shell-engine parameter "cmd" is missing""")
        case arrayNode: ArrayNode => arrayNode.getElements.toSeq.map(_ match {
          case x: TextNode => x.getTextValue
          case x => throw new ConfigurationException(s"""shell-engine parameter "cmd" array must contain only strings, not ${x.toString}""")
        })
        case x => throw new ConfigurationException(s"""shell-engine parameter "cmd" must be a string or an array of strings, not ${x.toString}""")
      }

      val dir = objectNode.get("dir") match {
        case null => None
        case textNode: TextNode => Some(textNode.getTextValue)
        case x => throw new ConfigurationException(s"""shell-engine parameter "dir" must be a string, not ${x.toString}""")
      }

      val env = objectNode.get("env") match {
        case null => Map[String, String]()
        case x: ObjectNode => x.getFields map {y => (y.getKey, y.getValue match {
          case z: TextNode => z.getTextValue
          case z => throw new ConfigurationException(s"""shell-engine parameter "env" map must point only to strings, not ${z.toString}""")
        })} toMap
        case x => throw new ConfigurationException(s"""shell-engine parameter "env" must be a map to strings, not ${x.toString}""")
      }
        
      val inputType = readAvroType(objectNode, "shell-engine", "input")
      val outputType = readAvroType(objectNode, "shell-engine", "output")

      val multiplicity = objectNode.get("multiplicity") match {
        case null => 1
        case intNode: IntNode if (intNode.getIntValue > 0) => intNode.getIntValue
        case x => throw new ConfigurationException(s"""shell-engine parameter "multiplicity" must be a positive integer, not ${x.toString}""")
      }

      val destinations = objectNode.get("destinations") match {
        case null => Seq[Destination]()
        case arrayNode: ArrayNode => arrayNode.getElements.toSeq.map(readDestination)
        case x => throw new ConfigurationException(s"""shell-engine parameter "destinations" must be an array of queues or outputs, not ${x.toString}""")
      }

      ShellEngine(cmd, dir, env, inputType, outputType, multiplicity, destinations)
    }
  }

  // read in with YAML (by first converting it to JSON)
  object yamlToTopology extends Function1[String, Topology] {
    import org.yaml.snakeyaml.constructor.AbstractConstruct
    import org.yaml.snakeyaml.constructor.Construct
    import org.yaml.snakeyaml.constructor.SafeConstructor
    import org.yaml.snakeyaml.error.YAMLException
    import org.yaml.snakeyaml.nodes.MappingNode
    import org.yaml.snakeyaml.nodes.Node
    import org.yaml.snakeyaml.nodes.ScalarNode
    import org.yaml.snakeyaml.nodes.SequenceNode
    import org.yaml.snakeyaml.nodes.Tag
    import org.yaml.snakeyaml.Yaml

    import org.codehaus.jackson.map.ObjectMapper

    private val objectMapper = new ObjectMapper
    def convertToJson(intermediate: AnyRef): String = objectMapper.writeValueAsString(intermediate)

    private val safeConstructor = new SafeConstructor

    private def yaml: Yaml = new Yaml(new ConstructorForJsonConversion)

    def apply(in: java.io.InputStream): Topology = jsonToTopology(convertToJson(yaml.load(in)))
    def apply(in: java.io.Reader): Topology = jsonToTopology(convertToJson(yaml.load(in)))
    def apply(in: String): Topology = jsonToTopology(convertToJson(yaml.load(in)))

    def yamlToJson(in: java.io.InputStream): String = convertToJson(yaml.load(in))
    def yamlToJson(in: java.io.Reader): String = convertToJson(yaml.load(in))
    def yamlToJson(in: String): String = convertToJson(yaml.load(in))

    private class ConstructorForJsonConversion extends SafeConstructor {
      this.yamlConstructors.put(Tag.BINARY, new MyConstructYamlBinary)
      this.yamlConstructors.put(Tag.TIMESTAMP, new safeConstructor.ConstructYamlStr)
      this.yamlConstructors.put(Tag.OMAP, SafeConstructor.undefinedConstructor)
      this.yamlConstructors.put(Tag.PAIRS, SafeConstructor.undefinedConstructor)
      this.yamlConstructors.put(Tag.SET, SafeConstructor.undefinedConstructor)
      this.yamlConstructors.put(Tag.SEQ, new MyConstructYamlSeq)
      this.yamlConstructors.put(Tag.MAP, new MyConstructYamlMap)

      class NoPosConstructYaml extends AbstractConstruct {
        val constructYamlNull = new safeConstructor.ConstructYamlNull
        val constructYamlBool = new safeConstructor.ConstructYamlBool
        val constructYamlInt = new safeConstructor.ConstructYamlInt
        val constructYamlFloat = new safeConstructor.ConstructYamlFloat
        val constructYamlStr = new safeConstructor.ConstructYamlStr
        val constructYamlSeq = new safeConstructor.ConstructYamlSeq
        val constructYamlMap = new safeConstructor.ConstructYamlMap

        override def construct(node: Node): AnyRef = node match {
          case _: ScalarNode => node.getTag match {
            case Tag.NULL => constructYamlNull.construct(node)
            case Tag.BOOL => constructYamlBool.construct(node)
            case Tag.INT => constructYamlInt.construct(node)
            case Tag.FLOAT => constructYamlFloat.construct(node)
            case Tag.STR => constructYamlStr.construct(node)
            case _ => throw new YAMLException("Node uses features not allowed in JSON: " + node)
          }
          case _: MappingNode => constructYamlMap.construct(node)
          case _: SequenceNode => constructYamlSeq.construct(node)
          case _ => throw new YAMLException("Node uses features not allowed in JSON: " + node)
        }
      }

      class MyConstructYamlBinary extends safeConstructor.ConstructYamlBinary {
        override def construct(node: Node): AnyRef = new String(super.construct(node).asInstanceOf[Array[Byte]])
      }

      class MyConstructYamlMap extends Construct {
        override def construct(node: Node): AnyRef = {
          if (node.isTwoStepsConstruction)
            createDefaultMap
          else
            constructMapping(node.asInstanceOf[MappingNode])
        }
        override def construct2ndStep(node: Node, obj: AnyRef): Unit = {
          if (node.isTwoStepsConstruction)
            constructMapping2ndStep(node.asInstanceOf[MappingNode], obj.asInstanceOf[java.util.Map[AnyRef, AnyRef]])
          else
            throw new YAMLException("Unexpected recursive mapping structure. Node: " + node)
        }
      }

      class MyConstructYamlSeq extends Construct {
        override def construct(node: Node): AnyRef = {
          val seqNode = node.asInstanceOf[SequenceNode]
          if (node.isTwoStepsConstruction)
            createDefaultList(seqNode.getValue.size)
          else
            constructSequence(seqNode)
        }
        override def construct2ndStep(node: Node, data: AnyRef): Unit = {
          if (node.isTwoStepsConstruction)
            constructSequenceStep2(node.asInstanceOf[SequenceNode], data.asInstanceOf[java.util.List[AnyRef]])
          else
            throw new YAMLException("Unexpected recursive sequence structure. Node: " + node)
        }
      }
    }
  }
}
