package com.opendatagroup.hadrian.gae

import javax.servlet.http.HttpServlet
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.IntNode
import org.codehaus.jackson.node.ObjectNode
import org.codehaus.jackson.node.TextNode

import org.apache.avro.AvroTypeException
import org.apache.avro.SchemaParseException
import org.codehaus.jackson.JsonParseException
import org.yaml.snakeyaml.error.YAMLException

import com.opendatagroup.hadrian.ast._
import com.opendatagroup.hadrian.errors._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.reader._
import com.opendatagroup.hadrian.yaml._

class Run extends HttpServlet {
  private val objectMapper = new ObjectMapper
  private val datasets = mutable.Map[String, Dataset]()

  val options = Map("timeout" -> new IntNode(1000))

  private val gaeClassLoader = {
    val tmpMap = new java.util.HashMap[AnyRef, AnyRef]()
    tmpMap.put("", "")
    val entry = tmpMap.entrySet.iterator.next()
    val hashMapEntry = entry.getClass

    val parentClassLoader = Thread.currentThread.getContextClassLoader

    new java.lang.ClassLoader(getClass.getClassLoader) {
      override def loadClass(className: String): java.lang.Class[_] = findClass(className)
      override def findClass(className: String): java.lang.Class[_] = {
        if (className == "java.util.HashMap$Entry")
          hashMapEntry
        else
          parentClassLoader.loadClass(className)
      }
    }
  }

  private def compilerError(writer: java.io.PrintWriter, err: Throwable): Unit = {
    writer.println("COMPILER-ERROR")
    writer.println(err.getClass.getName.split("\\.").last)
    writer.println(err.getMessage)
  }

  private def runtimeError(writer: java.io.PrintWriter, err: PFAException): Unit = {
    writer.println("[ERROR] %s: %s".format(err.getClass.getName.split("\\.").last, err.getMessage))
  }

  override def doOptions(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    super.doOptions(request, response)
    response.setHeader("Access-Control-Allow-Origin", "*")
    response.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept")
    response.setHeader("Access-Control-Allow-Methods", "POST")
  }

  override def doPost(request: HttpServletRequest, response: HttpServletResponse): Unit = {
    response.setHeader("Access-Control-Allow-Origin", "*")
    response.setContentType("text/plain")

    val inputStream = request.getInputStream
    val writer = response.getWriter

    val allinput = try {
      objectMapper.readTree(inputStream)
    }
    catch {
      case err: JsonParseException => compilerError(writer, err); null
    }

    val dataset: String =
      if (allinput != null) {
        allinput.findValue("dataset") match {
          case null => null
          case x: TextNode => x.getTextValue
          case _ => null
        }
      }
      else
        null

    val data: String =
      if (allinput != null) {
        allinput.findValue("data") match {
          case null => null
          case x: TextNode => x.getTextValue
          case _ => null
        }
      }
      else
        null

    if (dataset == null  &&  data == null)
      compilerError(writer, new Exception("'data' or 'dataset' field missing from request"))

    val format: String =
      if (allinput != null) {
        val out = allinput.findValue("format") match {
          case null => null
          case x: TextNode => x.getTextValue
          case _ => null
        }
        if (out == null)
          compilerError(writer, new Exception("'format' field missing from request"))
        out
      }
      else
        null

    val document: String =
      if (allinput != null) {
        val out = allinput.findValue("document") match {
          case null => null
          case x: ObjectNode => x.toString
          case x: TextNode => x.getTextValue
          case _ => null
        }
        if (out == null)
          compilerError(writer, new Exception("'document' field missing from request"))
        out
      }
      else
        null

    val ast = 
      if (format != null  &&  document != null) {
        try {
          format match {
            case "json" => jsonToAst(document)
            case "yaml" => yamlToAst(document)
            case _ => compilerError(writer, new Exception("'document' field missing from request")); null
          }
        }
        catch {
          case err: NotImplementedError => compilerError(writer, err); null
          case err: JsonParseException => compilerError(writer, err); null
          case err: YAMLException => compilerError(writer, err); null
          case err: SchemaParseException => compilerError(writer, err); null
          case err: PFAException => compilerError(writer, err); null
          case err: java.io.EOFException => compilerError(writer, err); null
        }
      }
      else
        null

    val engine =
      if (ast != null) {
        try {
          PFAEngine.fromAst(ast, options, parentClassLoader = Some(gaeClassLoader)).head
        }
        catch {
          case err: NotImplementedError => compilerError(writer, err); null
          case err: PFAException => compilerError(writer, err); null
          case err: java.lang.ClassNotFoundException => compilerError(writer, err); null
        }
      }
      else
        null

    if (engine != null  &&  (data != null  ||  dataset != null)) {
      val inputIterator =
        if (dataset != null) {
          val ds =
            (dataset, datasets.get(dataset)) match {
              case (_, Some(x)) => x
              case ("exoplanets", None) => {
                val out = new ExoplanetDataset(getServletContext)
                datasets(dataset) = out
                out
              }
              case _ => {
                compilerError(writer, new IllegalArgumentException("unrecognized dataset name \"%s\"".format(dataset)))
                null
              }
            }

          if (ds != null) {
            if (!engine.inputType.accepts(ds.avroType)) {
              compilerError(writer, new IllegalArgumentException("scoring engine's input type does not match data type"))
              null
            }
            else
              engine.avroInputIterator[AnyRef](ds.inputStream)
          }
          else
            null
        }
        else {
          val inputStream = new java.io.ByteArrayInputStream(data.getBytes())
          engine.jsonInputIterator[AnyRef](inputStream)
        }

      if (inputIterator != null) {
        engine.log = {(message: String, namespace: Option[String]) =>
          val ns =
            namespace match {
              case Some(x) => " " + x
              case None => ""
            }
          writer.println("[LOG%s] %s".format(ns, message))
        }

        engine match {
          case x: PFAEmitEngine[_, _] => {
            x.emit = {result: AnyRef => writer.println(engine.toJson(result, engine.outputType))}
          }
          case _ =>
        }

        val beginWorked =
          try {
            engine.begin(); true
          }
          catch {
            case err: PFAException => {
              runtimeError(writer, err)
              writer.println("Halting due to error in begin routine")
              false
            }
          }

        if (beginWorked) {
          var dataLine = 0
          while (inputIterator.hasNext) {
            dataLine += 1
            try {
              val datum = inputIterator.next()
              try {
                val result = engine.action(datum)
                if (engine.config.method != Method.EMIT)
                  writer.println(engine.toJson(result, engine.outputType))
              }
              catch {
                case err: PFAException => runtimeError(writer, err)
              }
            }
            catch {
              case err: JsonParseException => writer.println("Data line %d is not valid JSON".format(dataLine))
              case err: AvroTypeException => writer.println("Data line %d has the wrong Avro type".format(dataLine))
              case err: java.io.EOFException => writer.println("Data line %d is empty".format(dataLine))
            }
          }
        }

        try {
          engine.end()
        }
        catch {
          case err: PFAException => runtimeError(writer, err)
        }
      }
    }
  }
}
