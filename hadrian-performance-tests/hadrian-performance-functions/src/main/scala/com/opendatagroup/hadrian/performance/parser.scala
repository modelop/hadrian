package com.opendatagroup.hadrian.performance

import scala.collection.JavaConversions._
import scala.language.postfixOps

import org.codehaus.jackson.io.JsonStringEncoder
import org.codehaus.jackson.JsonFactory
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.JsonParser
import org.codehaus.jackson.JsonToken
import org.codehaus.jackson.map.ObjectMapper
import org.codehaus.jackson.node.ArrayNode
import org.codehaus.jackson.node.BooleanNode
import org.codehaus.jackson.node.NullNode
import org.codehaus.jackson.node.NumericNode
import org.codehaus.jackson.node.ObjectNode
import org.codehaus.jackson.node.TextNode

import com.opendatagroup.hadrian.jvmcompiler.PFAEngine
import com.opendatagroup.hadrian.datatype._
import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAMap

package functions {
  trait Sample {
    def input: AnyRef
  }
  case class SampleSuccess(input: AnyRef, jsonInput: String, value: String) extends Sample
  case class SampleFailure(input: AnyRef, jsonInput: String, code: Int) extends Sample
  case class TestFunction(function: String, engine: PFAEngine[AnyRef, AnyRef], samples: Vector[Sample])

  class PFATestsReader(file: java.io.File) extends Iterator[TestFunction] {
    val jsonFactory = new JsonFactory
    val objectMapper = new ObjectMapper
    jsonFactory.setCodec(objectMapper)
    val parser = jsonFactory.createJsonParser(file)

    val header = {
      parser.nextToken() match {
        case JsonToken.START_OBJECT =>
          var out = Map[String, String]()
          parser.nextToken()
          while (parser.getCurrentName != "pfa-tests") {
            out = out.updated(parser.getCurrentName, readString(parser, parser.nextToken()))
            parser.nextToken()
          }
          out
        case _ => throw new Exception()
      }
    }

    parser.nextToken() match {
      case JsonToken.START_ARRAY =>
      case _ => throw new Exception()
    }

    private var _next: Option[TestFunction] = readFunction(parser, parser.nextToken())
    def hasNext = _next != None
    def next() = {
      val out = _next.get
      _next = readFunction(parser, parser.nextToken())
      out
    }

    def readString(parser: JsonParser, token: JsonToken): String = token match {
      case JsonToken.VALUE_STRING => parser.getText
      case _ => throw new Exception()
    }

    def escapeJson(x: String): String = new String(JsonStringEncoder.getInstance.quoteAsString(x))

    def ingestJsonAsBytes(parser: JsonParser, token: JsonToken, baos: java.io.ByteArrayOutputStream): Unit = token match {
      case JsonToken.VALUE_NULL => baos.write("null".getBytes, 0, 4)
      case JsonToken.VALUE_TRUE => baos.write("true".getBytes, 0, 4)
      case JsonToken.VALUE_FALSE => baos.write("false".getBytes, 0, 5)
      case JsonToken.VALUE_NUMBER_INT =>
        val x = parser.getText.getBytes
        baos.write(x, 0, x.size)
      case JsonToken.VALUE_NUMBER_FLOAT =>
        val x = parser.getText.getBytes
        baos.write(x, 0, x.size)
      case JsonToken.VALUE_STRING =>
        val x = ("\"" + escapeJson(parser.getText) + "\"").getBytes
        baos.write(x, 0, x.size)
      case JsonToken.START_ARRAY => {
        baos.write("[".getBytes, 0, 1)
        var first = true
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_ARRAY) {
          if (first)
            first = false
          else
            baos.write(",".getBytes, 0, 1)
          ingestJsonAsBytes(parser, subtoken, baos)
          subtoken = parser.nextToken()
        }
        baos.write("]".getBytes, 0, 1)
      }
      case JsonToken.START_OBJECT => {
        baos.write("{".getBytes, 0, 1)
        var first = true
        var subtoken = parser.nextToken()
        while (subtoken != JsonToken.END_OBJECT) {
          if (first)
            first = false
          else
            baos.write(",".getBytes, 0, 1)
          val key = ("\"" + escapeJson(parser.getCurrentName) + "\":").getBytes
          baos.write(key, 0, key.size)
          ingestJsonAsBytes(parser, parser.nextToken(), baos)
          subtoken = parser.nextToken()
        }
        baos.write("}".getBytes, 0, 1)
      }
      case _ => throw new Exception()
    }

    def readJsonToString(parser: JsonParser, token: JsonToken): String = {
      val baos = new java.io.ByteArrayOutputStream
      ingestJsonAsBytes(parser, token, baos)
      new String(baos.toByteArray)
    }

    def jsonStringToNode(jsonString: String): JsonNode =
      jsonFactory.createJsonParser(jsonString).readValueAsTree

    def preprocess(avroType: AvroType, jsonNode: JsonNode, classLoader: java.lang.ClassLoader): AnyRef = (avroType, jsonNode) match {
      case (AvroNull(), _: NullNode) => null
      case (AvroBoolean(), x: BooleanNode) => java.lang.Boolean.valueOf(x.asBoolean)
      case (AvroInt(), x: NumericNode) => java.lang.Integer.valueOf(x.asInt)
      case (AvroLong(), x: NumericNode) => java.lang.Long.valueOf(x.asLong)
      case (AvroFloat(), x: NumericNode) => java.lang.Float.valueOf(x.asDouble.toFloat)
      case (AvroFloat(), x: TextNode) if (x.getTextValue == "nan") => java.lang.Float.valueOf(java.lang.Float.NaN)
      case (AvroFloat(), x: TextNode) if (x.getTextValue == "inf") => java.lang.Float.valueOf(java.lang.Float.POSITIVE_INFINITY)
      case (AvroFloat(), x: TextNode) if (x.getTextValue == "-inf") => java.lang.Float.valueOf(java.lang.Float.NEGATIVE_INFINITY)
      case (AvroDouble(), x: NumericNode) => java.lang.Double.valueOf(x.asDouble)
      case (AvroDouble(), x: TextNode) if (x.getTextValue == "nan") => java.lang.Double.valueOf(java.lang.Double.NaN)
      case (AvroDouble(), x: TextNode) if (x.getTextValue == "inf") => java.lang.Double.valueOf(java.lang.Double.POSITIVE_INFINITY)
      case (AvroDouble(), x: TextNode) if (x.getTextValue == "-inf") => java.lang.Double.valueOf(java.lang.Double.NEGATIVE_INFINITY)
      case (AvroBytes(), x: TextNode) => java.util.Base64.getDecoder.decode(x.getTextValue)
      case (t: AvroFixed, x: TextNode) =>
        val constructor = classLoader.loadClass(t.fullName).getConstructor(classOf[Array[Byte]])
        constructor.newInstance(java.util.Base64.getDecoder.decode(x.getTextValue)).asInstanceOf[AnyRef]
      case (AvroString(), x: TextNode) => x.getTextValue
      case (t: AvroEnum, x: TextNode) =>
        val constructor = classLoader.loadClass(t.fullName).getConstructor(classOf[org.apache.avro.Schema], classOf[String])
        constructor.newInstance(t.schema, x.getTextValue).asInstanceOf[AnyRef]
      case (AvroArray(items), x: ArrayNode) =>
        val out = PFAArray.empty[AnyRef](x.size)
        0 until x.size foreach {i => out.add(preprocess(items, x.get(i), classLoader))}
        out
      case (AvroMap(values), x: ObjectNode) =>
        val out = PFAMap.empty[AnyRef](x.size)
        x.getFieldNames foreach {k => out.put(k, preprocess(values, x.get(k), classLoader))}
        out
      case (t: AvroRecord, x: ObjectNode) =>
        val constructor = classLoader.loadClass(t.fullName).getConstructor(classOf[Array[AnyRef]])
        val convertedFields = t.fields.map(f => preprocess(f.avroType, x.get(f.name), classLoader)).toArray
        constructor.newInstance(convertedFields).asInstanceOf[AnyRef]
      case (AvroUnion(types), _: NullNode) => null
      case (AvroUnion(types), x: ObjectNode) =>
        val tag = x.getFieldNames.toList.head
        val value = x.get(tag)
        types.find(_.fullName == tag) match {
          case Some(t) => preprocess(t, value, classLoader)
          case None => throw new Exception()
        }
      case (t, x) => throw new Exception(s"t $t x $x")
    }

    def readFunction(parser: JsonParser, token: JsonToken): Option[TestFunction] = token match {
      case JsonToken.START_OBJECT =>
        // assume that the first thing in a function is "function": FUNCTION_NAME
        parser.nextToken()
        val function = parser.getCurrentName match {
          case "function" => readString(parser, parser.nextToken())
          case _ => throw new Exception()
        }

        // assume that the second thing in a function is "engine": ENGINE_PFA
        parser.nextToken()
        val engineJson = parser.getCurrentName match {
          case "engine" => readJsonToString(parser, parser.nextToken())
          case _ => throw new Exception()
        }
        val engine = PFAEngine.fromJson(engineJson).head
        val inputType = engine.inputType

        // sampleInputs and sampleResults are built in lock-step
        val samplesBuilder = Vector.newBuilder[Sample]

        // assume that the third thing in a funciton is "trials": [TRIAL1, TRIAL2, ...]
        parser.nextToken()
        parser.getCurrentName match {
          case "trials" =>
            parser.nextToken() match {
              // assume that the trials are in an array
              case JsonToken.START_ARRAY =>
                var subtoken = parser.nextToken()
                while (subtoken != JsonToken.END_ARRAY) subtoken match {
                  case JsonToken.START_OBJECT =>
                    // assume that the first thing in a trial is "sample": SAMPLE_VALUE
                    parser.nextToken()
                    val (sampleInput, jsonInput) = parser.getCurrentName match {
                      case "sample" =>
                        val jsonInput = readJsonToString(parser, parser.nextToken())
                        (preprocess(inputType, jsonStringToNode(jsonInput), engine.classLoader), jsonInput)
                      case _ => throw new Exception()
                    }

                    // assume that the second thing in a trial is "result": RESULT_VALUE or "error": ERROR_INTEGER
                    parser.nextToken()
                    val sample = parser.getCurrentName match {
                      case "result" => SampleSuccess(sampleInput, jsonInput, readJsonToString(parser, parser.nextToken()))
                      case "error" => SampleFailure(sampleInput, jsonInput, readJsonToString(parser, parser.nextToken()).toInt)
                      case "nondeterministic" => parser.nextToken(); SampleSuccess(sampleInput, jsonInput, "?")
                      case _ => throw new Exception()
                    }

                    samplesBuilder += sample

                    // assume that there may or may not be additional key-value pairs (zero or one "nondeterministic" key)
                    var subsubtoken = parser.nextToken()
                    while (subsubtoken != JsonToken.END_OBJECT) {
                      jsonStringToNode(readJsonToString(parser, parser.nextToken()))
                      subsubtoken = parser.nextToken()
                    }

                    // advance to the next trial in the trials array
                    subtoken = parser.nextToken()

                  case _ => throw new Exception()
                }
              case _ => throw new Exception()
            }
          case _ => throw new Exception()
        }

        parser.nextToken()

        val samples = {
          val out = samplesBuilder.result
          // make sure simple functions don't get under-sampled
          if (out.size < 10)
            (0 until 10).flatMap(x => out).toVector
          else
            out
        }

        Some(TestFunction(function, engine, Vector.fill(Main.warmupSamples)(samples.head) ++ samples))

      case JsonToken.END_ARRAY =>
        None

      case _ => throw new Exception()
    }
  }
}
