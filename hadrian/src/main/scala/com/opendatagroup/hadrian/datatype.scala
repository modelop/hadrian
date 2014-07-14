package com.opendatagroup.hadrian

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.language.postfixOps

import org.apache.avro.Schema
import org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.SchemaParseException
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

import com.opendatagroup.hadrian.util.escapeJson
import com.opendatagroup.hadrian.util.uniqueEnumName
import com.opendatagroup.hadrian.util.uniqueFixedName
import com.opendatagroup.hadrian.util.uniqueRecordName

package datatype {
  ///////////////////////////////////////////////////////// the most general types

  trait Type {
    def accepts(that: Type): Boolean
    def avroType: AvroType = throw new IllegalArgumentException
  }

  case class FcnType(params: Seq[Type], ret: AvroType) extends Type {
    def accepts(that: Type): Boolean = that match {
      case FcnType(thatparams, thatret) =>
        thatparams.corresponds(params) { _.accepts(_) }  &&  ret.accepts(thatret)
      case _ => false
    }
    override def toString() = """{"type":"function","params":[%s],"ret":%s}""".format(params.mkString(","), ret.toString)
  }

  ///////////////////////////////////////////////////////// Avro types

  object AvroConversions {
    implicit def schemaToAvroType(schema: Schema): AvroType = schema.getType match {
      case Schema.Type.NULL => new AvroNull(schema)
      case Schema.Type.BOOLEAN => new AvroBoolean(schema)
      case Schema.Type.INT => new AvroInt(schema)
      case Schema.Type.LONG => new AvroLong(schema)
      case Schema.Type.FLOAT => new AvroFloat(schema)
      case Schema.Type.DOUBLE => new AvroDouble(schema)
      case Schema.Type.BYTES => new AvroBytes(schema)
      case Schema.Type.FIXED => new AvroFixed(schema)
      case Schema.Type.STRING => new AvroString(schema)
      case Schema.Type.ENUM => new AvroEnum(schema)
      case Schema.Type.ARRAY => new AvroArray(schema)
      case Schema.Type.MAP => new AvroMap(schema)
      case Schema.Type.RECORD => new AvroRecord(schema)
      case Schema.Type.UNION => new AvroUnion(schema)
    }
    implicit def schemaToAvroPlaceholder(schema: Schema): AvroPlaceholder = AvroFilledPlaceholder(schemaToAvroType(schema))
    implicit def avroTypeToSchema(avroType: AvroType): Schema = avroType.schema
    implicit def avroTypeToPlaceholder(avroType: AvroType): AvroPlaceholder = AvroFilledPlaceholder(avroType)
    implicit def fileToAvroType(x: java.io.File): AvroType = schemaToAvroType((new Schema.Parser).parse(x))
    implicit def inputStreamToAvroType(x: java.io.InputStream): AvroType = schemaToAvroType((new Schema.Parser).parse(x))
    implicit def stringToAvroType(x: String): AvroType = schemaToAvroType((new Schema.Parser).parse(x))
  }

  abstract class AvroType extends Type {
    def schema: Schema
    override def equals(that: Any): Boolean = that match {
      case x: AvroType => schema == x.schema
      case _ => false
    }

    private def qualify(namespace: Option[String], alias: String): String =
      if (alias.contains(".")  ||  namespace == None)
        alias
      else
        namespace.get + "." + alias

    // this == "reader" (the anticipated signature, pattern to be matched),
    // that == "writer" (the given fact, argument to be accepted or rejected)
    def accepts(that: Type): Boolean = that match {
      case avroThat: AvroType =>
        checkReaderWriterCompatibility(this.schema, avroThat.schema).getType == SchemaCompatibilityType.COMPATIBLE
      case _ => false
    }
    override def avroType: AvroType = this

    override def toString(): String = schema.toString
  }

  trait AvroCompiled extends AvroType {
    def name: String
    def namespace: Option[String]
    def fullName: String
    def aliases: Set[String]
    def doc: String
  }
  trait AvroNumber extends AvroType
  trait AvroRaw extends AvroType
  trait AvroIdentifier extends AvroType
  trait AvroContainer extends AvroType
  trait AvroMapping extends AvroType
  object AvroNumber {
    def unapply(x: AvroNumber): Boolean = true
  }
  object AvroRaw {
    def unapply(x: AvroRaw): Boolean = true
  }
  object AvroIdentifier {
    def unapply(x: AvroIdentifier): Boolean = true
  }

  ///////////////////////////////////////////////////////// Avro type wrappers

  // start classes

  class AvroNull(val schema: Schema) extends AvroType
  class AvroBoolean(val schema: Schema) extends AvroType
  class AvroInt(val schema: Schema) extends AvroType with AvroNumber
  class AvroLong(val schema: Schema) extends AvroType with AvroNumber
  class AvroFloat(val schema: Schema) extends AvroType with AvroNumber
  class AvroDouble(val schema: Schema) extends AvroType with AvroNumber
  class AvroBytes(val schema: Schema) extends AvroType with AvroRaw

  class AvroFixed(val schema: Schema) extends AvroType with AvroRaw with AvroCompiled {
    def size: Int = schema.getFixedSize
    override def name: String = schema.getName
    override def namespace: Option[String] = schema.getNamespace match {case null => None; case x => Some(x)}
    override def fullName: String = schema.getFullName
    override def aliases: Set[String] = schema.getAliases.toSet
    override def doc: String = schema.getDoc
  }

  class AvroString(val schema: Schema) extends AvroType with AvroIdentifier

  class AvroEnum(val schema: Schema) extends AvroType with AvroIdentifier with AvroCompiled {
    def symbols: Seq[String] = schema.getEnumSymbols.toVector
    override def name: String = schema.getName
    override def namespace: Option[String] = schema.getNamespace match {case null => None; case x => Some(x)}
    override def fullName: String = schema.getFullName
    override def aliases: Set[String] = schema.getAliases.toSet
    override def doc: String = schema.getDoc
  }

  class AvroArray(val schema: Schema) extends AvroType with AvroContainer {
    def items: AvroType = AvroConversions.schemaToAvroType(schema.getElementType)
  }

  class AvroMap(val schema: Schema) extends AvroType with AvroContainer with AvroMapping {
    def values: AvroType = AvroConversions.schemaToAvroType(schema.getValueType)
  }

  class AvroRecord(val schema: Schema) extends AvroType with AvroContainer with AvroMapping with AvroCompiled {
    def fields: Seq[AvroField] = schema.getFields map { x => new AvroField(x) }
    def fieldOption(name: String): Option[AvroField] = schema.getFields find { _.name == name } map { new AvroField(_) }
    def field(name: String): AvroField = fieldOption(name).get
    override def name: String = schema.getName
    override def namespace: Option[String] = schema.getNamespace match {case null => None; case x => Some(x)}
    override def fullName: String = schema.getFullName
    override def aliases: Set[String] = schema.getAliases.toSet
    override def doc: String = schema.getDoc
  }

  class AvroField(val schemaField: Schema.Field) {
    override def toString(): String = {
      val showDefault =
        if (schemaField.defaultValue == null)
          ""
        else
          ""","default":""" + schemaField.defaultValue.toString
      val showOrder =
        if (order == Schema.Field.Order.ASCENDING)
          ""
        else
          ""","order":"%s"""".format(order.toString.toLowerCase)
      val showAliases =
        if (aliases.size == 0)
          ""
        else
          ""","aliases":[%s]""".format(schemaField.aliases map { x => "\"" + escapeJson(x.toString) + "\"" } mkString(","))
      """{"name":"%s","type":%s,"doc":"%s"%s%s%s}""".format(
        escapeJson(schemaField.name),
        schemaField.schema.toString,
        escapeJson(schemaField.doc),
        showDefault,
        showOrder,
        showAliases)
    }
    def name: String = schemaField.name
    def avroType: AvroType = AvroConversions.schemaToAvroType(schemaField.schema)
    def default: Option[JsonNode] =
      if (schemaField.defaultValue == null)
        None
      else
        Some(schemaField.defaultValue)
    def order: Schema.Field.Order = schemaField.order
    def aliases: Set[String] = schemaField.aliases.toSet
    def doc: String = schemaField.doc
  }

  class AvroUnion(val schema: Schema) extends AvroType {
    def types: Seq[AvroType] = schema.getTypes.map(AvroConversions.schemaToAvroType(_)).toList
  }

  // start objects

  object AvroNull {
    def apply(): AvroNull = new AvroNull(Schema.create(Schema.Type.NULL))
    def unapply(x: AvroNull): Boolean = true
  }

  object AvroBoolean {
    def apply(): AvroBoolean = new AvroBoolean(Schema.create(Schema.Type.BOOLEAN))
    def unapply(x: AvroBoolean): Boolean = true
  }

  object AvroInt {
    def apply(): AvroInt = new AvroInt(Schema.create(Schema.Type.INT))
    def unapply(x: AvroInt): Boolean = true
  }

  object AvroLong {
    def apply(): AvroLong = new AvroLong(Schema.create(Schema.Type.LONG))
    def unapply(x: AvroLong): Boolean = true
  }

  object AvroFloat {
    def apply(): AvroFloat = new AvroFloat(Schema.create(Schema.Type.FLOAT))
    def unapply(x: AvroFloat): Boolean = true
  }

  object AvroDouble {
    def apply(): AvroDouble = new AvroDouble(Schema.create(Schema.Type.DOUBLE))
    def unapply(x: AvroDouble): Boolean = true
  }

  object AvroBytes {
    def apply(): AvroBytes = new AvroBytes(Schema.create(Schema.Type.BYTES))
    def unapply(x: AvroBytes): Boolean = true
  }

  object AvroFixed {
    def apply(size: Int, name: String = uniqueFixedName(), namespace: Option[String] = None, aliases: Set[String] = Set[String](), doc: String = ""): AvroFixed = {
      val schema = Schema.createFixed(name, doc, namespace match {case Some(x) => x; case None => null}, size)
      for (alias <- aliases)
        schema.addAlias(alias)
      new AvroFixed(schema)
    }
    def unapply(x: AvroFixed): Option[(Int, String, Option[String], Set[String], String)] =
      Some((x.size, x.name, x.namespace, x.aliases, x.doc))
  }

  object AvroString {
    def apply(): AvroString = new AvroString(Schema.create(Schema.Type.STRING))
    def unapply(x: AvroString): Boolean = true
  }

  object AvroEnum {
    def apply(symbols: Seq[String], name: String = uniqueEnumName(), namespace: Option[String] = None, aliases: Set[String] = Set[String](), doc: String = ""): AvroEnum = {
      val schema = Schema.createEnum(name, doc, namespace match {case Some(x) => x; case None => null}, symbols)
      for (alias <- aliases)
        schema.addAlias(alias)
      new AvroEnum(schema)
    }
    def unapply(x: AvroEnum): Option[(List[String], String, Option[String], Set[String], String)] =
      Some((x.symbols.toList, x.name, x.namespace, x.aliases, x.doc))
  }

  object AvroArray {
    def apply(items: AvroType): AvroArray = new AvroArray(Schema.createArray(items.schema))
    def unapply(x: AvroArray): Option[AvroType] = Some(x.items)
  }

  object AvroMap {
    def apply(values: AvroType): AvroMap = new AvroMap(Schema.createMap(values.schema))
    def unapply(x: AvroMap): Option[AvroType] = Some(x.values)
  }

  object AvroRecord {
    def apply(fields: Seq[AvroField], name: String = uniqueRecordName(), namespace: Option[String] = None, aliases: Set[String] = Set[String](), doc: String = ""): AvroRecord = {
      val schema = Schema.createRecord(name, doc, namespace match {case Some(x) => x; case None => null}, false)
      schema.setFields(fields map { _.schemaField })
      for (alias <- aliases)
        schema.addAlias(alias)
      new AvroRecord(schema)
    }
    def unapply(x: AvroRecord): Option[(List[AvroField], String, Option[String], Set[String], String)] =
      Some((x.fields.toList, x.name, x.namespace, x.aliases, x.doc))
  }

  object AvroField {
    def apply(name: String, avroType: AvroType, default: Option[JsonNode] = None, order: Schema.Field.Order = Schema.Field.Order.ASCENDING, aliases: Set[String] = Set[String](), doc: String = ""): AvroField = {
      val schemaField = new Schema.Field(name, avroType.schema, doc, default match {case Some(x) => x; case None => null}, order)
      for (alias <- aliases)
        schemaField.addAlias(alias)
      new AvroField(schemaField)
    }
    def unapply(x: AvroField): Option[(String, AvroType, Option[JsonNode], Schema.Field.Order, Set[String], String)] =
      Some((x.name, x.avroType, x.default, x.order, x.aliases, x.doc))
  }

  object AvroUnion {
    def apply(types: Seq[AvroType]): AvroUnion = new AvroUnion(Schema.createUnion(types map { _.schema }))
    def unapply(x: AvroUnion): Option[Seq[AvroType]] = Some(x.types)
  }

  /////////////////////////// resolving types out of order in streaming input

  class AvroPlaceholder(original: String, forwardDeclarationParser: ForwardDeclarationParser) {
    override def equals(other: Any): Boolean = other match {
      case that: AvroPlaceholder => this.avroType == that.avroType
      case that: AvroType => this.avroTypeOption == Some(that)
      case _ => false
    }
    def avroType: AvroType = forwardDeclarationParser.lookup(original)
    def avroTypeOption: Option[AvroType] = forwardDeclarationParser.lookupOption(original)
    override def toString(): String = forwardDeclarationParser.lookupOption(original) match {
      case Some(x) => x.toString
      case None => """{"type":"unknown"}"""
    }
    def parser = forwardDeclarationParser
  }
  object AvroPlaceholder {
    def apply(original: String, forwardDeclarationParser: ForwardDeclarationParser): AvroPlaceholder = new AvroPlaceholder(original, forwardDeclarationParser)
    def unapply(avroPlaceholder: AvroPlaceholder): Option[AvroType] = avroPlaceholder.avroTypeOption
  }

  case class AvroFilledPlaceholder(override val avroType: AvroType) extends AvroPlaceholder("", null) {
    override def avroTypeOption: Option[AvroType] = Some(avroType)
    override def toString(): String = avroType.toString
  }

  class ForwardDeclarationParser {
    private var types: java.util.Map[String, Schema] = null
    private var lookupTable = Map[String, AvroType]()

    def lookup(original: String): AvroType = lookupTable(original)
    def lookupOption(original: String): Option[AvroType] = lookupTable.get(original)
    def compiledTypes: Set[AvroCompiled] = types.collect({case (n, s) if (s.getType == Schema.Type.FIXED  ||  s.getType == Schema.Type.RECORD  ||  s.getType == Schema.Type.ENUM) =>
        AvroConversions.schemaToAvroType(s).asInstanceOf[AvroCompiled]}).toSet

    def parse(jsons: Seq[String]): Map[String, AvroType] = {
      var schemae = Map[String, Schema]()
      var unresolvedSize = -1
      var lastUnresolvedSize = -1

      def findFullyParsedTypes(schemae: Seq[Schema]): Set[String] = schemae.flatMap({schema: Schema =>
        schema.getType match {
          case Schema.Type.ARRAY => findFullyParsedTypes(List(schema.getElementType))
          case Schema.Type.MAP => findFullyParsedTypes(List(schema.getValueType))
          case Schema.Type.RECORD => List(schema.getFullName) ++ findFullyParsedTypes(schema.getFields.map(_.schema))
          case Schema.Type.ENUM | Schema.Type.FIXED => List(schema.getFullName)
          case _ => List[String]()
        }
      }).toSet

      val errorMessages = mutable.Map[String, String]()

      // This algorithm is O(N^2) in the number of types, but there shouldn't be too many types...
      do {
        for (json <- jsons if (!schemae.contains(json))) {
          val parser = new Schema.Parser
          if (types != null)
            parser.addTypes(types)

          try {
            schemae = schemae + (json -> parser.parse(json))
            types = parser.getTypes
          }
          catch {
            case err: SchemaParseException if (lastUnresolvedSize > -1  &&  err.getMessage.contains("Can't redefine:")) =>  // this keeps a "Can't redefine" error message from shadowing the true user error
            case err: SchemaParseException => errorMessages(json) = err.getMessage
            case err: java.lang.NumberFormatException => errorMessages(json) = "NumberFormatException: " + err.getMessage
          }
        }

        val unresolved = jsons.toSet.diff(schemae.keys.toSet)
        unresolvedSize = unresolved.size

        if (unresolvedSize == lastUnresolvedSize)
          throw new SchemaParseException("Could not resolve the following types:\n    " + (unresolved map {json => "%s (%s)".format(json, errorMessages(json))} mkString("\n    ")))
        lastUnresolvedSize = unresolvedSize
      } while (unresolvedSize > 0)

      val result = schemae map { case(k, v) => (k, AvroConversions.schemaToAvroType(v)) }
      lookupTable = lookupTable ++ result
      result
    }

    private val objectMapper = new ObjectMapper
    private val MatchQuoted = """\s*"(.*)"\s*""".r
    private val MatchSimple = """\s*\{\s*"type"\s*:\s*("[^"]*")\s*\}\s*""".r
    private val MatchArray = """\s*\{\s*("type"\s*:\s*"array"\s*,\s*"items"\s*:(.*)|"items"\s*:(.*),\s*"type"\s*:\s*"array")\s*\}\s*""".r
    private val MatchMap = """\s*\{\s*("type"\s*:\s*"map"\s*,\s*"values"\s*:(.*)|"values"\s*:(.*),\s*"type"\s*:\s*"map")\s*\}\s*""".r
    private val MatchBracket = """\s*\[(.*)\]\s*""".r

    def getSchema(description: String): Option[Schema] =
      if (types == null)
        None
      else
        description match {
          case MatchQuoted(x) => getSchema(x)
          case MatchSimple(x) => getSchema(x)
          case MatchArray(x, y, null) => getSchema(y) match {
            case Some(z) => Some(Schema.createArray(z))
            case None => None
          }
          case MatchArray(x, null, y) => getSchema(y) match {
            case Some(z) => Some(Schema.createArray(z))
            case None => None
          }
          case MatchMap(x, y, null) => getSchema(y) match {
            case Some(z) => Some(Schema.createMap(z))
            case None => None
          }
          case MatchMap(x, null, y) => getSchema(y) match {
            case Some(z) => Some(Schema.createMap(z))
            case None => None
          }
          case MatchBracket(_) => {
            val parsed = objectMapper.readValue(description, classOf[JsonNode])
            val schemaOptions = parsed.iterator.map(x => getSchema(x.toString))
            if (schemaOptions.contains(None))
              None
            else
              Some(Schema.createUnion(seqAsJavaList(schemaOptions.flatten.toList)))
          }
          case "null" => Some(Schema.create(Schema.Type.NULL))
          case "boolean" => Some(Schema.create(Schema.Type.BOOLEAN))
          case "int" => Some(Schema.create(Schema.Type.INT))
          case "long" => Some(Schema.create(Schema.Type.LONG))
          case "float" => Some(Schema.create(Schema.Type.FLOAT))
          case "double" => Some(Schema.create(Schema.Type.DOUBLE))
          case "bytes" => Some(Schema.create(Schema.Type.BYTES))
          case "string" => Some(Schema.create(Schema.Type.STRING))
          case x => types.get(description) match {
            case null => None
            case x => Some(x)
          }
        }

    def getAvroType(description: String): Option[AvroType] =
      getSchema(description).map(AvroConversions.schemaToAvroType(_))
  }

  class AvroTypeBuilder {
    val forwardDeclarationParser = new ForwardDeclarationParser
    private var originals: List[String] = Nil

    def makePlaceholder(avroJsonString: String): AvroPlaceholder = {
      originals = avroJsonString :: originals
      AvroPlaceholder(avroJsonString, forwardDeclarationParser)
    }

    def resolveTypes(): Unit = {
      forwardDeclarationParser.parse(originals)
      originals = Nil
    }
  }

}
