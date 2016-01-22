// Copyright (C) 2014  Open Data ("Open Data" refers to
// one or more of the following companies: Open Data Partners LLC,
// Open Data Research LLC, or Open Data Capital LLC.)
// 
// This file is part of Hadrian.
// 
// Licensed under the Hadrian Personal Use and Evaluation License (PUEL);
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://raw.githubusercontent.com/opendatagroup/hadrian/master/LICENSE
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.opendatagroup.hadrian

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.language.implicitConversions
import scala.language.postfixOps

import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.node.IntNode
import org.codehaus.jackson.node.JsonNodeFactory
import org.codehaus.jackson.node.TextNode

import org.apache.avro.Schema
import org.apache.avro.SchemaParseException
import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.map.ObjectMapper

// import org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility
// import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
// def accepts(that: Type): Boolean = that match {
//   case exceptionThat: ExceptionType => false
//   case avroThat: AvroType =>
//     checkReaderWriterCompatibility(this.schema, avroThat.schema).getType == SchemaCompatibilityType.COMPATIBLE
//   case _ => false
// }

import com.opendatagroup.hadrian.util.convertToJson
import com.opendatagroup.hadrian.util.escapeJson
import com.opendatagroup.hadrian.util.uniqueEnumName
import com.opendatagroup.hadrian.util.uniqueFixedName
import com.opendatagroup.hadrian.util.uniqueRecordName

package datatype {
  ///////////////////////////////////////////////////////// the most general types

  /** Superclass of all Avro types and also inline functions, which can only appear in argument lists.
    */
  trait Type {
    /** Perform type resolution: would PFA objects of type `that` be accepted where a PFA object of type `this` is expected?
      * 
      * If `x.accepts(y) && y.accepts(x)`, then `x` and `y` are equal. In general, acceptability is not symmetric.
      * 
      * @param that the given argument to be accepted or rejected
      * @param checkNames if `true`, require named types to have the same name (nominal type checking); if `false`, consider types with the same structure but different names to be equivalent (structural type checking)
      * @return `true` if `that` is an acceptable substitute for `this` (or is exactly the same); `false` if incompatible
      */
    def accepts(that: Type, checkNames: Boolean = true): Boolean
    /** Return `this` if an [[com.opendatagroup.hadrian.datatype.AvroType AvroType]], raise an `IllegalArgumentException` otherwise.
      */
    def avroType: AvroType = throw new IllegalArgumentException
  }

  /** Pseudo-type for inlines functions that can appear in argument lists.
    */
  case class FcnType(params: Seq[Type], ret: AvroType) extends Type {
    def accepts(that: Type, checkNames: Boolean = true): Boolean = that match {
      case FcnType(thatparams, thatret) =>
        thatparams.corresponds(params) {(x, y) => x.accepts(y, checkNames) }  &&  ret.accepts(thatret, checkNames)
      case _ => false
    }
    override def toString() = """{"type":"function","params":[%s],"ret":%s}""".format(params.mkString(","), ret.toString)
  }

  ///////////////////////////////////////////////////////// Avro types

  /** Import this object to get implicit conversions among `org.apache.avro.Schema`, [[com.opendatagroup.hadrian.datatype.AvroType AvroType]], [[com.opendatagroup.hadrian.datatype.AvroPlaceholder AvroPlaceholder]], and their JSON representation.
    */
  object AvroConversions {
    /** Convert a `org.apache.avro.Schema` from the Avro library into a Hadrian [[com.opendatagroup.hadrian.datatype.AvroType AvroType]].
      */
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
    /** Convert a `org.apache.avro.Schema` from the Avro library into a Hadrian [[com.opendatagroup.hadrian.datatype.AvroPlaceholder AvroPlaceholder]].
      */
    implicit def schemaToAvroPlaceholder(schema: Schema): AvroPlaceholder = AvroFilledPlaceholder(schemaToAvroType(schema))
    /** Convert a Hadrian [[com.opendatagroup.hadrian.datatype.AvroType AvroType]] into a `org.apache.avro.Schema` for the Avro library.
      */
    implicit def avroTypeToSchema(avroType: AvroType): Schema = avroType.schema
    /** Convert an AvroType into an AvroPlaceholder.
      */
    implicit def avroTypeToPlaceholder(avroType: AvroType): AvroPlaceholder = AvroFilledPlaceholder(avroType)
    /** Convert a file containing an Avro schema in JSON (usually ending with ".avsc") into a Hadrian [[com.opendatagroup.hadrian.datatype.AvroType AvroType]].
      */
    implicit def fileToAvroType(x: java.io.File): AvroType = schemaToAvroType((new Schema.Parser).parse(x))
    /** Convert an `InputStream` containing an Avro schema in JSON into a Hadrian [[com.opendatagroup.hadrian.datatype.AvroType AvroType]].
      */
    implicit def inputStreamToAvroType(x: java.io.InputStream): AvroType = schemaToAvroType((new Schema.Parser).parse(x))
    /** Convert a `String` containing an Avro schema in JSON into a Hadrian [[com.opendatagroup.hadrian.datatype.AvroType AvroType]].
      */
    implicit def stringToAvroType(x: String): AvroType = schemaToAvroType((new Schema.Parser).parse(x))
  }

  /** Base class for types of all PFA/Avro values.
    * 
    * Thin wrapper around an `org.apache.avro.Schema`, providing a different case class for each Avro kind (for more type safety).
    */
  abstract class AvroType extends Type {
    /** The "name" of the type, which is used as a key in tagged unions.
      */
    def name: String
    /** The a fully qualified name of this type, including the optional namespace.
      */
    def fullName: String = name
    /** The the Avro library `org.apache.avro.Schema` wrapped by this object.
      */
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

    private[datatype] def _recordFieldsOkay(thisRecord: AvroRecord, thatRecord: AvroRecord, memo: mutable.Set[String], checkRecord: Boolean, checkNames: Boolean): Boolean = {
      for (xf <- thisRecord.fields)
        if (xf.default == None) {
          if (!thatRecord.fields.exists(yf => xf.name == yf.name  &&  xf.avroType._accepts(yf.avroType, memo, checkRecord, checkNames)))
            return false
        }
        else {
          // not having a matching name in y is fine: x has a default
          // but having a matching name with a mismatched type is bad
          // (spec isn't clear, but org.apache.avro.SchemaCompatibility works that way)
          for (yf <- thatRecord.fields if (xf.name == yf.name))
            if (!xf.avroType._accepts(yf.avroType, memo, checkRecord, checkNames))
              return false
        }
      true
    }

    // this == "reader" (the anticipated signature, pattern to be matched),
    // that == "writer" (the given fact, argument to be accepted or rejected)
    def accepts(that: Type, checkNames: Boolean = true): Boolean = _accepts(that, mutable.Set(), true, checkNames)

    private def _accepts(that: Type, memo: mutable.Set[String] = mutable.Set(), checkRecord: Boolean = true, checkNames: Boolean = true): Boolean = (this, that) match {
      case (_, exceptionThat: ExceptionType) => false

      case (AvroNull(), AvroNull()) => true
      case (AvroBoolean(), AvroBoolean()) => true
      case (AvroBytes(), AvroBytes()) => true
      case (AvroString(), AvroString()) => true

      case (AvroInt(), AvroInt()) => true
      case (AvroLong(), AvroInt() | AvroLong()) => true
      case (AvroFloat(), AvroInt() | AvroLong() | AvroFloat()) => true
      case (AvroDouble(), AvroInt() | AvroLong() | AvroFloat() | AvroDouble()) => true

      case (AvroArray(x), AvroArray(y)) => x._accepts(y, memo, checkRecord, checkNames)
      case (AvroMap(x), AvroMap(y)) => x._accepts(y, memo, checkRecord, checkNames)

      case (AvroFixed(thisSize, thisName, thisNamespace, _, _), AvroFixed(thatSize, thatName, thatNamespace, _, _)) =>
        thisSize == thatSize  &&  (!checkNames  ||  (thisName == thatName  &&  thisNamespace == thatNamespace))

      case (AvroEnum(thisSymbols, thisName, thisNamespace, _, _), AvroEnum(thatSymbols, thatName, thatNamespace, _, _)) =>
        (thatSymbols.toSet subsetOf thisSymbols.toSet)  &&  (!checkNames  ||  (thisName == thatName  &&  thisNamespace == thatNamespace))

      case (thisRecord @ AvroRecord(thisFields, _, _, _, _), thatRecord @ AvroRecord(thatFields, _, _, _, _)) =>
        if (checkNames  &&  thisRecord.fullName != thatRecord.fullName)
          false
        else if (checkRecord  &&  !memo.contains(thatRecord.fullName)) {
          if (!_recordFieldsOkay(thisRecord, thatRecord, memo, checkRecord = false, checkNames))
            return false
          memo.add(thisRecord.fullName)
          if (!_recordFieldsOkay(thisRecord, thatRecord, memo, checkRecord, checkNames))
            return false
          true
        }
        else
          true

      case (AvroUnion(thisTypes), AvroUnion(thatTypes)) =>
        for (yt <- thatTypes)
          if (!thisTypes.exists(xt => xt._accepts(yt, memo, checkRecord, checkNames)))
            return false
        true

      case (AvroUnion(thisTypes), _) =>
        thisTypes.exists(xt => xt._accepts(that, memo, checkRecord, checkNames))

      case (_, AvroUnion(thatTypes)) =>
        thatTypes.forall(yt => this._accepts(yt, memo, checkRecord, checkNames))

      case _ => false
    }

    /** Convert the type to a JSON string (suitable for an Avro ".avsc" file).
      */
    def toJson(): String = convertToJson(jsonNode(mutable.Set[String]()))
    /** Convert the type to a Jackson node.
      * 
      * @param memo used to avoid infinite loops on recursive record types.
      */
    def jsonNode(memo: mutable.Set[String]): JsonNode
    /** Calls `toJson()`.
      */
    override def toString(): String = toJson()

    override def avroType: AvroType = this
  }

  /** AvroTypes that are compiled in Java (AvroRecord, AvroFixed, AvroEnum).
    */
  trait AvroCompiled extends AvroType {
    /** Optional namespace.
      */
    def namespace: Option[String]
    /** Optional set of alternate names that can be resolved to this type (for Avro schema resolution backward compatibility).
      */
    def aliases: Set[String]
    /** Optional documentation string.
      */
    def doc: String
  }
  /** Numeric AvroTypes (AvroInt, AvroLong, AvroFloat, AvroDouble).
    */
  trait AvroNumber extends AvroType
  /** Raw-byte AvroTypes (AvroBytes, AvroFixed).
    */
  trait AvroRaw extends AvroType
  /** AvroTypes that can be used as identifiers (AvroString, AvroEnum).
    */
  trait AvroIdentifier extends AvroType
  /** AvroTypes that contain other AvroTypes (AvroArray, AvroMap, AvroRecord).
    */
  trait AvroContainer extends AvroType
  /** AvroTypes that are represented by a JSON object in JSON (AvroMap, AvroRecord).
    */
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

  // exception types are not part of Avro; this is a placeholder used by exceptions
  // (which must return a bottom type, a type that can have no value)
  /** Pseudo-type for exceptions (the "bottom" type in type theory).
    */
  private[hadrian] case class ExceptionType() extends AvroType {
    val name = "exception"
    override def accepts(that: Type, checkNames: Boolean = true): Boolean = that.isInstanceOf[ExceptionType]
    override def toString() = """{"type":"exception"}"""
    def schema: Schema = AvroNull().schema
    def jsonNode(memo: mutable.Set[String]): JsonNode = throw new Exception("don't call jsonNode() on ExceptionType")
  }

  ///////////////////////////////////////////////////////// Avro type wrappers

  // start classes
  /** Avro "null" type. Has only one possible value, `null`.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroNull(val schema: Schema) extends AvroType {
    val name = "null"
    def jsonNode(memo: mutable.Set[String]): JsonNode = new TextNode("null")
  }

  /** Avro "boolean" type. Has only two possible values, `true` and `false`.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroBoolean(val schema: Schema) extends AvroType {
    val name = "boolean"
    def jsonNode(memo: mutable.Set[String]): JsonNode = new TextNode("boolean")
  }

  /** Avro "int" type for 32-bit integers.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroInt(val schema: Schema) extends AvroType with AvroNumber {
    val name = "int"
    def jsonNode(memo: mutable.Set[String]): JsonNode = new TextNode("int")
  }

  /** Avro "long" type for 64-bit integers.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroLong(val schema: Schema) extends AvroType with AvroNumber {
    val name = "long"
    def jsonNode(memo: mutable.Set[String]): JsonNode = new TextNode("long")
  }

  /** Avro "float" type for 32-bit IEEE floating-point numbers.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroFloat(val schema: Schema) extends AvroType with AvroNumber {
    val name = "float"
    def jsonNode(memo: mutable.Set[String]): JsonNode = new TextNode("float")
  }

  /** Avro "double" type for 64-bit IEEE floating-point numbers.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroDouble(val schema: Schema) extends AvroType with AvroNumber {
    val name = "double"
    def jsonNode(memo: mutable.Set[String]): JsonNode = new TextNode("double")
  }

  /** Avro "bytes" type for arbitrary byte arrays.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroBytes(val schema: Schema) extends AvroType with AvroRaw {
    val name = "bytes"
    def jsonNode(memo: mutable.Set[String]): JsonNode = new TextNode("bytes")
  }

  /** Avro "fixed" type for fixed-length byte arrays.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroFixed(val schema: Schema) extends AvroType with AvroRaw with AvroCompiled {
    def size: Int = schema.getFixedSize
    override def name: String = schema.getName
    override def namespace: Option[String] = schema.getNamespace match {case null => None; case x => Some(x)}
    override def fullName: String = schema.getFullName
    override def aliases: Set[String] = schema.getAliases.toSet
    override def doc: String = schema.getDoc

    def jsonNode(memo: mutable.Set[String]): JsonNode =
      if (memo.contains(fullName))
        new TextNode(fullName)
      else {
        memo.add(fullName)
        val factory = JsonNodeFactory.instance
        val out = factory.objectNode
        out.put("type", "fixed")
        out.put("size", new IntNode(size))

        out.put("name", new TextNode(name))
        namespace.foreach(x => out.put("namespace", new TextNode(x)))
        if (!aliases.isEmpty) {
          val aliasItems = factory.arrayNode
          aliases.foreach(x => aliasItems.add(new TextNode(x)))
          out.put("aliases", aliasItems)
        }
        if (doc != null  &&  !doc.isEmpty)
          out.put("doc", new TextNode(doc))
        out
      }
  }

  /** Avro "string" type for UTF-8 encoded strings.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroString(val schema: Schema) extends AvroType with AvroIdentifier {
    val name = "string"
    def jsonNode(memo: mutable.Set[String]): JsonNode = new TextNode("string")
  }

  /** Avro "enum" type for a small collection of string-labeled values.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroEnum(val schema: Schema) extends AvroType with AvroIdentifier with AvroCompiled {
    def symbols: Seq[String] = schema.getEnumSymbols.toVector
    override def name: String = schema.getName
    override def namespace: Option[String] = schema.getNamespace match {case null => None; case x => Some(x)}
    override def fullName: String = schema.getFullName
    override def aliases: Set[String] = schema.getAliases.toSet
    override def doc: String = schema.getDoc

    def jsonNode(memo: mutable.Set[String]): JsonNode =
      if (memo.contains(fullName))
        new TextNode(fullName)
      else {
        memo.add(fullName)
        val factory = JsonNodeFactory.instance
        val out = factory.objectNode
        out.put("type", "enum")
        val symbolItems = factory.arrayNode
        symbols.foreach(x => symbolItems.add(new TextNode(x)))
        out.put("symbols", symbolItems)

        out.put("name", new TextNode(name))
        namespace.foreach(x => out.put("namespace", new TextNode(x)))
        if (!aliases.isEmpty) {
          val aliasItems = factory.arrayNode
          aliases.foreach(x => aliasItems.add(new TextNode(x)))
          out.put("aliases", aliasItems)
        }
        if (doc != null  &&  !doc.isEmpty)
          out.put("doc", new TextNode(doc))
        out
      }
  }

  /** Avro "array" type for homogeneous lists.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroArray(val schema: Schema) extends AvroType with AvroContainer {
    val name = "array"
    /** Type of the contained objects.
      */
    def items: AvroType = AvroConversions.schemaToAvroType(schema.getElementType)
    def jsonNode(memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      out.put("type", "array")
      out.put("items", items.jsonNode(memo))
      out
    }
  }

  /** Avro "map" type for homogeneous maps (keys must be strings).
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroMap(val schema: Schema) extends AvroType with AvroContainer with AvroMapping {
    val name = "map"
    /** Type of the contained objects.
      */
    def values: AvroType = AvroConversions.schemaToAvroType(schema.getValueType)
    def jsonNode(memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      out.put("type", "map")
      out.put("values", values.jsonNode(memo))
      out
    }
  }

  /** Avro "record" type for inhomogeneous collections of named (and required) fields.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroRecord(val schema: Schema) extends AvroType with AvroContainer with AvroMapping with AvroCompiled {
    /** Names of the fields, in order.
      */
    def fieldNames: Seq[String] = schema.getFields map { x => x.name }
    /** Field references, in order.
      */
    def fields: Seq[AvroField] = schema.getFields map { x => new AvroField(x) }
    /** Field requested by name, returning `None` if not present.
      * 
      * @param name name of the requested field
      */
    def fieldOption(name: String): Option[AvroField] = schema.getFields find { _.name == name } map { new AvroField(_) }
    /** Field requested by name, raising `NoSuchElementException` if not present.
      * 
      * @param name name of the requested field
      */
    def field(name: String): AvroField = fieldOption(name).get
    override def name: String = schema.getName
    override def namespace: Option[String] = schema.getNamespace match {case null => None; case x => Some(x)}
    override def fullName: String = schema.getFullName
    override def aliases: Set[String] = schema.getAliases.toSet
    override def doc: String = schema.getDoc

    def jsonNode(memo: mutable.Set[String]): JsonNode =
      if (memo.contains(fullName))
        new TextNode(fullName)
      else {
        memo.add(fullName)
        val factory = JsonNodeFactory.instance
        val out = factory.objectNode
        out.put("type", "record")
        val fieldItems = factory.arrayNode
        fields.foreach(x => fieldItems.add(x.jsonNode(memo)))
        out.put("fields", fieldItems)

        out.put("name", new TextNode(name))
        namespace.foreach(x => out.put("namespace", new TextNode(x)))
        if (!aliases.isEmpty) {
          val aliasItems = factory.arrayNode
          aliases.foreach(x => aliasItems.add(new TextNode(x)))
          out.put("aliases", aliasItems)
        }
        if (doc != null  &&  !doc.isEmpty)
          out.put("doc", new TextNode(doc))
        out
      }
  }

  /** Field for an Avro "record" type.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema.Field` object.
    */
  class AvroField(val schemaField: Schema.Field) {
    /** The "name" of this field.
      */
    def name: String = schemaField.name
    /** The "type" of this field.
      */
    def avroType: AvroType = AvroConversions.schemaToAvroType(schemaField.schema)
    /** The "default" value of this field as a Jackson node.
      */
    def default: Option[JsonNode] =
      if (schemaField.defaultValue == null)
        None
      else
        Some(schemaField.defaultValue)
    /** The "order" this field, which defines a sort order in Avro ("ascending", "descending", or "ignore").
      */
    def order: Schema.Field.Order = schemaField.order
    /** Set of alternate names for this field (for Avro schema resolution backward compatibility).
      */
    def aliases: Set[String] = schemaField.aliases.toSet
    /** Optional documentation string for this field.
      */
    def doc: String = schemaField.doc

    /** Convert the field to a Jackson node.
      * 
      * @param memo used to avoid infinite loops on recursive record types.
      */
    def jsonNode(memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.objectNode
      out.put("name", name)
      out.put("type", avroType.jsonNode(memo))
      default.foreach(x => out.put("default", x))
      if (!aliases.isEmpty) {
        val aliasItems = factory.arrayNode
        aliases.foreach(x => aliasItems.add(new TextNode(x)))
        out.put("aliases", aliasItems)
      }
      if (order != Schema.Field.Order.ASCENDING)
        out.put("order", new TextNode(order.toString))
      out
    }

    /** Convert the field to a JSON string (suitable for part of an Avro ".avsc" file).
      */
    def toJson(): String = convertToJson(jsonNode(mutable.Set[String]()))

    /** Calls `toJson()`.
      */
    override def toString(): String = toJson()
  }

  /** Avro "union" type for tagged unions.
    * 
    * See companion object for a constructor that does not require Avro library objects.
    * 
    * @param schema wrapped Avro library `org.apache.avro.Schema` object.
    */
  class AvroUnion(val schema: Schema) extends AvroType {
    val name = "union"
    def types: Seq[AvroType] = schema.getTypes.map(AvroConversions.schemaToAvroType(_)).toList

    def jsonNode(memo: mutable.Set[String]): JsonNode = {
      val factory = JsonNodeFactory.instance
      val out = factory.arrayNode
      types.foreach(x => out.add(x.jsonNode(memo)))
      out
    }
  }

  // start objects

  object AvroNull {
    /** Create an AvroNull class without Avro library objects.
      */
    def apply(): AvroNull = new AvroNull(Schema.create(Schema.Type.NULL))
    def unapply(x: AvroNull): Boolean = true
  }

  object AvroBoolean {
    /** Create an AvroBoolean class without Avro library objects.
      */
    def apply(): AvroBoolean = new AvroBoolean(Schema.create(Schema.Type.BOOLEAN))
    def unapply(x: AvroBoolean): Boolean = true
  }

  object AvroInt {
    /** Create an AvroInt class without Avro library objects.
      */
    def apply(): AvroInt = new AvroInt(Schema.create(Schema.Type.INT))
    def unapply(x: AvroInt): Boolean = true
  }

  object AvroLong {
    /** Create an AvroLong class without Avro library objects.
      */
    def apply(): AvroLong = new AvroLong(Schema.create(Schema.Type.LONG))
    def unapply(x: AvroLong): Boolean = true
  }

  object AvroFloat {
    /** Create an AvroFloat class without Avro library objects.
      */
    def apply(): AvroFloat = new AvroFloat(Schema.create(Schema.Type.FLOAT))
    def unapply(x: AvroFloat): Boolean = true
  }

  object AvroDouble {
    /** Create an AvroDouble class without Avro library objects.
      */
    def apply(): AvroDouble = new AvroDouble(Schema.create(Schema.Type.DOUBLE))
    def unapply(x: AvroDouble): Boolean = true
  }

  object AvroBytes {
    /** Create an AvroBytes class without Avro library objects.
      */
    def apply(): AvroBytes = new AvroBytes(Schema.create(Schema.Type.BYTES))
    def unapply(x: AvroBytes): Boolean = true
  }

  object AvroFixed {
    /** Create an AvroFixed class without Avro library objects.
      * 
      * @param size lenght of the fixed-length byte arrays
      * @param name name or an auto-generated name
      * @param namespace optional namespace
      * @param aliases optional set of alternate names for backward-compatibility
      * @param doc optional documentation string
      */
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
    /** Create an AvroString class without Avro library objects.
      */
    def apply(): AvroString = new AvroString(Schema.create(Schema.Type.STRING))
    def unapply(x: AvroString): Boolean = true
  }

  object AvroEnum {
    /** Create an AvroEnum class without Avro library objects.
      * 
      * @param symbols collection of labels
      * @param name name or an auto-generated name
      * @param namespace namespace or no namespace
      * @param aliases optional set of alternate names for backward-compatibility
      * @param doc optional documentation string
      */
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
    /** Create an AvroArray class without Avro library objects.
      * 
      * @param items type of the contained objects
      */
    def apply(items: AvroType): AvroArray = new AvroArray(Schema.createArray(items.schema))
    def unapply(x: AvroArray): Option[AvroType] = Some(x.items)
  }

  object AvroMap {
    /** Create an AvroMap class without Avro library objects.
      * @param values type of the contained objects
      */
    def apply(values: AvroType): AvroMap = new AvroMap(Schema.createMap(values.schema))
    def unapply(x: AvroMap): Option[AvroType] = Some(x.values)
  }

  object AvroRecord {
    /** Create an AvroRecord class without Avro library objects.
      * 
      * @param fields field names and types in order
      * @param name name or an auto-generated name
      * @param namespace namespace or no namespace
      * @param aliases optional set of alternate names for backward-compatibility
      * @param doc optional documentation string
      */
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
    /** Create an AvroField class for an AvroRecord without Avro library objects.
      * 
      * @param name name of this field
      * @param type type of this field
      * @param default optional default value for this field as a Jackson node
      * @param order Avro sort order for this field ("ascending", "descending", or "ignore")
      * @param aliases optional set of alternate names for backward-compatibility
      * @param doc optional documentation string
      */
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
    /** Create an AvroUnion class without Avro library objects.
      * 
      * @param types possible types for this union, in the order of their resolution.
      */
    def apply(types: Seq[AvroType]): AvroUnion = new AvroUnion(Schema.createUnion(types map { _.schema }))
    def unapply(x: AvroUnion): Option[Seq[AvroType]] = Some(x.types)
  }

  /////////////////////////// resolving types out of order in streaming input

  /** Represents a type that can't be resolved yet because JSON objects may be streamed in an unknown order.
    */
  class AvroPlaceholder(original: String, forwardDeclarationParser: ForwardDeclarationParser) {
    override def equals(other: Any): Boolean = other match {
      case that: AvroPlaceholder => this.avroType == that.avroType
      case that: AvroType => this.avroTypeOption == Some(that)
      case _ => false
    }
    /** Called after [[com.opendatagroup.hadrian.datatype.AvroTypeBuilder AvroTypeBuilder]] `resolveTypes` to get the resolved type.
      */
    def avroType: AvroType = forwardDeclarationParser.lookup(original)
    /** Attempt to get `avroType`, returning `None` if types have not been resolved yet.
      */
    def avroTypeOption: Option[AvroType] = forwardDeclarationParser.lookupOption(original)
    /** Represents the placeholder as its resolved type in JSON or `{"type":"unknown"}` if not resolved yet.
      */
    override def toString(): String = forwardDeclarationParser.lookupOption(original) match {
      case Some(x) => x.toJson()
      case None => """{"type":"unknown"}"""
    }

    /** Represent the resolved type as a JSON string.
      */
    def toJson() = convertToJson(jsonNode(mutable.Set[String]()))
    /** Represent the resolved type as a Jackson node.
      * 
      * @param memo used to avoid infinite loops with recursive records
      */
    def jsonNode(memo: mutable.Set[String]) = avroType.jsonNode(memo)

    /** The [[com.opendatagroup.hadrian.datatype.ForwardDeclarationParser ForwardDeclarationParser]] responsible for this placeholder.
      */
    def parser = forwardDeclarationParser
  }
  object AvroPlaceholder {
    /** Create an `AvroPlaceholder` from a JSON Avro type string.
      * 
      * @param original JSON string
      * @param forwardDeclarationParser the parser used to read types in any order
      */
    def apply(original: String, forwardDeclarationParser: ForwardDeclarationParser): AvroPlaceholder = new AvroPlaceholder(original, forwardDeclarationParser)
    def unapply(avroPlaceholder: AvroPlaceholder): Option[AvroType] = avroPlaceholder.avroTypeOption
  }

  /** Used to create [[com.opendatagroup.hadrian.datatype.AvroPlaceholder AvroPlaceholder]] objects to satisfy functions that require them, yet the type is already known.
    */
  case class AvroFilledPlaceholder(override val avroType: AvroType) extends AvroPlaceholder("", null) {
    override def avroTypeOption: Option[AvroType] = Some(avroType)
    override def toString(): String = avroType.toString
  }

  /** Container that stores Avro types as they're collected from a PFA file, returning [[com.opendatagroup.hadrian.datatype.AvroPlaceholder AvroPlaceholder]] objects, and then resolves those types indepenedent of the order in which they were read from the file.
    */
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

  /** Factory that coordinates the process of collecting Avro strings, putting them in a [[com.opendatagroup.hadrian.datatype.ForwardDeclarationParser ForwardDeclarationParser]], and then resolving them all at the end, independent of order.
    */
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
