package test.scala.data

import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap
import scala.collection.mutable

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import org.codehaus.jackson.map.ObjectMapper
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import org.apache.avro.io.JsonEncoder
import org.apache.avro.Schema
import org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType

import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.datatype._
import com.opendatagroup.hadrian.datatype.AvroConversions._
import test.scala._

@RunWith(classOf[JUnitRunner])
class DataSuite extends FlatSpec with Matchers {
  val outsideSpecificData = new PFASpecificData(getClass.getClassLoader)

  def loadFromJson(json: String, schema: Schema, specificData: PFASpecificData = outsideSpecificData): AnyRef = {
    val reader = new PFADatumReader[AnyRef](specificData)
    reader.setSchema(schema)
    val decoder = DecoderFactory.get.jsonDecoder(schema, json)
    reader.read(null, decoder)
  }

  def loadFromAvro(avro: Array[Byte], schema: Schema, specificData: PFASpecificData = outsideSpecificData): AnyRef = {
    val reader = new PFADatumReader[AnyRef](specificData)
    reader.setSchema(schema)
    val decoder = DecoderFactory.get.validatingDecoder(schema, DecoderFactory.get.binaryDecoder(avro, null))
    reader.read(null, decoder)
  }

  def writeToJson(obj: AnyRef, schema: Schema, specificData: PFASpecificData = outsideSpecificData): String = {
    val out = new java.io.ByteArrayOutputStream
    val encoder = EncoderFactory.get.jsonEncoder(schema, out)
    val writer = new PFADatumWriter[AnyRef](schema, specificData)
    writer.write(obj, encoder)
    encoder.flush()
    out.toString
  }

  def writeToAvro(obj: AnyRef, schema: Schema, specificData: PFASpecificData = outsideSpecificData): Array[Byte] = {
    val out = new java.io.ByteArrayOutputStream
    val encoder = EncoderFactory.get.validatingEncoder(schema, EncoderFactory.get.binaryEncoder(out, null))
    val writer = new PFADatumWriter[AnyRef](schema, specificData)
    writer.write(obj, encoder)
    encoder.flush()
    out.toByteArray
  }

  "Avro data representation" must "load strings as Java Strings, not avro.util.Utf8" taggedAs(Data) in {
    val stringSchema = AvroString().schema
    val string = loadFromJson(""""hello"""", stringSchema)
    string should be ("hello")
    string.getClass.getName should be ("java.lang.String")
    writeToJson(string, stringSchema) should be (""""hello"""")

    val stringInArraySchema = AvroArray(AvroString()).schema
    val stringInArray = loadFromJson("""["hello"]""", stringInArraySchema).asInstanceOf[java.util.List[_]]
    stringInArray.head should be ("hello")
    stringInArray.head.getClass.getName should be ("java.lang.String")
    stringInArray.head.getClass.getName should be ("java.lang.String")
    writeToJson(stringInArray, stringInArraySchema) should be ("""["hello"]""")

    val stringInMapSchema = AvroMap(AvroString()).schema
    val stringInMap = loadFromJson("""{"hey": "hello"}""", stringInMapSchema).asInstanceOf[java.util.Map[_, _]]
    stringInMap.toMap.keys.head should be ("hey")
    stringInMap.get("hey") should be ("hello")
    stringInMap.toMap.keys.head.getClass.getName should be ("java.lang.String")
    stringInMap.get("hey").getClass.getName should be ("java.lang.String")
    writeToJson(stringInMap, stringInMapSchema) should be ("""{"hey":"hello"}""")

    // val stringInRecordSchema = AvroRecord(List(AvroField("hey", AvroString())), "HeyString", Some("test.scala.avro")).schema
    // val stringInRecord = loadFromJson("""{"hey": "hello"}""", stringInRecordSchema).asInstanceOf[org.apache.avro.generic.GenericRecord]
    // stringInRecord.get("hey") should be ("hello")
    // stringInRecord.get("hey").getClass.getName should be ("java.lang.String")
    // writeToJson(stringInRecord, stringInRecordSchema) should be ("""{"hey":"hello"}""")

    val stringInUnionSchema = AvroUnion(List(AvroString())).schema
    val stringInUnion = loadFromJson("""{"string": "hello"}""", stringInUnionSchema)
    stringInUnion should be ("hello")
    stringInUnion.getClass.getName should be ("java.lang.String")
    writeToJson(stringInUnion, stringInUnionSchema) should be ("""{"string":"hello"}""")

    val string2 = loadFromAvro(writeToAvro(string, stringSchema), stringSchema)
    string2 should be ("hello")
    string2.getClass.getName should be ("java.lang.String")

    val stringInArray2 = loadFromAvro(writeToAvro(stringInArray, stringInArraySchema), stringInArraySchema).asInstanceOf[java.util.List[_]]
    stringInArray2.head should be ("hello")
    stringInArray2.head.getClass.getName should be ("java.lang.String")

    val stringInMap2 = loadFromAvro(writeToAvro(stringInMap, stringInMapSchema), stringInMapSchema).asInstanceOf[java.util.Map[_, _]]
    stringInMap2.toMap.keys.head should be ("hey")
    stringInMap2.get("hey") should be ("hello")
    stringInMap2.toMap.keys.head.getClass.getName should be ("java.lang.String")
    stringInMap2.get("hey").getClass.getName should be ("java.lang.String")

    // val stringInRecord2 = loadFromAvro(writeToAvro(stringInRecord, stringInRecordSchema), stringInRecordSchema).asInstanceOf[org.apache.avro.generic.GenericRecord]
    // stringInRecord2.get("hey") should be ("hello")
    // stringInRecord2.get("hey").getClass.getName should be ("java.lang.String")

    val stringInUnion2 = loadFromAvro(writeToAvro(stringInUnion, stringInUnionSchema), stringInUnionSchema)
    stringInUnion2 should be ("hello")
    stringInUnion2.getClass.getName should be ("java.lang.String")
  }

  it must "load arrays as PFAArray, not avro.generic.GenericData.Array" taggedAs(Data) in {
    val arrayOfStringsSchema = AvroArray(AvroString()).schema
    val arrayOfStrings = loadFromJson("""["one", "two", "three"]""", arrayOfStringsSchema).asInstanceOf[PFAArray[String]]
    arrayOfStrings.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAArray")
    arrayOfStrings(0) should be ("one")
    arrayOfStrings(0).getClass.getName should be ("java.lang.String")
    writeToJson(arrayOfStrings, arrayOfStringsSchema) should be ("""["one","two","three"]""")

    val arrayOfDoublesSchema = AvroArray(AvroDouble()).schema
    val arrayOfDoubles = loadFromJson("""[1, 2, 3]""", arrayOfDoublesSchema).asInstanceOf[PFAArray[Double]]
    arrayOfDoubles.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAArray")
    arrayOfDoubles(0) should be (1)
    arrayOfDoubles(0).getClass.getName should be ("double")
    writeToJson(arrayOfDoubles, arrayOfDoublesSchema) should be ("""[1.0,2.0,3.0]""")

    val arrayOfarrayOfStringsSchema = AvroArray(AvroArray(AvroString())).schema
    val arrayOfarrayOfStrings = loadFromJson("""[["one", "two", "three"], ["uno", "dos", "tres"], ["un", "deux", "trois"]]""", arrayOfarrayOfStringsSchema).asInstanceOf[PFAArray[PFAArray[String]]]
    arrayOfarrayOfStrings.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAArray")
    arrayOfarrayOfStrings(0)(0) should be ("one")
    arrayOfarrayOfStrings(0).getClass.getName should be ("com.opendatagroup.hadrian.data.PFAArray")
    arrayOfarrayOfStrings(0)(0).getClass.getName should be ("java.lang.String")
    writeToJson(arrayOfarrayOfStrings, arrayOfarrayOfStringsSchema) should be ("""[["one","two","three"],["uno","dos","tres"],["un","deux","trois"]]""")

    val arrayOfStringsInMapSchema = AvroMap(AvroArray(AvroString())).schema
    val arrayOfStringsInMap = loadFromJson("""{"hey": ["one", "two", "three"]}""", arrayOfStringsInMapSchema).asInstanceOf[java.util.Map[String, java.util.List[_]]]
    arrayOfStringsInMap.get("hey").getClass.getName should be ("com.opendatagroup.hadrian.data.PFAArray")
    arrayOfStringsInMap.get("hey").get(0) should be ("one")
    writeToJson(arrayOfStringsInMap, arrayOfStringsInMapSchema) should be ("""{"hey":["one","two","three"]}""")

    // val arrayOfStringsInRecordSchema = AvroRecord(List(AvroField("hey", AvroArray(AvroString())))).schema
    // val arrayOfStringsInRecord = loadFromJson("""{"hey": ["one", "two", "three"]}""", arrayOfStringsInRecordSchema).asInstanceOf[org.apache.avro.generic.GenericRecord]
    // arrayOfStringsInRecord.get("hey").getClass.getName should be ("com.opendatagroup.hadrian.data.PFAArray")
    // arrayOfStringsInRecord.get("hey").asInstanceOf[com.opendatagroup.hadrian.data.PFAArray[_]](0) should be ("one")
    // writeToJson(arrayOfStringsInRecord, arrayOfStringsInRecordSchema) should be ("""{"hey":["one","two","three"]}""")

    val arrayOfStringsInUnionSchema = AvroUnion(List(AvroArray(AvroString()))).schema
    val arrayOfStringsInUnion = loadFromJson("""{"array": ["one", "two", "three"]}""", arrayOfStringsInUnionSchema).asInstanceOf[java.util.List[_]]
    arrayOfStringsInUnion.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAArray")
    arrayOfStringsInUnion.get(0) should be ("one")
    writeToJson(arrayOfStringsInUnion, arrayOfStringsInUnionSchema) should be ("""{"array":["one","two","three"]}""")

    val arrayOfStrings2 = loadFromAvro(writeToAvro(arrayOfStrings, arrayOfStringsSchema), arrayOfStringsSchema)
    arrayOfStrings2 should be (arrayOfStrings)
    arrayOfStrings2.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAArray")

    val arrayOfDoubles2 = loadFromAvro(writeToAvro(arrayOfDoubles, arrayOfDoublesSchema), arrayOfDoublesSchema)
    arrayOfDoubles2 should be (arrayOfDoubles)
    arrayOfDoubles2.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAArray")

    val arrayOfStringsInMap2 = loadFromAvro(writeToAvro(arrayOfStringsInMap, arrayOfStringsInMapSchema), arrayOfStringsInMapSchema).asInstanceOf[java.util.Map[String, _]]
    arrayOfStringsInMap2 should be (arrayOfStringsInMap)
    arrayOfStringsInMap2.get("hey").getClass.getName should be ("com.opendatagroup.hadrian.data.PFAArray")

    // val arrayOfStringsInRecord2 = loadFromAvro(writeToAvro(arrayOfStringsInRecord, arrayOfStringsInRecordSchema), arrayOfStringsInRecordSchema).asInstanceOf[org.apache.avro.generic.GenericRecord]
    // arrayOfStringsInRecord2 should be (arrayOfStringsInRecord)
    // arrayOfStringsInRecord2.get("hey").getClass.getName should be ("com.opendatagroup.hadrian.data.PFAArray")

    val arrayOfStringsInUnion2 = loadFromAvro(writeToAvro(arrayOfStringsInUnion, arrayOfStringsInUnionSchema), arrayOfStringsInUnionSchema)
    arrayOfStringsInUnion2 should be (arrayOfStringsInUnion)
    arrayOfStringsInUnion2.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAArray")
  }

  it must "load maps as PFAMap, not java.util.Map" taggedAs(Data) in {
    val mapOfStringsSchema = AvroMap(AvroString()).schema
    val mapOfStrings = loadFromJson("""{"a": "one", "b": "two", "c": "three"}""", mapOfStringsSchema).asInstanceOf[PFAMap[String]]
    mapOfStrings.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAMap")
    mapOfStrings("a") should be ("one")
    mapOfStrings("a").getClass.getName should be ("java.lang.String")
    loadFromJson(writeToJson(mapOfStrings, mapOfStringsSchema), mapOfStringsSchema) should be (mapOfStrings)

    val mapOfDoublesSchema = AvroMap(AvroDouble()).schema
    val mapOfDoubles = loadFromJson("""{"a": 1, "b": 2, "c": 3}""", mapOfDoublesSchema).asInstanceOf[PFAMap[java.lang.Double]]
    mapOfDoubles.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAMap")
    mapOfDoubles("a") should be (1)
    mapOfDoubles("a").getClass.getName should be ("java.lang.Double")
    loadFromJson(writeToJson(mapOfDoubles, mapOfDoublesSchema), mapOfDoublesSchema) should be (mapOfDoubles)

    val mapOfmapOfStringsSchema = AvroMap(AvroMap(AvroString())).schema
    val mapOfmapOfStrings = loadFromJson("""{"en": {"a": "one", "b": "two", "c": "three"}, "es": {"a": "uno", "b": "dos", "c": "tres"}, "fr": {"a": "un", "b": "deux", "c": "trois"}}""", mapOfmapOfStringsSchema).asInstanceOf[PFAMap[PFAMap[String]]]
    mapOfmapOfStrings.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAMap")
    mapOfmapOfStrings("en")("a") should be ("one")
    mapOfmapOfStrings("en").getClass.getName should be ("com.opendatagroup.hadrian.data.PFAMap")
    mapOfmapOfStrings("en")("a").getClass.getName should be ("java.lang.String")
    loadFromJson(writeToJson(mapOfmapOfStrings, mapOfmapOfStringsSchema), mapOfmapOfStringsSchema) should be (mapOfmapOfStrings)

    val mapOfStringsInArraySchema = AvroArray(AvroMap(AvroString())).schema
    val mapOfStringsInArray = loadFromJson("""[{"a": "one", "b": "two", "c": "three"}, {"a": "uno", "b": "dos", "c": "tres"}, {"a": "un", "b": "deux", "c": "trois"}]""", mapOfStringsInArraySchema).asInstanceOf[java.util.List[java.util.Map[String, _]]]
    mapOfStringsInArray.get(0).getClass.getName should be ("com.opendatagroup.hadrian.data.PFAMap")
    mapOfStringsInArray.get(0).get("a") should be ("one")
    loadFromJson(writeToJson(mapOfStringsInArray, mapOfStringsInArraySchema), mapOfStringsInArraySchema) should be (mapOfStringsInArray)

    // val mapOfStringsInRecordSchema = AvroRecord(List(AvroField("hey", AvroMap(AvroString())))).schema
    // val mapOfStringsInRecord = loadFromJson("""{"hey": {"a": "one", "b": "two", "c": "three"}}""", mapOfStringsInRecordSchema).asInstanceOf[org.apache.avro.generic.GenericRecord]
    // mapOfStringsInRecord.get("hey").getClass.getName should be ("com.opendatagroup.hadrian.data.PFAMap")
    // mapOfStringsInRecord.get("hey").asInstanceOf[com.opendatagroup.hadrian.data.PFAMap[_]]("a") should be ("one")
    // loadFromJson(writeToJson(mapOfStringsInRecord, mapOfStringsInRecordSchema), mapOfStringsInRecordSchema) should be (mapOfStringsInRecord)

    val mapOfStringsInUnionSchema = AvroUnion(List(AvroMap(AvroString()))).schema
    val mapOfStringsInUnion = loadFromJson("""{"map": {"a": "one", "b": "two", "c": "three"}}""", mapOfStringsInUnionSchema).asInstanceOf[java.util.Map[String, _]]
    mapOfStringsInUnion.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAMap")
    mapOfStringsInUnion.get("a") should be ("one")
    loadFromJson(writeToJson(mapOfStringsInUnion, mapOfStringsInUnionSchema), mapOfStringsInUnionSchema) should be (mapOfStringsInUnion)

    val mapOfStrings2 = loadFromAvro(writeToAvro(mapOfStrings, mapOfStringsSchema), mapOfStringsSchema)
    mapOfStrings2 should be (mapOfStrings)
    mapOfStrings2.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAMap")

    val mapOfDoubles2 = loadFromAvro(writeToAvro(mapOfDoubles, mapOfDoublesSchema), mapOfDoublesSchema)
    mapOfDoubles2 should be (mapOfDoubles)
    mapOfDoubles2.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAMap")

    val mapOfStringsInArray2 = loadFromAvro(writeToAvro(mapOfStringsInArray, mapOfStringsInArraySchema), mapOfStringsInArraySchema).asInstanceOf[java.util.List[_]]
    mapOfStringsInArray2 should be (mapOfStringsInArray)
    mapOfStringsInArray2.get(0).getClass.getName should be ("com.opendatagroup.hadrian.data.PFAMap")

    // val mapOfStringsInRecord2 = loadFromAvro(writeToAvro(mapOfStringsInRecord, mapOfStringsInRecordSchema), mapOfStringsInRecordSchema).asInstanceOf[org.apache.avro.generic.GenericRecord]
    // mapOfStringsInRecord2 should be (mapOfStringsInRecord)
    // mapOfStringsInRecord2.get("hey").getClass.getName should be ("com.opendatagroup.hadrian.data.PFAMap")

    val mapOfStringsInUnion2 = loadFromAvro(writeToAvro(mapOfStringsInUnion, mapOfStringsInUnionSchema), mapOfStringsInUnionSchema)
    mapOfStringsInUnion2 should be (mapOfStringsInUnion)
    mapOfStringsInUnion2.getClass.getName should be ("com.opendatagroup.hadrian.data.PFAMap")
  }

  it must "load maps as Array[Byte], not java.nio.ByteBuffer" taggedAs(Data) in {
    val bytesSchema = AvroBytes().schema
    val bytes = loadFromJson(""""hello"""", bytesSchema).asInstanceOf[Array[Byte]]
    bytes.toList should be (List(104, 101, 108, 108, 111))
    bytes.getClass.getName should be ("[B")
    writeToJson(bytes, bytesSchema) should be (""""hello"""")

    val bytesInArraySchema = AvroArray(AvroBytes()).schema
    val bytesInArray = loadFromJson("""["hello"]""", bytesInArraySchema).asInstanceOf[java.util.List[_]]
    bytesInArray.head.asInstanceOf[Array[Byte]].toList should be (List(104, 101, 108, 108, 111))
    bytesInArray.head.getClass.getName should be ("[B")
    bytesInArray.head.getClass.getName should be ("[B")
    writeToJson(bytesInArray, bytesInArraySchema) should be ("""["hello"]""")

    val bytesInMapSchema = AvroMap(AvroBytes()).schema
    val bytesInMap = loadFromJson("""{"hey": "hello"}""", bytesInMapSchema).asInstanceOf[java.util.Map[_, _]]
    bytesInMap.get("hey").asInstanceOf[Array[Byte]].toList should be (List(104, 101, 108, 108, 111))
    bytesInMap.get("hey").getClass.getName should be ("[B")
    writeToJson(bytesInMap, bytesInMapSchema) should be ("""{"hey":"hello"}""")

    // val bytesInRecordSchema = AvroRecord(List(AvroField("hey", AvroBytes()))).schema
    // val bytesInRecord = loadFromJson("""{"hey": "hello"}""", bytesInRecordSchema).asInstanceOf[org.apache.avro.generic.GenericRecord]
    // bytesInRecord.get("hey").asInstanceOf[Array[Byte]].toList should be (List(104, 101, 108, 108, 111))
    // bytesInRecord.get("hey").getClass.getName should be ("[B")
    // writeToJson(bytesInRecord, bytesInRecordSchema) should be ("""{"hey":"hello"}""")

    val bytesInUnionSchema = AvroUnion(List(AvroBytes())).schema
    val bytesInUnion = loadFromJson("""{"bytes": "hello"}""", bytesInUnionSchema)
    bytesInUnion.asInstanceOf[Array[Byte]].toList should be (List(104, 101, 108, 108, 111))
    bytesInUnion.getClass.getName should be ("[B")
    writeToJson(bytesInUnion, bytesInUnionSchema) should be ("""{"bytes":"hello"}""")

    val bytes2 = loadFromAvro(writeToAvro(bytes, bytesSchema), bytesSchema)
    bytes2.asInstanceOf[Array[Byte]].toList should be (List(104, 101, 108, 108, 111))
    bytes2.getClass.getName should be ("[B")

    val bytesInArray2 = loadFromAvro(writeToAvro(bytesInArray, bytesInArraySchema), bytesInArraySchema).asInstanceOf[java.util.List[_]]
    bytesInArray2.head.asInstanceOf[Array[Byte]].toList should be (List(104, 101, 108, 108, 111))
    bytesInArray2.head.getClass.getName should be ("[B")

    val bytesInMap2 = loadFromAvro(writeToAvro(bytesInMap, bytesInMapSchema), bytesInMapSchema).asInstanceOf[java.util.Map[_, _]]
    bytesInMap2.get("hey").asInstanceOf[Array[Byte]].toList should be (List(104, 101, 108, 108, 111))
    bytesInMap2.get("hey").getClass.getName should be ("[B")

    // val bytesInRecord2 = loadFromAvro(writeToAvro(bytesInRecord, bytesInRecordSchema), bytesInRecordSchema).asInstanceOf[org.apache.avro.generic.GenericRecord]
    // bytesInRecord2.get("hey").asInstanceOf[Array[Byte]].toList should be (List(104, 101, 108, 108, 111))
    // bytesInRecord2.get("hey").getClass.getName should be ("[B")

    val bytesInUnion2 = loadFromAvro(writeToAvro(bytesInUnion, bytesInUnionSchema), bytesInUnionSchema)
    bytesInUnion2.asInstanceOf[Array[Byte]].toList should be (List(104, 101, 108, 108, 111))
    bytesInUnion2.getClass.getName should be ("[B")
  }

  it must "load fixed as PFAFixed, not SpecificFixed" taggedAs(Data) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: {type: fixed, name: TestFixed, size: 5}
action:
  - {type: TestFixed, value: "hello"}
""").head
    val fixed = engine.action(java.lang.Double.valueOf(0))
    fixed.getClass.getName should be ("TestFixed")
    fixed.asInstanceOf[PFAFixed].size should be (5)
    fixed.asInstanceOf[PFAFixed].bytes.toList should be (List(104, 101, 108, 108, 111))

    val fixedSchema = AvroFixed(5, "TestFixed")
    writeToJson(fixed, fixedSchema) should be (""""hello"""")

    val engine2 = PFAEngine.fromYaml("""
input: double
output: {type: fixed, name: TestFixed, namespace: com.wowie, size: 5}
action:
  - {type: com.wowie.TestFixed, value: "hello"}
""").head
    val fixed2 = engine2.action(java.lang.Double.valueOf(0))
    fixed2.getClass.getName should be ("com.wowie.TestFixed")
    fixed2.asInstanceOf[PFAFixed].size should be (5)
    fixed2.asInstanceOf[PFAFixed].bytes.toList should be (List(104, 101, 108, 108, 111))

    val fixedSchema2 = AvroFixed(5, "com.wowie.TestFixed")
    writeToJson(fixed2, fixedSchema2) should be (""""hello"""")

    val ssd = new PFASpecificData(engine2.classLoader)
    val fixed3 = loadFromAvro(writeToAvro(fixed2, fixedSchema2, ssd), fixedSchema2, ssd)
    fixed3.getClass.getName should be ("com.wowie.TestFixed")
    fixed3.asInstanceOf[PFAFixed].size should be (5)
    fixed3.asInstanceOf[PFAFixed].bytes.toList should be (List(104, 101, 108, 108, 111))
  }

  it must "load fixed as PFAEnumSymbol, not an unnamed Enum" taggedAs(Data) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: {type: enum, name: TestEnum, symbols: [zero, one, two, three, four, five]}
action:
  - {type: TestEnum, value: "two"}
""").head
    val enum = engine.action(java.lang.Double.valueOf(0))

    enum.getClass.getName should be ("TestEnum")
    enum.asInstanceOf[PFAEnumSymbol].numSymbols should be (6)

    enum.asInstanceOf[PFAEnumSymbol].intToStr(0) should be ("zero")
    enum.asInstanceOf[PFAEnumSymbol].intToStr(1) should be ("one")
    enum.asInstanceOf[PFAEnumSymbol].intToStr(2) should be ("two")
    enum.asInstanceOf[PFAEnumSymbol].intToStr(3) should be ("three")
    enum.asInstanceOf[PFAEnumSymbol].intToStr(4) should be ("four")
    enum.asInstanceOf[PFAEnumSymbol].intToStr(5) should be ("five")

    enum.asInstanceOf[PFAEnumSymbol].strToInt("zero") should be (0)
    enum.asInstanceOf[PFAEnumSymbol].strToInt("one") should be (1)
    enum.asInstanceOf[PFAEnumSymbol].strToInt("two") should be (2)
    enum.asInstanceOf[PFAEnumSymbol].strToInt("three") should be (3)
    enum.asInstanceOf[PFAEnumSymbol].strToInt("four") should be (4)
    enum.asInstanceOf[PFAEnumSymbol].strToInt("five") should be (5)

    val enumSchema = AvroEnum(List("zero", "one", "two", "three", "four", "five"), "TestEnum")
    writeToJson(enum, enumSchema) should be (""""two"""")

    val engine2 = PFAEngine.fromYaml("""
input: double
output: {type: enum, name: TestEnum, namespace: com.wowie, symbols: [zero, one, two, three, four, five]}
action:
  - {type: com.wowie.TestEnum, value: "two"}
""").head
    val enum2 = engine2.action(java.lang.Double.valueOf(0))

    enum2.getClass.getName should be ("com.wowie.TestEnum")
    enum2.asInstanceOf[PFAEnumSymbol].numSymbols should be (6)

    enum2.asInstanceOf[PFAEnumSymbol].intToStr(0) should be ("zero")
    enum2.asInstanceOf[PFAEnumSymbol].intToStr(1) should be ("one")
    enum2.asInstanceOf[PFAEnumSymbol].intToStr(2) should be ("two")
    enum2.asInstanceOf[PFAEnumSymbol].intToStr(3) should be ("three")
    enum2.asInstanceOf[PFAEnumSymbol].intToStr(4) should be ("four")
    enum2.asInstanceOf[PFAEnumSymbol].intToStr(5) should be ("five")

    enum2.asInstanceOf[PFAEnumSymbol].strToInt("zero") should be (0)
    enum2.asInstanceOf[PFAEnumSymbol].strToInt("one") should be (1)
    enum2.asInstanceOf[PFAEnumSymbol].strToInt("two") should be (2)
    enum2.asInstanceOf[PFAEnumSymbol].strToInt("three") should be (3)
    enum2.asInstanceOf[PFAEnumSymbol].strToInt("four") should be (4)
    enum2.asInstanceOf[PFAEnumSymbol].strToInt("five") should be (5)

    val enumSchema2 = AvroEnum(List("zero", "one", "two", "three", "four", "five"), "TestEnum", Some("com.wowie"))
    writeToJson(enum2, enumSchema2) should be (""""two"""")

    val ssd = new PFASpecificData(engine2.classLoader)
    val enum3 = loadFromAvro(writeToAvro(enum2, enumSchema2, ssd), enumSchema2, ssd)
    enum3.getClass.getName should be ("com.wowie.TestEnum")
    enum3.asInstanceOf[PFAEnumSymbol].numSymbols should be (6)

    enum3.asInstanceOf[PFAEnumSymbol].intToStr(0) should be ("zero")
    enum3.asInstanceOf[PFAEnumSymbol].intToStr(1) should be ("one")
    enum3.asInstanceOf[PFAEnumSymbol].intToStr(2) should be ("two")
    enum3.asInstanceOf[PFAEnumSymbol].intToStr(3) should be ("three")
    enum3.asInstanceOf[PFAEnumSymbol].intToStr(4) should be ("four")
    enum3.asInstanceOf[PFAEnumSymbol].intToStr(5) should be ("five")

    enum3.asInstanceOf[PFAEnumSymbol].strToInt("zero") should be (0)
    enum3.asInstanceOf[PFAEnumSymbol].strToInt("one") should be (1)
    enum3.asInstanceOf[PFAEnumSymbol].strToInt("two") should be (2)
    enum3.asInstanceOf[PFAEnumSymbol].strToInt("three") should be (3)
    enum3.asInstanceOf[PFAEnumSymbol].strToInt("four") should be (4)
    enum3.asInstanceOf[PFAEnumSymbol].strToInt("five") should be (5)
  }

  it must "load record as PFARecord, not SpecificRecordBase" taggedAs(Data) in {
    val engine = PFAEngine.fromYaml("""
input: double
output: {type: record, name: TestRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
action:
  - {type: TestRecord, value: {"one": 1, "two": 2.2, "three": "THREE"}}
""").head
    val record = engine.action(java.lang.Double.valueOf(0))

    record.getClass.getName should be ("TestRecord")
    record.asInstanceOf[PFARecord].numFields should be (3)
    record.asInstanceOf[PFARecord].fieldNames.toList should be (List("one", "two", "three"))

    record.asInstanceOf[PFARecord].get(0) should be (1)
    record.asInstanceOf[PFARecord].get(1) should be (2.2)
    record.asInstanceOf[PFARecord].get(2) should be ("THREE")
    record.asInstanceOf[PFARecord].get("one") should be (1)
    record.asInstanceOf[PFARecord].get("two") should be (2.2)
    record.asInstanceOf[PFARecord].get("three") should be ("THREE")

    record.asInstanceOf[PFARecord].put(0, 100)
    record.asInstanceOf[PFARecord].put(1, 22.2)
    record.asInstanceOf[PFARecord].put(2, "trois")
    record.asInstanceOf[PFARecord].get(0) should be (100)
    record.asInstanceOf[PFARecord].get(1) should be (22.2)
    record.asInstanceOf[PFARecord].get(2) should be ("trois")

    record.asInstanceOf[PFARecord].put("one", -100)
    record.asInstanceOf[PFARecord].put("two", -22.2)
    record.asInstanceOf[PFARecord].put("three", "tres")
    record.asInstanceOf[PFARecord].get(0) should be (-100)
    record.asInstanceOf[PFARecord].get(1) should be (-22.2)
    record.asInstanceOf[PFARecord].get(2) should be ("tres")

    val engine2 = PFAEngine.fromYaml("""
input: double
output: {type: record, name: TestRecord, namespace: com.wowie, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
action:
  - {type: com.wowie.TestRecord, value: {"one": 1, "two": 2.2, "three": "THREE"}}
""").head
    val record2 = engine2.action(java.lang.Double.valueOf(0))

    record2.getClass.getName should be ("com.wowie.TestRecord")
    record2.asInstanceOf[PFARecord].numFields should be (3)
    record2.asInstanceOf[PFARecord].fieldNames.toList should be (List("one", "two", "three"))

    record2.asInstanceOf[PFARecord].get(0) should be (1)
    record2.asInstanceOf[PFARecord].get(1) should be (2.2)
    record2.asInstanceOf[PFARecord].get(2) should be ("THREE")
    record2.asInstanceOf[PFARecord].get("one") should be (1)
    record2.asInstanceOf[PFARecord].get("two") should be (2.2)
    record2.asInstanceOf[PFARecord].get("three") should be ("THREE")

    record2.asInstanceOf[PFARecord].put(0, 100)
    record2.asInstanceOf[PFARecord].put(1, 22.2)
    record2.asInstanceOf[PFARecord].put(2, "trois")
    record2.asInstanceOf[PFARecord].get(0) should be (100)
    record2.asInstanceOf[PFARecord].get(1) should be (22.2)
    record2.asInstanceOf[PFARecord].get(2) should be ("trois")

    record2.asInstanceOf[PFARecord].put("one", -100)
    record2.asInstanceOf[PFARecord].put("two", -22.2)
    record2.asInstanceOf[PFARecord].put("three", "tres")
    record2.asInstanceOf[PFARecord].get(0) should be (-100)
    record2.asInstanceOf[PFARecord].get(1) should be (-22.2)
    record2.asInstanceOf[PFARecord].get(2) should be ("tres")

    val recordSchema2 = AvroRecord(List(AvroField("one", AvroInt()), AvroField("two", AvroDouble()), AvroField("three", AvroString())), "TestRecord", Some("com.wowie"))
    writeToJson(record2, recordSchema2) should be ("""{"one":-100,"two":-22.2,"three":"tres"}""")

    val ssd = new PFASpecificData(engine2.classLoader)
    val record3 = loadFromAvro(writeToAvro(record2, recordSchema2, ssd), recordSchema2, ssd)
    record3.getClass.getName should be ("com.wowie.TestRecord")
    record3.asInstanceOf[PFARecord].numFields should be (3)
    record3.asInstanceOf[PFARecord].fieldNames.toList should be (List("one", "two", "three"))

    record2.asInstanceOf[PFARecord].get(0) should be (-100)
    record2.asInstanceOf[PFARecord].get(1) should be (-22.2)
    record2.asInstanceOf[PFARecord].get(2) should be ("tres")
    record2.asInstanceOf[PFARecord].get("one") should be (-100)
    record2.asInstanceOf[PFARecord].get("two") should be (-22.2)
    record2.asInstanceOf[PFARecord].get("three") should be ("tres")
  }
}
