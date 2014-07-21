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

package com.opendatagroup.hadrian.lib1.model

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.collection.JavaConversions._

import org.apache.avro.AvroRuntimeException
import org.apache.avro.SchemaCompatibility.checkReaderWriterCompatibility
import org.apache.avro.SchemaCompatibility.SchemaCompatibilityType
import org.apache.avro.Schema

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema
import com.opendatagroup.hadrian.jvmcompiler.javaType
import com.opendatagroup.hadrian.jvmcompiler.JVMNameMangle

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFARecord
import com.opendatagroup.hadrian.data.PFAEnumSymbol
import com.opendatagroup.hadrian.data.ComparisonOperator

import com.opendatagroup.hadrian.signature.P
import com.opendatagroup.hadrian.signature.Sig
import com.opendatagroup.hadrian.signature.Signature
import com.opendatagroup.hadrian.signature.Sigs

import com.opendatagroup.hadrian.datatype.Type
import com.opendatagroup.hadrian.datatype.FcnType
import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.datatype.AvroNull
import com.opendatagroup.hadrian.datatype.AvroBoolean
import com.opendatagroup.hadrian.datatype.AvroInt
import com.opendatagroup.hadrian.datatype.AvroLong
import com.opendatagroup.hadrian.datatype.AvroFloat
import com.opendatagroup.hadrian.datatype.AvroDouble
import com.opendatagroup.hadrian.datatype.AvroBytes
import com.opendatagroup.hadrian.datatype.AvroFixed
import com.opendatagroup.hadrian.datatype.AvroString
import com.opendatagroup.hadrian.datatype.AvroEnum
import com.opendatagroup.hadrian.datatype.AvroArray
import com.opendatagroup.hadrian.datatype.AvroMap
import com.opendatagroup.hadrian.datatype.AvroRecord
import com.opendatagroup.hadrian.datatype.AvroField
import com.opendatagroup.hadrian.datatype.AvroUnion

package object tree {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "model.tree."

  //////////////////////////////////////////////////////////////////// 

  object SimpleComparison {
    def apply(datum: PFARecord, field: PFAEnumSymbol, operator: String, value: Any, fieldSchema: Schema, valueSchema: Schema, missingOperators: Boolean): Boolean = {
      if (operator == "alwaysTrue")
        true
      else if (operator == "alwaysFalse")
        false
      else if (missingOperators  &&  operator == "isMissing")
        datum.get(field.value) == null
      else if (missingOperators  &&  operator == "notMissing")
        datum.get(field.value) != null
      else if (operator == "in"  ||  operator == "notIn") {
        if (valueSchema.getType != Schema.Type.ARRAY  ||  checkReaderWriterCompatibility(valueSchema.getElementType, fieldSchema).getType != SchemaCompatibilityType.COMPATIBLE)
          throw new PFARuntimeException("bad value type")
        val vector = value.asInstanceOf[PFAArray[Any]].toVector

        val fieldValue = (datum.get(field.value), valueSchema.getElementType.getType) match {
          case (x: java.lang.Number, Schema.Type.INT) => x.intValue
          case (x: java.lang.Number, Schema.Type.LONG) => x.longValue
          case (x: java.lang.Number, Schema.Type.FLOAT) => x.floatValue
          case (x: java.lang.Number, Schema.Type.DOUBLE) => x.doubleValue
          case (x, _) => x
        }

        val containedInVector = vector.contains(fieldValue)

        if (operator == "in")
          containedInVector
        else
          !containedInVector
      }

      else {
        fieldSchema.getType match {
          case Schema.Type.INT | Schema.Type.LONG | Schema.Type.FLOAT | Schema.Type.DOUBLE => {
            val fieldNumber = datum.get(field.value).asInstanceOf[java.lang.Number].doubleValue
            val valueNumber = value match {
              case null => throw new PFARuntimeException("bad value type")
              case x: Int => x.toDouble
              case x: Long => x.toDouble
              case x: Float => x.toDouble
              case x: Double => x
              case x: java.lang.Number => x.doubleValue
              case _ => throw new PFARuntimeException("bad value type")
            }

            operator match {
              case "<=" => fieldNumber <= valueNumber
              case "<" => fieldNumber < valueNumber
              case ">=" => fieldNumber >= valueNumber
              case ">" => fieldNumber > valueNumber
              case "==" => fieldNumber == valueNumber
              case "!=" => fieldNumber != valueNumber
              case _ => throw new PFARuntimeException("invalid comparison operator")
            }
          }

          case Schema.Type.STRING => {
            val fieldString = datum.get(field.value).asInstanceOf[String]
            val valueString = value match {
              case null => throw new PFARuntimeException("bad value type")
              case x: String => x
              case _ => throw new PFARuntimeException("bad value type")
            }

            operator match {
              case "==" => fieldString == valueString
              case "!=" => fieldString != valueString
              case "<=" => (new ComparisonOperator(valueSchema, -3)).apply(fieldString, valueString)
              case "<" => (new ComparisonOperator(valueSchema, -2)).apply(fieldString, valueString)
              case ">=" => (new ComparisonOperator(valueSchema, 2)).apply(fieldString, valueString)
              case ">" => (new ComparisonOperator(valueSchema, 3)).apply(fieldString, valueString)
              case _ => throw new PFARuntimeException("invalid comparison operator")
            }
          }

          case _ => {
            if (checkReaderWriterCompatibility(valueSchema, fieldSchema).getType != SchemaCompatibilityType.COMPATIBLE)
              throw new PFARuntimeException("bad value type")

            val fieldObject = (datum.get(field.value), valueSchema.getType) match {
              case (x: java.lang.Integer, Schema.Type.INT) => x
              case (x: java.lang.Integer, Schema.Type.LONG) => java.lang.Long.valueOf(x.longValue)
              case (x: java.lang.Integer, Schema.Type.FLOAT) => java.lang.Float.valueOf(x.floatValue)
              case (x: java.lang.Integer, Schema.Type.DOUBLE) => java.lang.Double.valueOf(x.doubleValue)
              case (x: java.lang.Long, Schema.Type.LONG) => x
              case (x: java.lang.Long, Schema.Type.FLOAT) => java.lang.Float.valueOf(x.floatValue)
              case (x: java.lang.Long, Schema.Type.DOUBLE) => java.lang.Double.valueOf(x.doubleValue)
              case (x: java.lang.Float, Schema.Type.FLOAT) => x
              case (x: java.lang.Float, Schema.Type.DOUBLE) => java.lang.Double.valueOf(x.doubleValue)
              case (x: java.lang.Double, Schema.Type.DOUBLE) => x
              case (x, _) => x
            }

            operator match {
              case "<=" => (new ComparisonOperator(valueSchema, -3)).apply(fieldObject, value.asInstanceOf[AnyRef])
              case "<" => (new ComparisonOperator(valueSchema, -2)).apply(fieldObject, value.asInstanceOf[AnyRef])
              case "!=" => (new ComparisonOperator(valueSchema, -1)).apply(fieldObject, value.asInstanceOf[AnyRef])
              case "==" => (new ComparisonOperator(valueSchema, 1)).apply(fieldObject, value.asInstanceOf[AnyRef])
              case ">=" => (new ComparisonOperator(valueSchema, 2)).apply(fieldObject, value.asInstanceOf[AnyRef])
              case ">" => (new ComparisonOperator(valueSchema, 3)).apply(fieldObject, value.asInstanceOf[AnyRef])
              case _ => throw new PFARuntimeException("invalid comparison operator")
            }
          }
        }
      }
    }

    def removeNull(schema: Schema): Schema = schema.getType match {
      case Schema.Type.UNION => {
        val withoutNull = schema.getTypes filter {_.getType != Schema.Type.NULL}
        if (withoutNull.isEmpty)
          schema
        else if (withoutNull.size == 1)
          withoutNull.head
        else
          Schema.createUnion(withoutNull)
      }
      case _ => schema
    }
  }

  ////   simpleTest (SimpleTest)
  object SimpleTest extends LibFcn {
    val name = prefix + "simpleTest"
    val sig = Sig(List("datum" -> P.WildRecord("D", Map()), "comparison" -> P.WildRecord("T", ListMap("field" -> P.EnumFields("F", "D"), "operator" -> P.String, "value" -> P.Wildcard("V")))), P.Boolean)
    val doc =
      <doc>
        <desc>Determine if <p>datum</p> passes a test defined by <p>comparison</p>.</desc>
        <param name="datum">Sample value to test.</param>
        <param name="comparison">Record that describes a test.
          <paramField name="field">Field name from <p>datum</p>: the enumeration type must include all fields of <tp>D</tp> in their declaration order.</paramField>
          <paramField name="operator">One of the following: "==" (equal), "!=" (not equal), "&lt;" (less than), "&lt;=" (less or equal), "&gt;" (greater than), "&gt;=" (greater or equal), "in" (member of a set), "notIn" (not a member of a set), "alwaysTrue" (ignore <pf>value</pf>, return <c>true</c>), "alwaysFalse" (ignore <pf>value</pf>, return <c>false</c>), "isMissing" (ignore <pf>value</pf>, return <c>true</c> iff the field of <p>datum</p> is <c>null</c>), and "notMissing" (ignore <pf>value</pf>, return <c>false</c> iff the field of <p>datum</p> is <c>null</c>).</paramField>
          <paramField name="value">Value to which the field of <p>datum</p> is compared.</paramField>
        </param>
        <ret>Returns <c>true</c> if the field of <p>datum</p> &lt;op&gt; <pf>value</pf> is <c>true</c>, <c>false</c> otherwise, where &lt;op&gt; is the <pf>operator</pf>.</ret>
        <error>Raises an "invalid comparison operator" if <pf>operator</pf> is not one of "==", "!=", "&lt;", "&lt;=", "&gt;", "&gt;=", "in", "notIn", "alwaysTrue", "alwaysFalse", "isMissing", "notMissing".</error>
        <error>Raises a "bad value type" if the <pf>field</pf> of <p>datum</p> and <tp>V</tp> are not both numbers and the <pf>field</pf> cannot be upcast to <tp>V</tp>.</error>
      </doc>

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("""(new Object() {
public boolean apply(com.opendatagroup.hadrian.data.PFARecord datum, %s comparison) {
com.opendatagroup.hadrian.data.PFAEnumSymbol field = (com.opendatagroup.hadrian.data.PFAEnumSymbol)(comparison.%s);
return com.opendatagroup.hadrian.lib1.model.tree.package$SimpleComparison$.MODULE$.apply(datum, field, comparison.%s, comparison.%s, datum.fieldTypes()[field.value()], comparison.fieldTypes()[comparison.fieldIndex("value")], true);
} }).apply((com.opendatagroup.hadrian.data.PFARecord)%s, (%s)%s)""",
        javaType(paramTypes(1).asInstanceOf[AvroRecord], true, true, false),
        JVMNameMangle.s("field"),
        JVMNameMangle.s("operator"),
        JVMNameMangle.s("value"),
        wrapArg(0, args, paramTypes, true),
        javaType(paramTypes(1).asInstanceOf[AvroRecord], true, true, false),
        wrapArg(1, args, paramTypes, true))

  }
  provide(SimpleTest)

  ////   missingTest (MissingTest)
  object MissingTest extends LibFcn {
    val name = prefix + "missingTest"
    val sig = Sig(List("datum" -> P.WildRecord("D", Map()), "comparison" -> P.WildRecord("T", ListMap("field" -> P.EnumFields("F", "D"), "operator" -> P.String, "value" -> P.Wildcard("V")))), P.Union(List(P.Null, P.Boolean)))
    val doc =
      <doc>
        <desc>Determine if <p>datum</p> passes a test defined by <p>comparison</p>, allowing for missing values.</desc>
        <param name="datum">Sample value to test.</param>
        <param name="comparison">Record that describes a test.
          <paramField name="field">Field name from <p>datum</p>: the enumeration type must include all fields of <tp>D</tp> in their declaration order.</paramField>
          <paramField name="operator">One of the following: "==" (equal), "!=" (not equal), "&lt;" (less than), "&lt;=" (less or equal), "&gt;" (greater than), "&gt;=" (greater or equal), "in" (member of a set), "notIn" (not a member of a set), "alwaysTrue" (ignore <pf>value</pf>, return <c>true</c>), "alwaysFalse" (ignore <pf>value</pf>, return <c>false</c>).</paramField>
          <paramField name="value">Value to which the field of <p>datum</p> is compared.</paramField>
        </param>
        <ret>If the field of <p>datum</p> is <c>null</c>, this function returns <c>null</c> (unknown test result).  Otherwise, it returns <p>datum</p> field &lt;op&gt; <pf>value</pf>, where &lt;op&gt; is the <pf>operator</pf></ret>
        <error>Raises an "invalid comparison operator" if <pf>operator</pf> is not one of "==", "!=", "&lt;", "&lt;=", "&gt;", "&gt;=", "in", "notIn", "alwaysTrue", "alwaysFalse".</error>
        <error>Raises a "bad value type" if the <pf>field</pf> of <p>datum</p> and <tp>V</tp> are not both numbers and the <pf>field</pf> cannot be upcast to <tp>V</tp>.</error>
      </doc>

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("""(new Object() {
public java.lang.Boolean apply(com.opendatagroup.hadrian.data.PFARecord datum, %s comparison) {
com.opendatagroup.hadrian.data.PFAEnumSymbol field = (com.opendatagroup.hadrian.data.PFAEnumSymbol)(comparison.%s);
if (datum.get(field.value()) == null)
    return null;
return java.lang.Boolean.valueOf(com.opendatagroup.hadrian.lib1.model.tree.package$SimpleComparison$.MODULE$.apply(datum, field, comparison.%s, comparison.%s, com.opendatagroup.hadrian.lib1.model.tree.package$SimpleComparison$.MODULE$.removeNull(datum.fieldTypes()[field.value()]), comparison.fieldTypes()[comparison.fieldIndex("value")], false));
} }).apply((com.opendatagroup.hadrian.data.PFARecord)%s, (%s)%s)""",
        javaType(paramTypes(1).asInstanceOf[AvroRecord], true, true, false),
        JVMNameMangle.s("field"),
        JVMNameMangle.s("operator"),
        JVMNameMangle.s("value"),
        wrapArg(0, args, paramTypes, true),
        javaType(paramTypes(1).asInstanceOf[AvroRecord], true, true, false),
        wrapArg(1, args, paramTypes, true))
  }
  provide(MissingTest)

  ////   surrogateTest (SurrogateTest)
  object SurrogateTest extends LibFcn {
    val name = prefix + "surrogateTest"
    val sig = Sig(List("datum" -> P.WildRecord("D", Map()), "comparisons" -> P.Array(P.WildRecord("T", Map())), "missingTest" -> P.Fcn(List(P.Wildcard("D"), P.Wildcard("T")), P.Union(List(P.Null, P.Boolean)))), P.Boolean)
    val doc =
      <doc>
        <desc>Apply <p>missingTest</p> to an array of <p>comparisons</p> until one yields a non-null result.</desc>
        <param name="datum">Sample value to test.</param>
        <param name="comparisons">Records that describe the tests.</param>
        <ret>Returns the value of the first test that returns <c>true</c> or <c>false</c>.</ret>
        <error>If all tests return <c>null</c>, this function raises a "no successful surrogate" error.</error>
      </doc>
    def apply(datum: PFARecord, comparisons: PFAArray[PFARecord], missingTest: (PFARecord, PFARecord) => java.lang.Boolean): Boolean = {
      var result = false
      if (comparisons.toVector.indexWhere(comparison =>
        missingTest(datum, comparison) match {
          case null => false
          case x: java.lang.Boolean => {
            result = x.booleanValue
            true
          }
        }) == -1)
        throw new PFARuntimeException("no successful surrogate")
      else
        result
    }
  }
  provide(SurrogateTest)

  ////   simpleWalk (SimpleWalk)
  object SimpleWalk extends LibFcn {
    val name = prefix + "simpleWalk"
    val sig = Sig(List(
      "datum" -> P.WildRecord("D", Map()),
      "treeNode" -> P.WildRecord("T", ListMap("pass" -> P.Union(List(P.WildRecord("T", Map()), P.Wildcard("S"))), "fail" -> P.Union(List(P.WildRecord("T", Map()), P.Wildcard("S"))))),
      "test" -> P.Fcn(List(P.Wildcard("D"), P.Wildcard("T")), P.Boolean)
    ), P.Wildcard("S"))
    val doc =
      <doc>
        <desc>Descend through a tree, testing the fields of <p>datum</p> with the <p>test</p> function using <p>treeNode</p> to define the comparison, continuing to <pf>pass</pf> or <pf>fail</pf> until reaching a leaf node of type <tp>S</tp> (score).</desc>
        <param name="datum">Sample value to test.</param>
        <param name="treeNode">Node of the tree, which contains a predicate to be interpreted by <p>test</p>.
          <paramField name="pass">Branch to follow if <p>test</p> returns <c>true</c>.</paramField>
          <paramField name="fail">Branch to follow if <p>test</p> returns <c>false</c>.</paramField>
        </param>
        <param name="test">Test function that converts <p>datum</p> and <p>treeNode</p> into <c>true</c> or <c>false</c>.</param>
        <ret>Leaf node of type <tp>S</tp>, which must be different from the tree nodes.  For a classification tree, <tp>S</tp> could be a string or an enumeration set.  For a regression tree, <tp>S</tp> would be a numerical type.  For a multivariate regression tree, <tp>S</tp> would be an array of numbers, etc.</ret>
      </doc>
    @tailrec
    def apply(datum: PFARecord, treeNode: PFARecord, test: (PFARecord, PFARecord) => Boolean): AnyRef = {
      val next =
        if (test(datum, treeNode))
          treeNode.get("pass")
        else
          treeNode.get("fail")
      next match {
        case x: PFARecord if (treeNode.getSchema.getFullName == x.getSchema.getFullName) => apply(datum, x, test)
        case x => x
      }
    }
  }
  provide(SimpleWalk)

}
