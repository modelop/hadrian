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

package com.opendatagroup.hadrian.lib1

import scala.collection.JavaConversions._
import scala.language.postfixOps

import org.apache.avro.Schema
import org.codehaus.jackson.map.ObjectMapper

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema
import com.opendatagroup.hadrian.jvmcompiler.javaType
import com.opendatagroup.hadrian.jvmcompiler.PFAEngineBase

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAMap
import com.opendatagroup.hadrian.data.ComparisonOperatorLT

import com.opendatagroup.hadrian.signature.P
import com.opendatagroup.hadrian.signature.Sig
import com.opendatagroup.hadrian.signature.Signature
import com.opendatagroup.hadrian.signature.Sigs

import com.opendatagroup.hadrian.datatype.Type
import com.opendatagroup.hadrian.datatype.FcnType
import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.datatype.AvroInt
import com.opendatagroup.hadrian.datatype.AvroLong
import com.opendatagroup.hadrian.datatype.AvroFloat
import com.opendatagroup.hadrian.datatype.AvroDouble
import com.opendatagroup.hadrian.datatype.AvroArray
import com.opendatagroup.hadrian.datatype.AvroMap

package object map {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "map."

  def sideEffectFree(exceptions: String = "") = <detail>Note: <p>m</p> is not changed in-place; this is a side-effect-free function{exceptions}.</detail>
  val orderNotGuaranteed = <detail>The order in which <p>fcn</p> is called on items in <p>m</p> is not guaranteed, though it will be called exactly once for each value.</detail>

  def box(x: Any): AnyRef = x match {
    case y: Boolean => java.lang.Boolean.valueOf(y)
    case y: Int => java.lang.Integer.valueOf(y)
    case y: Long => java.lang.Long.valueOf(y)
    case y: Float => java.lang.Float.valueOf(y)
    case y: Double => java.lang.Double.valueOf(y)
    case y: AnyRef => y
  }

  def unbox(x: AnyRef): Any = x match {
    case y: java.lang.Boolean => y.booleanValue
    case y: java.lang.Integer => y.intValue
    case y: java.lang.Long => y.longValue
    case y: java.lang.Float => y.floatValue
    case y: java.lang.Double => y.doubleValue
    case y: AnyRef => y
  }

  trait ObjKey {
    val objectMapper = new ObjectMapper

    def toKey(x: AnyRef, pfaEngineBase: PFAEngineBase, schema: Schema): String =
      objectMapper.convertValue(pfaEngineBase.toAvro(x, schema), classOf[String])

    def fromKey(key: String, pfaEngineBase: PFAEngineBase, schema: Schema): AnyRef =
      pfaEngineBase.fromAvro(objectMapper.convertValue(key, classOf[Array[Byte]]), schema)
  }

  //////////////////////////////////////////////////////////////////// basic access

  ////   len (Len)
  object Len extends LibFcn {
    val name = prefix + "len"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A"))), P.Int)
    val doc =
      <doc>
        <desc>Return the length of a map.</desc>
      </doc>
    def apply[X <: AnyRef](m: PFAMap[X]): Int = m.toMap.size
  }
  provide(Len)

  ////   keys (Keys)
  object Keys extends LibFcn {
    val name = prefix + "keys"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A"))), P.Array(P.String))
    val doc =
      <doc>
        <desc>Return the keys of a map (in no particular order).</desc>
      </doc>
    def apply[X <: AnyRef](m: PFAMap[X]): PFAArray[String] = PFAArray.fromVector(m.toMap.keys.toVector)
  }
  provide(Keys)

  ////   values (Values)
  object Values extends LibFcn {
    val name = prefix + "values"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return the values of a map (in no particular order).</desc>
      </doc>
    def apply[X <: AnyRef, Y](m: PFAMap[X]): PFAArray[Y] = PFAArray.fromVector(m.toMap.values.map(unbox).asInstanceOf[Seq[Y]].toVector)
  }
  provide(Values)

  //////////////////////////////////////////////////////////////////// searching

  ////   containsKey (ContainsKey)
  object ContainsKey extends LibFcn {
    val name = prefix + "containsKey"
    val sig = Sigs(List(Sig(List("m" -> P.Map(P.Wildcard("A")), "key" -> P.String), P.Boolean),
                        Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.String), P.Boolean)), P.Boolean)))
    val doc =
      <doc>
        <desc>Return <c>true</c> if the keys of <p>m</p> contains <p>key</p> or <p>fcn</p> evaluates to <c>true</c> for some key of <p>m</p>, <c>false</c> otherwise.</desc>
      </doc>
    def apply[X <: AnyRef](m: PFAMap[X], key: String): Boolean =
      m.toMap.contains(key)
    def apply[X <: AnyRef](m: PFAMap[X], fcn: (String => Boolean)): Boolean =
      m.toMap.keys.find(fcn) != None
  }
  provide(ContainsKey)

  ////   containsValue (ContainsValue)
  object ContainsValue extends LibFcn {
    val name = prefix + "containsValue"
    val sig = Sigs(List(Sig(List("m" -> P.Map(P.Wildcard("A")), "value" -> P.Wildcard("A")), P.Boolean),
                        Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Boolean)))
    val doc =
      <doc>
        <desc>Return <c>true</c> if the values of <p>m</p> contains <p>value</p> or <p>fcn</p> evaluates to <c>true</c> for some key of <p>m</p>, <c>false</c> otherwise.</desc>
      </doc>
    def apply[X <: AnyRef](m: PFAMap[X], value: X): Boolean =
      m.toMap.values.find(x => x == value) != None
    def apply[X <: AnyRef](m: PFAMap[X], fcn: (X => Boolean)): Boolean =
      m.toMap.values.find(fcn) != None
  }
  provide(ContainsValue)

  //////////////////////////////////////////////////////////////////// manipulation

  ////   add (Add)
  object Add extends LibFcn with ObjKey {
    val name = prefix + "add"
    val sig = Sigs(List(Sig(List("m" -> P.Map(P.Wildcard("A")), "key" -> P.String, "value" -> P.Wildcard("A")), P.Map(P.Wildcard("A"))),
                        Sig(List("m" -> P.Map(P.Wildcard("A")), "item" -> P.Wildcard("A")), P.Map(P.Wildcard("A")))))
    val doc =
      <doc>
        <desc>Return a new map by adding the <p>key</p> <p>value</p> pair to <p>m</p> or a new set by adding the <p>item</p> to set <p>m</p>, where a set is represented as a map from serialized objects to objects.</desc>{sideEffectFree()}
        <detail>If <p>key</p> is in <p>m</p>, its value will be replaced.</detail>
        <detail>The serialization format for keys of sets is base64-encoded Avro.</detail>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      if (args.size == 3)
        JavaCode("%s.apply(%s)", javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString, wrapArgs(args, paramTypes, true))
      else
        JavaCode("%s.MODULE$.applySet(%s, %s, thisEngineBase, %s)",
          this.getClass.getName,
          wrapArg(0, args, paramTypes, true),
          wrapArg(1, args, paramTypes, true),
          javaSchema(paramTypes.head.asInstanceOf[AvroMap].values, false))

    def applySet[X <: AnyRef](m: PFAMap[X], item: X, pfaEngineBase: PFAEngineBase, schema: Schema): PFAMap[AnyRef] =
      PFAMap.fromMap(m.toMap.updated(toKey(box(item), pfaEngineBase, schema), item))

    def apply[X <: AnyRef](m: PFAMap[X], key: String, value: X): PFAMap[X] =
      PFAMap.fromMap(m.toMap.updated(key, value))
  }
  provide(Add)

  ////   remove (Remove)
  object Remove extends LibFcn {
    val name = prefix + "remove"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "key" -> P.String), P.Map(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return a new map by removing <p>key</p> from <p>m</p>.</desc>{sideEffectFree()}
        <detail>If <p>key</p> is not in <p>m</p>, the return value is simply <p>m</p>.</detail>
      </doc>
    def apply[X <: AnyRef](m: PFAMap[X], key: String): PFAMap[X] =
      PFAMap.fromMap(m.toMap - key)
  }
  provide(Remove)

  ////   only (Only)
  object Only extends LibFcn {
    val name = prefix + "only"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "keys" -> P.Array(P.String)), P.Map(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return a new map, keeping only <p>keys</p> from <p>m</p>.</desc>{sideEffectFree()}
        <detail>If some <p>keys</p> are not in <p>m</p>, they are ignored and do not appear in the return value.</detail>
      </doc>
    def apply[X <: AnyRef](m: PFAMap[X], keys: PFAArray[String]): PFAMap[X] = {
      val map = m.toMap
      PFAMap.fromMap(keys.toVector collect {case (x: String) if (map.contains(x)) => (x, map(x))} toMap)
    }
  }
  provide(Only)

  ////   except (Except)
  object Except extends LibFcn {
    val name = prefix + "except"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "keys" -> P.Array(P.String)), P.Map(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return a new map, keeping all but <p>keys</p> from <p>m</p>.</desc>{sideEffectFree()}
        <detail>If some <p>keys</p> are not in <p>m</p>, they are ignored and do not appear in the return value.</detail>
      </doc>
    def apply[X <: AnyRef](m: PFAMap[X], keys: PFAArray[String]): PFAMap[X] = {
      val vector = keys.toVector
      PFAMap.fromMap(m.toMap.filterKeys(x => !vector.contains(x)))
    }
  }
  provide(Except)

  ////   update (Update)
  object Update extends LibFcn {
    val name = prefix + "update"
    val sig = Sig(List("base" -> P.Map(P.Wildcard("A")), "overlay" -> P.Map(P.Wildcard("A"))), P.Map(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return a new map with key-value pairs from <p>overlay</p> in place of or in addition to key-value pairs from <p>base</p>.</desc>{sideEffectFree()}
        <detail>Keys of <p>overlay</p> that are not in <p>base</p> are added to those in <p>base</p> and keys of <p>overlay</p> that are in <p>base</p> supersede those in <p>base</p>.</detail>
      </doc>
    def apply[X <: AnyRef](base: PFAMap[X], overlay: PFAMap[X]): PFAMap[X] =
      PFAMap.fromMap(base.toMap ++ overlay.toMap)
  }
  provide(Update)

  ////   split (Split)
  object Split extends LibFcn {
    val name = prefix + "split"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A"))), P.Array(P.Map(P.Wildcard("A"))))
    val doc =
      <doc>
        <desc>Split the map into an array of maps, each containing only one key-value pair.</desc>
      </doc>
    def apply[X <: AnyRef](m: PFAMap[X]): PFAArray[PFAMap[X]] = PFAArray.fromVector(m.toMap map {case (k, v) => PFAMap.fromMap(Map(k -> v))} toVector)
  }
  provide(Split)

  ////   join (Join)
  object Join extends LibFcn {
    val name = prefix + "join"
    val sig = Sig(List("a" -> P.Array(P.Map(P.Wildcard("A")))), P.Map(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Join an array of maps into one map, overlaying from left to right.</desc>
      </doc>
    def apply[X <: AnyRef](a: PFAArray[PFAMap[X]]) = PFAMap.fromMap(a.toVector map {_.toMap} reduce {(x, y) => x ++ y})
  }
  provide(Join)

  //////////////////////////////////////////////////////////////////// set or set-like functions

  ////   toset (ToSet)
  object ToSet extends LibFcn with ObjKey {
    val name = prefix + "toset"
    val sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Map(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Convert an array of objects into a set of objects, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("%s.MODULE$.apply(%s, thisEngineBase, %s)",
        this.getClass.getName,
        wrapArg(0, args, paramTypes, true),
        javaSchema(paramTypes.head.asInstanceOf[AvroArray].items, false))
    def apply[X](a: PFAArray[X], pfaEngineBase: PFAEngineBase, schema: Schema): PFAMap[AnyRef] =
      PFAMap.fromMap(a.toVector map {x => val y = box(x); (toKey(y, pfaEngineBase, schema), y)} toMap)
  }
  provide(ToSet)

  ////   fromset (FromSet)
  object FromSet extends LibFcn with ObjKey {
    val name = prefix + "fromset"
    val sig = Sig(List("s" -> P.Map(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Convert a set of objects into an array of objects, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only values, not keys.</detail>
      </doc>
    def apply[X <: AnyRef, Y](s: PFAMap[X]): PFAArray[Y] =
      PFAArray.fromVector(s.toMap.values.map(unbox).toVector.asInstanceOf[Vector[Y]])
  }
  provide(FromSet)

  ////   in (In)
  object In extends LibFcn with ObjKey {
    val name = prefix + "in"
    val sig = Sig(List("s" -> P.Map(P.Wildcard("A")), "x" -> P.Wildcard("A")), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is contained in set <p>s</p>, <c>false</c> otherwise, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("%s.MODULE$.apply(%s, %s, thisEngineBase, %s)",
        this.getClass.getName,
        wrapArg(0, args, paramTypes, true),
        wrapArg(1, args, paramTypes, true),
        javaSchema(paramTypes.head.asInstanceOf[AvroMap].values, false))
    def apply[X <: AnyRef](s: PFAMap[X], x: X, pfaEngineBase: PFAEngineBase, schema: Schema): Boolean =
      s.toMap.contains(toKey(box(x), pfaEngineBase, schema))
  }
  provide(In)

  ////   union (Union)
  object Union extends LibFcn {
    val name = prefix + "union"
    val sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("A"))), P.Map(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return the union of sets <p>a</p> and <p>b</p>, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    def apply[X <: AnyRef](a: PFAMap[X], b: PFAMap[X]): PFAMap[X] =
      PFAMap.fromMap(a.toMap ++ b.toMap)
  }
  provide(Union)

  ////   intersection (Intersection)
  object Intersection extends LibFcn {
    val name = prefix + "intersection"
    val sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("A"))), P.Map(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return the intersection of sets <p>a</p> and <p>b</p>, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    def apply[X <: AnyRef](a: PFAMap[X], b: PFAMap[X]): PFAMap[X] = {
      val other = b.toMap
      PFAMap.fromMap(a.toMap.filterKeys(other.contains(_)))
    }
  }
  provide(Intersection)

  ////   diff (Diff)
  object Diff extends LibFcn {
    val name = prefix + "diff"
    val sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("A"))), P.Map(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return the difference of sets <p>a</p> and <p>b</p>, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    def apply[X <: AnyRef](a: PFAMap[X], b: PFAMap[X]): PFAMap[X] = {
      val other = b.toMap
      PFAMap.fromMap(a.toMap.filterKeys(!other.contains(_)))
    }
  }
  provide(Diff)

  ////   symdiff (SymDiff)
  object SymDiff extends LibFcn {
    val name = prefix + "symdiff"
    val sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("A"))), P.Map(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Return the difference of sets <p>a</p> and <p>b</p>, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    def apply[X <: AnyRef](a: PFAMap[X], b: PFAMap[X]): PFAMap[X] = {
      val aa = a.toMap.keys.toSet
      val bb = b.toMap.keys.toSet
      val cc = (aa diff bb) union (bb diff aa)
      PFAMap.fromMap((a.toMap ++ b.toMap).toMap.filterKeys(cc.contains(_)))
    }
  }
  provide(SymDiff)

  ////   subset (Subset)
  object Subset extends LibFcn {
    val name = prefix + "subset"
    val sig = Sig(List("little" -> P.Map(P.Wildcard("A")), "big" -> P.Map(P.Wildcard("A"))), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if set <p>little</p> is a subset of set <p>big</p>, <c>false</c> otherwise, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    def apply[X <: AnyRef](little: PFAMap[X], big: PFAMap[X]): Boolean = {
      val biggg = big.toMap
      little.toMap.keys.forall(biggg.contains(_))
    }
  }
  provide(Subset)

  ////   disjoint (Disjoint)
  object Disjoint extends LibFcn {
    val name = prefix + "disjoint"
    val sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("A"))), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if set <p>a</p> and set <p>b</p> are disjoint, <c>false</c> otherwise, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    def apply[X <: AnyRef](a: PFAMap[X], b: PFAMap[X]): Boolean =
      (a.toMap.keys.toSet intersect b.toMap.keys.toSet).isEmpty
  }
  provide(Disjoint)

  //////////////////////////////////////////////////////////////////// functional programming

  ////   map (MapApply)
  object MapApply extends LibFcn {
    val name = prefix + "map"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Wildcard("B"))), P.Map(P.Wildcard("B")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each value of <p>m</p> and return a map of transformed values (keys are unchanged).</desc>{orderNotGuaranteed}
        <detail>To transform both keys and values, consider applying <f>map.split</f>, <f>a.map</f>, then <f>map.join</f>.</detail>
      </doc>
    def apply[X <: AnyRef, Y <: AnyRef, Z](m: PFAMap[X], fcn: (X => Z)): PFAMap[Y] =
      PFAMap.fromMap(m.toMap map {case (k, v) => (k, box(fcn(v)).asInstanceOf[Y])})
  }
  provide(MapApply)

  ////   mapWithKey (MapWithKey)
  object MapWithKey extends LibFcn {
    val name = prefix + "mapWithKey"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.String, P.Wildcard("A")), P.Wildcard("B"))), P.Map(P.Wildcard("B")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each key, value pair of <p>m</p> and return a map of transformed values (keys are unchanged).</desc>{orderNotGuaranteed}
        <detail>To transform both keys and values, consider applying <f>map.split</f>, <f>a.map</f>, then <f>map.join</f>.</detail>
      </doc>
    def apply[X <: AnyRef, Y <: AnyRef, Z](m: PFAMap[X], fcn: ((String, X) => Z)): PFAMap[Y] =
      PFAMap.fromMap(m.toMap map {case (k, v) => (k, box(fcn(k, v)).asInstanceOf[Y])})
  }
  provide(MapWithKey)

  ////   filter (Filter)
  object Filter extends LibFcn {
    val name = prefix + "filter"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Map(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each value of <p>m</p> and return a map of the values for which <p>fcn</p> returns <c>true</c> (keys are unchanged).</desc>{orderNotGuaranteed}
      </doc>
    def apply[X <: AnyRef](m: PFAMap[X], fcn: (X => Boolean)): PFAMap[X] =
      PFAMap.fromMap(m.toMap filter {case (k, v) => fcn(v)})
  }
  provide(Filter)

  ////   filterWithKey (FilterWithKey)
  object FilterWithKey extends LibFcn {
    val name = prefix + "filterWithKey"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.String, P.Wildcard("A")), P.Boolean)), P.Map(P.Wildcard("A")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each value of <p>m</p> and return a map of the values for which <p>fcn</p> returns <c>true</c> (keys are unchanged).</desc>{orderNotGuaranteed}
      </doc>
    def apply[X <: AnyRef](m: PFAMap[X], fcn: ((String, X) => Boolean)): PFAMap[X] =
      PFAMap.fromMap(m.toMap filter {case (k, v) => fcn(k, v)})
  }
  provide(FilterWithKey)

  ////   filterMap (FilterMap)
  object FilterMap extends LibFcn {
    val name = prefix + "filterMap"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Union(List(P.Wildcard("B"), P.Null)))), P.Map(P.Wildcard("B")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each value of <p>m</p> and return a map of the results that are not <c>null</c>.</desc>{orderNotGuaranteed}
      </doc>
    def apply[X <: AnyRef, Y <: AnyRef](m: PFAMap[X], fcn: (X => Y)): PFAMap[Y] = {
      val builder = Map.newBuilder[String, Y]
      builder.sizeHint(m.toMap.size)
      for ((key, value) <- m.toMap)
        fcn(value) match {
          case null =>
          case x => builder += (key -> x)
        }
      PFAMap.fromMap(builder.result)
    }
  }
  provide(FilterMap)

  ////   filterMapWithKey (FilterMapWithKey)
  object FilterMapWithKey extends LibFcn {
    val name = prefix + "filterMapWithKey"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.String, P.Wildcard("A")), P.Union(List(P.Wildcard("B"), P.Null)))), P.Map(P.Wildcard("B")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each key-value pair of <p>m</p> and return a map of the results that are not <c>null</c>.</desc>{orderNotGuaranteed}
      </doc>
    def apply[X <: AnyRef, Y <: AnyRef](m: PFAMap[X], fcn: ((String, X) => Y)): PFAMap[Y] = {
      val builder = Map.newBuilder[String, Y]
      builder.sizeHint(m.toMap.size)
      for ((key, value) <- m.toMap)
        fcn(key, value) match {
          case null =>
          case x => builder += (key -> x)
        }
      PFAMap.fromMap(builder.result)
    }
  }
  provide(FilterMapWithKey)

  ////   flatMapWithKey (FlatMapWithKey)
  object FlatMapWithKey extends LibFcn {
    val name = prefix + "flatMapWithKey"
    val sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.String, P.Wildcard("A")), P.Map(P.Wildcard("B")))), P.Map(P.Wildcard("B")))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each key-value pair of <p>m</p> and return a map of overlaid results.</desc>{orderNotGuaranteed}
      </doc>
    def apply[X <: AnyRef, Y <: AnyRef](m: PFAMap[X], fcn: ((String, X) => PFAMap[Y])): PFAMap[Y] = {
      val builder = Map.newBuilder[String, Y]
      builder.sizeHint(m.toMap.size)
      for ((key, value) <- m.toMap)
        for ((k, v) <- fcn(key, value).toMap)
          builder += (k -> v)
      PFAMap.fromMap(builder.result)
    }
  }
  provide(FlatMapWithKey)

  //////////////////////////////////////////////////////////////////// functional tests

  ////   corresponds (Corresponds)
  object Corresponds extends LibFcn {
    val name = prefix + "corresponds"
    val sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("B")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B")), P.Boolean)), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>fcn</p> is <c>true</c> when applied to all pairs of values, one from <p>a</p> and the other from <p>b</p> (logical relation).</desc>
        <detail>The number of <p>fcn</p> calls is not guaranteed.</detail>
        <detail>If the key sets of <p>a</p> and <p>b</p> are not equal, this function returns <c>false</c>.</detail>
      </doc>
    def apply[X <: AnyRef, Y <: AnyRef](a: PFAMap[X], b: PFAMap[Y], fcn: (X, Y) => Boolean): Boolean = {
      val amap = a.toMap
      val bmap = b.toMap
      val aset = amap.keySet
      val bset = bmap.keySet
      if (aset != bset)
        false
      else
        aset forall {k => fcn(amap(k), bmap(k))}
    }
  }
  provide(Corresponds)

  ////   correspondsWithKey (CorrespondsWithKey)
  object CorrespondsWithKey extends LibFcn {
    val name = prefix + "correspondsWithKey"
    val sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("B")), "fcn" -> P.Fcn(List(P.String, P.Wildcard("A"), P.Wildcard("B")), P.Boolean)), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>fcn</p> is <c>true</c> when applied to all triples of key, value from <p>a</p>, value from <p>b</p> (logical relation).</desc>
        <detail>The number of <p>fcn</p> calls is not guaranteed.</detail>
        <detail>If the key sets of <p>a</p> and <p>b</p> are not equal, this function returns <c>false</c>.</detail>
      </doc>
    def apply[X <: AnyRef, Y <: AnyRef](a: PFAMap[X], b: PFAMap[Y], fcn: (String, X, Y) => Boolean): Boolean = {
      val amap = a.toMap
      val bmap = b.toMap
      val aset = amap.keySet
      val bset = bmap.keySet
      if (aset != bset)
        false
      else
        aset forall {k => fcn(k, amap(k), bmap(k))}
    }
  }
  provide(CorrespondsWithKey)

}
