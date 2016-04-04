// Copyright (C) 2014  Open Data ("Open Data" refers to
// one or more of the following companies: Open Data Partners LLC,
// Open Data Research LLC, or Open Data Capital LLC.)
// 
// This file is part of Hadrian.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.opendatagroup.hadrian.lib

import scala.collection.JavaConversions._
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.language.postfixOps

import org.apache.avro.Schema
import org.codehaus.jackson.map.ObjectMapper

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema
import com.opendatagroup.hadrian.jvmcompiler.javaType
import com.opendatagroup.hadrian.jvmcompiler.PFAEngineBase
import com.opendatagroup.hadrian.options.EngineOptions

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
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "map."

  def sideEffectFree(exceptions: String = "") = <detail>Note: <p>m</p> is not changed in-place; this is a side-effect-free function{exceptions}.</detail>
  val orderNotGuaranteed = <detail>The order in which <p>fcn</p> is called on items in <p>m</p> is not guaranteed, though it will be called exactly once for each value.</detail>
  def errorOnEmpty(code: Int) = <error code={s"${code}"}>If <p>m</p> is empty, an "empty map" runtime error is raised.</error>
  val sortsUnique = <detail>If any values are not unique, their keys will be returned in lexicographic order.</detail>
  def errorOnNegativeN(code: Int) = <error code={s"${code}"}>If <p>n</p> is negative, an {""""n < 0""""} runtime error is raised.</error>

  def box(x: Any): AnyRef = x match {
    case null => null
    case y: Boolean => java.lang.Boolean.valueOf(y)
    case y: Int => java.lang.Integer.valueOf(y)
    case y: Long => java.lang.Long.valueOf(y)
    case y: Float => java.lang.Float.valueOf(y)
    case y: Double => java.lang.Double.valueOf(y)
    case y: AnyRef => y
  }

  def unbox(x: AnyRef): Any = x match {
    case null => null
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
  class Len(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "len"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A"))), P.Int)
    def doc =
      <doc>
        <desc>Return the length of a map.</desc>
      </doc>
    def errcodeBase = 26000
    def apply[X <: AnyRef](m: PFAMap[X]): Int = m.toMap.size
  }
  provide(new Len)

  ////   keys (Keys)
  class Keys(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "keys"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A"))), P.Array(P.String))
    def doc =
      <doc>
        <desc>Return the keys of a map (in no particular order).</desc>
        <nondeterministic type="unordered" />
      </doc>
    def errcodeBase = 26010
    def apply[X <: AnyRef](m: PFAMap[X]): PFAArray[String] = PFAArray.fromVector(m.toMap.keys.toVector)
  }
  provide(new Keys)

  ////   values (Values)
  class Values(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "values"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return the values of a map (in no particular order).</desc>
        <nondeterministic type="unordered" />
      </doc>
    def errcodeBase = 26020
    def apply(m: AnyRef): PFAArray[_] = apply(m.asInstanceOf[PFAMap[AnyRef]])
    def apply[X <: AnyRef](m: PFAMap[X]): PFAArray[_] = {
      val out = m.toMap.values.toVector
      if (out.isEmpty)
        PFAArray.fromVector(out)
      else if (out.forall(_.isInstanceOf[java.lang.Boolean]))
        PFAArray.fromVector(out.map(_.asInstanceOf[java.lang.Boolean].booleanValue))
      else if (out.forall(_.isInstanceOf[java.lang.Integer]))
        PFAArray.fromVector(out.map(_.asInstanceOf[java.lang.Integer].intValue))
      else if (out.forall(_.isInstanceOf[java.lang.Long]))
        PFAArray.fromVector(out.map(_.asInstanceOf[java.lang.Long].longValue))
      else if (out.forall(_.isInstanceOf[java.lang.Float]))
        PFAArray.fromVector(out.map(_.asInstanceOf[java.lang.Float].floatValue))
      else if (out.forall(_.isInstanceOf[java.lang.Double]))
        PFAArray.fromVector(out.map(_.asInstanceOf[java.lang.Double].doubleValue))
      else
        PFAArray.fromVector(out)
    }
  }
  provide(new Values)

  //////////////////////////////////////////////////////////////////// searching

  ////   containsKey (ContainsKey)
  class ContainsKey(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "containsKey"
    def sig = Sigs(List(Sig(List("m" -> P.Map(P.Wildcard("A")), "key" -> P.String), P.Boolean),
                        Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.String), P.Boolean)), P.Boolean)))
    def doc =
      <doc>
        <desc>Return <c>true</c> if the keys of <p>m</p> contains <p>key</p> or <p>fcn</p> evaluates to <c>true</c> for some key of <p>m</p>, <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 26030
    def apply[X <: AnyRef](m: PFAMap[X], key: String): Boolean =
      m.toMap.contains(key)
    def apply[X <: AnyRef](m: PFAMap[X], fcn: (String => Boolean)): Boolean =
      m.toMap.keys.find(fcn) != None
  }
  provide(new ContainsKey)

  ////   containsValue (ContainsValue)
  class ContainsValue(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "containsValue"
    def sig = Sigs(List(Sig(List("m" -> P.Map(P.Wildcard("A")), "value" -> P.Wildcard("A")), P.Boolean),
                        Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Boolean)))
    def doc =
      <doc>
        <desc>Return <c>true</c> if the values of <p>m</p> contains <p>value</p> or <p>fcn</p> evaluates to <c>true</c> for some key of <p>m</p>, <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 26040
    def apply[X <: AnyRef](m: PFAMap[X], value: X): Boolean =
      m.toMap.values.find(x => x == value) != None
    def apply[X <: AnyRef](m: PFAMap[X], fcn: (X => Boolean)): Boolean =
      m.toMap.values.find(fcn) != None
  }
  provide(new ContainsValue)

  //////////////////////////////////////////////////////////////////// manipulation

  ////   add (Add)
  class Add(val pos: Option[String] = None) extends LibFcn with ObjKey {
    def name = prefix + "add"
    def sig = Sigs(List(Sig(List("m" -> P.Map(P.Wildcard("A")), "key" -> P.String, "value" -> P.Wildcard("A")), P.Map(P.Wildcard("A"))),
                        Sig(List("m" -> P.Map(P.Wildcard("A")), "item" -> P.Wildcard("A")), P.Map(P.Wildcard("A")))))
    def doc =
      <doc>
        <desc>Return a new map by adding the <p>key</p> <p>value</p> pair to <p>m</p> or a new set by adding the <p>item</p> to set <p>m</p>, where a set is represented as a map from serialized objects to objects.</desc>{sideEffectFree()}
        <detail>If <p>key</p> is in <p>m</p>, its value will be replaced.</detail>
        <detail>The serialization format for keys of sets is base64-encoded Avro.</detail>
      </doc>
    def errcodeBase = 26050
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      if (args.size == 3)
        JavaCode("%s.apply(%s)", javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString, wrapArgs(args, paramTypes, true))
      else
        JavaCode("%s.applySet(%s, %s, thisEngineBase, %s)",
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true),
          wrapArg(1, args, paramTypes, true),
          javaSchema(paramTypes.head.asInstanceOf[AvroMap].values, false))

    def applySet[X <: AnyRef](m: PFAMap[X], item: X, pfaEngineBase: PFAEngineBase, schema: Schema): PFAMap[AnyRef] =
      PFAMap.fromMap(m.toMap.updated(toKey(box(item), pfaEngineBase, schema), item))

    def apply[X <: AnyRef](m: PFAMap[X], key: String, value: X): PFAMap[X] =
      PFAMap.fromMap(m.toMap.updated(key, value))
  }
  provide(new Add)

  ////   remove (Remove)
  class Remove(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "remove"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "key" -> P.String), P.Map(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return a new map by removing <p>key</p> from <p>m</p>.</desc>{sideEffectFree()}
        <detail>If <p>key</p> is not in <p>m</p>, the return value is simply <p>m</p>.</detail>
      </doc>
    def errcodeBase = 26060
    def apply[X <: AnyRef](m: PFAMap[X], key: String): PFAMap[X] =
      PFAMap.fromMap(m.toMap - key)
  }
  provide(new Remove)

  ////   only (Only)
  class Only(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "only"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "keys" -> P.Array(P.String)), P.Map(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return a new map, keeping only <p>keys</p> from <p>m</p>.</desc>{sideEffectFree()}
        <detail>If some <p>keys</p> are not in <p>m</p>, they are ignored and do not appear in the return value.</detail>
      </doc>
    def errcodeBase = 26070
    def apply[X <: AnyRef](m: PFAMap[X], keys: PFAArray[String]): PFAMap[X] = {
      val map = m.toMap
      PFAMap.fromMap(keys.toVector collect {case (x: String) if (map.contains(x)) => (x, map(x))} toMap)
    }
  }
  provide(new Only)

  ////   except (Except)
  class Except(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "except"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "keys" -> P.Array(P.String)), P.Map(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return a new map, keeping all but <p>keys</p> from <p>m</p>.</desc>{sideEffectFree()}
        <detail>If some <p>keys</p> are not in <p>m</p>, they are ignored and do not appear in the return value.</detail>
      </doc>
    def errcodeBase = 26080
    def apply[X <: AnyRef](m: PFAMap[X], keys: PFAArray[String]): PFAMap[X] = {
      val vector = keys.toVector
      PFAMap.fromMap(m.toMap.filterKeys(x => !vector.contains(x)))
    }
  }
  provide(new Except)

  ////   update (Update)
  class Update(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "update"
    def sig = Sig(List("base" -> P.Map(P.Wildcard("A")), "overlay" -> P.Map(P.Wildcard("A"))), P.Map(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return a new map with key-value pairs from <p>overlay</p> in place of or in addition to key-value pairs from <p>base</p>.</desc>{sideEffectFree()}
        <detail>Keys of <p>overlay</p> that are not in <p>base</p> are added to those in <p>base</p> and keys of <p>overlay</p> that are in <p>base</p> supersede those in <p>base</p>.</detail>
      </doc>
    def errcodeBase = 26090
    def apply[X <: AnyRef](base: PFAMap[X], overlay: PFAMap[X]): PFAMap[X] =
      PFAMap.fromMap(base.toMap ++ overlay.toMap)
  }
  provide(new Update)

  ////   split (Split)
  class Split(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "split"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A"))), P.Array(P.Map(P.Wildcard("A"))))
    def doc =
      <doc>
        <desc>Split the map into an array of maps, each containing only one key-value pair (in no particular order).</desc>
        <nondeterministic type="unordered" />
      </doc>
    def errcodeBase = 26100
    def apply[X <: AnyRef](m: PFAMap[X]): PFAArray[PFAMap[X]] = PFAArray.fromVector(m.toMap map {case (k, v) => PFAMap.fromMap(Map(k -> v))} toVector)
  }
  provide(new Split)

  ////   join (Join)
  class Join(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "join"
    def sig = Sig(List("a" -> P.Array(P.Map(P.Wildcard("A")))), P.Map(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Join an array of maps into one map, overlaying from left to right.</desc>
      </doc>
    def errcodeBase = 26110
    def apply[X <: AnyRef](a: PFAArray[PFAMap[X]]) =
      if (a.toVector.isEmpty)
        PFAMap.fromMap(Map[String, X]())
      else
        PFAMap.fromMap(a.toVector map {_.toMap} reduce {(x, y) => x ++ y})
  }
  provide(new Join)

  //////////////////////////////////////////////////////////////////// min/max functions

  // these map-specific argHighestN and argLowestN functions can deal with keys arriving out of order, at the expense of one additional lt() call

  def argHighestN[X](m: Map[String, X], n: Int, lt: (X, X) => Boolean): Vector[String] = {
    var out = mutable.ListBuffer[(X, String)]()
    for ((k, x) <- m) {
      out.indexWhere({case (bestx, bestk) => lt(bestx, x)  ||  (!lt(x, bestx)  &&  bestk > k)}) match {
        case -1 => out.append((x, k))
        case index => {
          var ind = index
          while (ind < out.size  &&  !lt(out(ind)._1, x)  &&  !lt(x, out(ind)._1)  &&  out(ind)._2 < k)
            ind += 1
          out.insert(ind, (x, k))
        }
      }
      if (out.size > n)
        out = out.init
    }
    out map {_._2} toVector
  }

  def argLowestN[X](m: Map[String, X], n: Int, lt: (X, X) => Boolean): Vector[String] = {
    var out = mutable.ListBuffer[(X, String)]()
    for ((k, x) <- m) {
      out.indexWhere({case (bestx, bestk) => lt(x, bestx)  ||  (!lt(bestx, x)  &&  bestk > k)}) match {
        case -1 => out.append((x, k))
        case index => {
          var ind = index
          while (ind < out.size  &&  !lt(out(ind)._1, x)  &&  !lt(x, out(ind)._1)  &&  out(ind)._2 < k)
            ind += 1
          out.insert(ind, (x, k))
        }
      }
      if (out.size > n)
        out = out.init
    }
    out map {_._2} toVector
  }

  ////   argmax (Argmax)
  object Argmax {
    def name = prefix + "argmax"
    def errcodeBase = 26120
    class MaxOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](m: PFAMap[X]): String =
        if (m.toMap.isEmpty)
          throw new PFARuntimeException("empty map", errcodeBase + 0, name, pos)
        else
          argHighestN(m.toMap, 1, comparisonOperatorLT).head
    }
  }
  class Argmax(val pos: Option[String] = None) extends LibFcn {
    def name = Argmax.name
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A"))), P.String)
    def doc =
      <doc>
        <desc>Return the key of the highest value in <p>m</p> (as defined by Avro's sort order).</desc>
        {sortsUnique}{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = Argmax.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroMap(x) =>
        JavaCode("(new " + this.getClass.getName + "$MaxOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new Argmax)

  ////   argmin (Argmin)
  object Argmin {
    def name = prefix + "argmin"
    def errcodeBase = 26130
    class MinOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](m: PFAMap[X]): String =
        if (m.toMap.isEmpty)
          throw new PFARuntimeException("empty map", errcodeBase + 0, name, pos)
        else
          argLowestN(m.toMap, 1, comparisonOperatorLT).head
    }
  }
  class Argmin(val pos: Option[String] = None) extends LibFcn {
    def name = Argmin.name
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A"))), P.String)
    def doc =
      <doc>
        <desc>Return the key of the lowest value in <p>m</p> (as defined by Avro's sort order).</desc>
        {sortsUnique}{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = Argmin.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroMap(x) =>
        JavaCode("(new " + this.getClass.getName + "$MinOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new Argmin)

  ////   argmaxLT (ArgmaxLT)
  class ArgmaxLT(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "argmaxLT"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.String)
    def doc =
      <doc>
        <desc>Return the key of the highest value in <p>m</p> as defined by the <p>lessThan</p> function.</desc>
        {sortsUnique}{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = 26140
    def apply[X <: AnyRef](m: PFAMap[X], lt: (X, X) => Boolean): String =
      if (m.toMap.isEmpty)
        throw new PFARuntimeException("empty map", errcodeBase + 0, name, pos)
      else
        argHighestN(m.toMap, 1, lt).head
  }
  provide(new ArgmaxLT)

  ////   argminLT (ArgminLT)
  class ArgminLT(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "argminLT"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.String)
    def doc =
      <doc>
        <desc>Return the key of the lowest value in <p>m</p> as defined by the <p>lessThan</p> function.</desc>
        {sortsUnique}{errorOnEmpty(errcodeBase + 0)}
      </doc>
    def errcodeBase = 26150
    def apply[X <: AnyRef](m: PFAMap[X], lt: (X, X) => Boolean): String =
      if (m.toMap.isEmpty)
        throw new PFARuntimeException("empty map", errcodeBase + 0, name, pos)
      else
        argLowestN(m.toMap, 1, lt).head
  }
  provide(new ArgminLT)

  ////   argmaxN (ArgmaxN)
  object ArgmaxN {
    def name = prefix + "argmaxN"
    def errcodeBase = 26160
    class MaxOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](m: PFAMap[X], n: Int): PFAArray[String] =
        if (m.toMap.isEmpty)
          throw new PFARuntimeException("empty map", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(argHighestN(m.toMap, n, comparisonOperatorLT))
    }
  }
  class ArgmaxN(val pos: Option[String] = None) extends LibFcn {
    def name = ArgmaxN.name
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "n" -> P.Int), P.Array(P.String))
    def doc =
      <doc>
        <desc>Return the keys of the <p>n</p> highest values in <p>m</p> (as defined by Avro's sort order).</desc>
        {sortsUnique}{errorOnEmpty(errcodeBase + 0)}{errorOnNegativeN(errcodeBase + 1)}
      </doc>
    def errcodeBase = ArgmaxN.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroMap(x) =>
        JavaCode("(new " + this.getClass.getName + "$MaxOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new ArgmaxN)

  ////   argminN (ArgminN)
  object ArgminN {
    def name = prefix + "argminN"
    def errcodeBase = 26170
    class MinOther(comparisonOperatorLT: ComparisonOperatorLT, pos: Option[String]) {
      def apply[X <: AnyRef](m: PFAMap[X], n: Int): PFAArray[String] =
        if (m.toMap.isEmpty)
          throw new PFARuntimeException("empty map", errcodeBase + 0, name, pos)
        else if (n < 0)
          throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
        else
          PFAArray.fromVector(argLowestN(m.toMap, n, comparisonOperatorLT))
    }
  }
  class ArgminN(val pos: Option[String] = None) extends LibFcn {
    def name = ArgminN.name
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "n" -> P.Int), P.Array(P.String))
    def doc =
      <doc>
        <desc>Return the keys of the <p>n</p> lowest values in <p>m</p> (as defined by Avro's sort order).</desc>
        {sortsUnique}{errorOnEmpty(errcodeBase + 0)}{errorOnNegativeN(errcodeBase + 1)}
      </doc>
    def errcodeBase = ArgminN.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case AvroMap(x) =>
        JavaCode("(new " + this.getClass.getName + "$MinOther(new com.opendatagroup.hadrian.data.ComparisonOperatorLT(%s), " + posToJava + "))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new ArgminN)

  ////   argmaxNLT (ArgmaxNLT)
  class ArgmaxNLT(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "argmaxNLT"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "n" -> P.Int, "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Array(P.String))
    def doc =
      <doc>
        <desc>Return the keys of the <p>n</p> highest values in <p>a</p> as defined by the <p>lessThan</p> function.</desc>
        {sortsUnique}{errorOnEmpty(errcodeBase + 0)}{errorOnNegativeN(errcodeBase + 1)}
      </doc>
    def errcodeBase = 26180
    def apply[X <: AnyRef](m: PFAMap[X], n: Int, lt: (X, X) => Boolean): PFAArray[String] =
      if (m.toMap.isEmpty)
        throw new PFARuntimeException("empty map", errcodeBase + 0, name, pos)
      else if (n < 0)
        throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
      else
        PFAArray.fromVector(argHighestN(m.toMap, n, lt))
  }
  provide(new ArgmaxNLT)

  ////   argminNLT (ArgminNLT)
  class ArgminNLT(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "argminNLT"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "n" -> P.Int, "lessThan" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("A")), P.Boolean)), P.Array(P.String))
    def doc =
      <doc>
        <desc>Return the keys of the <p>n</p> highest values in <p>a</p> as defined by the <p>lessThan</p> function.</desc>
        {sortsUnique}{errorOnEmpty(errcodeBase + 0)}{errorOnNegativeN(errcodeBase + 1)}
      </doc>
    def errcodeBase = 26190
    def apply[X <: AnyRef](m: PFAMap[X], n: Int, lt: (X, X) => Boolean): PFAArray[String] =
      if (m.toMap.isEmpty)
        throw new PFARuntimeException("empty map", errcodeBase + 0, name, pos)
      else if (n < 0)
        throw new PFARuntimeException("n < 0", errcodeBase + 1, name, pos)
      else
        PFAArray.fromVector(argLowestN(m.toMap, n, lt))
  }
  provide(new ArgminNLT)

  //////////////////////////////////////////////////////////////////// set or set-like functions

  ////   toset (ToSet)
  class ToSet(val pos: Option[String] = None) extends LibFcn with ObjKey {
    def name = prefix + "toset"
    def sig = Sig(List("a" -> P.Array(P.Wildcard("A"))), P.Map(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Convert an array of objects into a set of objects, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
      </doc>
    def errcodeBase = 26200
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("%s.apply(%s, thisEngineBase, %s)",
        javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
        wrapArg(0, args, paramTypes, true),
        javaSchema(paramTypes.head.asInstanceOf[AvroArray].items, false))
    def apply[X](a: PFAArray[X], pfaEngineBase: PFAEngineBase, schema: Schema): PFAMap[AnyRef] =
      PFAMap.fromMap(a.toVector map {x => val y = box(x); (toKey(y, pfaEngineBase, schema), y)} toMap)
  }
  provide(new ToSet)

  ////   fromset (FromSet)
  class FromSet(val pos: Option[String] = None) extends LibFcn with ObjKey {
    def name = prefix + "fromset"
    def sig = Sig(List("s" -> P.Map(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Convert a set of objects into an array of objects (in no particular order), where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only values, not keys.</detail>
        <nondeterministic type="unordered" />
      </doc>
    def errcodeBase = 26210
    def apply[X <: AnyRef, Y](s: PFAMap[X]): PFAArray[Y] =
      PFAArray.fromVector(s.toMap.values.map(unbox).toVector.asInstanceOf[Vector[Y]])
  }
  provide(new FromSet)

  ////   in (In)
  class In(val pos: Option[String] = None) extends LibFcn with ObjKey {
    def name = prefix + "in"
    def sig = Sig(List("s" -> P.Map(P.Wildcard("A")), "x" -> P.Wildcard("A")), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is contained in set <p>s</p>, <c>false</c> otherwise, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    def errcodeBase = 26220
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("%s.apply(%s, %s, thisEngineBase, %s)",
        javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
        wrapArg(0, args, paramTypes, true),
        wrapArg(1, args, paramTypes, true),
        javaSchema(paramTypes.head.asInstanceOf[AvroMap].values, false))
    def apply[X <: AnyRef](s: PFAMap[X], x: X, pfaEngineBase: PFAEngineBase, schema: Schema): Boolean =
      s.toMap.contains(toKey(box(x), pfaEngineBase, schema))
  }
  provide(new In)

  ////   union (Union)
  class Union(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "union"
    def sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("A"))), P.Map(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return the union of sets <p>a</p> and <p>b</p>, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    def errcodeBase = 26230
    def apply[X <: AnyRef](a: PFAMap[X], b: PFAMap[X]): PFAMap[X] =
      PFAMap.fromMap(a.toMap ++ b.toMap)
  }
  provide(new Union)

  ////   intersection (Intersection)
  class Intersection(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "intersection"
    def sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("A"))), P.Map(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return the intersection of sets <p>a</p> and <p>b</p>, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    def errcodeBase = 26240
    def apply[X <: AnyRef](a: PFAMap[X], b: PFAMap[X]): PFAMap[X] = {
      val other = b.toMap
      PFAMap.fromMap(a.toMap.filterKeys(other.contains(_)))
    }
  }
  provide(new Intersection)

  ////   diff (Diff)
  class Diff(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "diff"
    def sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("A"))), P.Map(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return the difference of sets <p>a</p> and <p>b</p>, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    def errcodeBase = 26250
    def apply[X <: AnyRef](a: PFAMap[X], b: PFAMap[X]): PFAMap[X] = {
      val other = b.toMap
      PFAMap.fromMap(a.toMap.filterKeys(!other.contains(_)))
    }
  }
  provide(new Diff)

  ////   symdiff (SymDiff)
  class SymDiff(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "symdiff"
    def sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("A"))), P.Map(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return the difference of sets <p>a</p> and <p>b</p>, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    def errcodeBase = 26260
    def apply[X <: AnyRef](a: PFAMap[X], b: PFAMap[X]): PFAMap[X] = {
      val aa = a.toMap.keys.toSet
      val bb = b.toMap.keys.toSet
      val cc = (aa diff bb) union (bb diff aa)
      PFAMap.fromMap((a.toMap ++ b.toMap).toMap.filterKeys(cc.contains(_)))
    }
  }
  provide(new SymDiff)

  ////   subset (Subset)
  class Subset(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "subset"
    def sig = Sig(List("little" -> P.Map(P.Wildcard("A")), "big" -> P.Map(P.Wildcard("A"))), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if set <p>little</p> is a subset of set <p>big</p>, <c>false</c> otherwise, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    def errcodeBase = 26270
    def apply[X <: AnyRef](little: PFAMap[X], big: PFAMap[X]): Boolean = {
      val biggg = big.toMap
      little.toMap.keys.forall(biggg.contains(_))
    }
  }
  provide(new Subset)

  ////   disjoint (Disjoint)
  class Disjoint(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "disjoint"
    def sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("A"))), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if set <p>a</p> and set <p>b</p> are disjoint, <c>false</c> otherwise, where a set is represented as a map from serialized objects to objects.</desc>
        <detail>The serialization format is base64-encoded Avro.</detail>
        <detail>This function does not verify that the serialized objects (keys) and objects (values) match: it considers only keys, not values.</detail>
      </doc>
    def errcodeBase = 26280
    def apply[X <: AnyRef](a: PFAMap[X], b: PFAMap[X]): Boolean =
      (a.toMap.keys.toSet intersect b.toMap.keys.toSet).isEmpty
  }
  provide(new Disjoint)

  //////////////////////////////////////////////////////////////////// functional programming

  ////   map (MapApply)
  class MapApply(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "map"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Wildcard("B"))), P.Map(P.Wildcard("B")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each value of <p>m</p> and return a map of transformed values (keys are unchanged).</desc>{orderNotGuaranteed}
        <detail>To transform both keys and values, consider applying <f>map.split</f>, <f>a.map</f>, then <f>map.join</f>.</detail>
      </doc>
    def errcodeBase = 26290
    def apply[X <: AnyRef, Y <: AnyRef, Z](m: PFAMap[X], fcn: (X => Z)): PFAMap[Y] =
      PFAMap.fromMap(m.toMap map {case (k, v) => (k, box(fcn(v)).asInstanceOf[Y])})
  }
  provide(new MapApply)

  ////   mapWithKey (MapWithKey)
  class MapWithKey(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "mapWithKey"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.String, P.Wildcard("A")), P.Wildcard("B"))), P.Map(P.Wildcard("B")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each key, value pair of <p>m</p> and return a map of transformed values (keys are unchanged).</desc>{orderNotGuaranteed}
        <detail>To transform both keys and values, consider applying <f>map.split</f>, <f>a.map</f>, then <f>map.join</f>.</detail>
      </doc>
    def errcodeBase = 26300
    def apply[X <: AnyRef, Y <: AnyRef, Z](m: PFAMap[X], fcn: ((String, X) => Z)): PFAMap[Y] =
      PFAMap.fromMap(m.toMap map {case (k, v) => (k, box(fcn(k, v)).asInstanceOf[Y])})
  }
  provide(new MapWithKey)

  ////   filter (Filter)
  class Filter(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "filter"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Boolean)), P.Map(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each value of <p>m</p> and return a map of the values for which <p>fcn</p> returns <c>true</c> (keys are unchanged).</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 26310
    def apply[X <: AnyRef](m: PFAMap[X], fcn: (X => Boolean)): PFAMap[X] =
      PFAMap.fromMap(m.toMap filter {case (k, v) => fcn(v)})
  }
  provide(new Filter)

  ////   filterWithKey (FilterWithKey)
  class FilterWithKey(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "filterWithKey"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.String, P.Wildcard("A")), P.Boolean)), P.Map(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each value of <p>m</p> and return a map of the values for which <p>fcn</p> returns <c>true</c> (keys are unchanged).</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 26320
    def apply[X <: AnyRef](m: PFAMap[X], fcn: ((String, X) => Boolean)): PFAMap[X] =
      PFAMap.fromMap(m.toMap filter {case (k, v) => fcn(k, v)})
  }
  provide(new FilterWithKey)

  ////   filterMap (FilterMap)
  class FilterMap(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "filterMap"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Union(List(P.Wildcard("B"), P.Null)))), P.Map(P.Wildcard("B")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each value of <p>m</p> and return a map of the results that are not <c>null</c>.</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 26330
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
  provide(new FilterMap)

  ////   filterMapWithKey (FilterMapWithKey)
  class FilterMapWithKey(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "filterMapWithKey"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.String, P.Wildcard("A")), P.Union(List(P.Wildcard("B"), P.Null)))), P.Map(P.Wildcard("B")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each key-value pair of <p>m</p> and return a map of the results that are not <c>null</c>.</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 26340
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
  provide(new FilterMapWithKey)

  ////   flatMap (FlatMap)
  class FlatMap(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "flatMap"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.Wildcard("A")), P.Map(P.Wildcard("B")))), P.Map(P.Wildcard("B")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each value of <p>m</p> and return a map of overlaid results.</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 26350
    def apply[X <: AnyRef, Y <: AnyRef](m: PFAMap[X], fcn: X => PFAMap[Y]): PFAMap[Y] = {
      val builder = Map.newBuilder[String, Y]
      builder.sizeHint(m.toMap.size)
      for ((key, value) <- m.toMap)
        for ((k, v) <- fcn(value).toMap)
          builder += (k -> v)
      PFAMap.fromMap(builder.result)
    }
  }
  provide(new FlatMap)

  ////   flatMapWithKey (FlatMapWithKey)
  class FlatMapWithKey(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "flatMapWithKey"
    def sig = Sig(List("m" -> P.Map(P.Wildcard("A")), "fcn" -> P.Fcn(List(P.String, P.Wildcard("A")), P.Map(P.Wildcard("B")))), P.Map(P.Wildcard("B")))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each key-value pair of <p>m</p> and return a map of overlaid results.</desc>{orderNotGuaranteed}
      </doc>
    def errcodeBase = 26360
    def apply[X <: AnyRef, Y <: AnyRef](m: PFAMap[X], fcn: ((String, X) => PFAMap[Y])): PFAMap[Y] = {
      val builder = Map.newBuilder[String, Y]
      builder.sizeHint(m.toMap.size)
      for ((key, value) <- m.toMap)
        for ((k, v) <- fcn(key, value).toMap)
          builder += (k -> v)
      PFAMap.fromMap(builder.result)
    }
  }
  provide(new FlatMapWithKey)

  ////   zipmap (ZipMap)
  class ZipMap(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "zipmap"
    def sig = Sigs(List(Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("B")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B")), P.Wildcard("Z"))), P.Map(P.Wildcard("Z"))),
                        Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("B")), "c" -> P.Map(P.Wildcard("C")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C")), P.Wildcard("Z"))), P.Map(P.Wildcard("Z"))),
                        Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("B")), "c" -> P.Map(P.Wildcard("C")), "d" -> P.Map(P.Wildcard("D")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C"), P.Wildcard("D")), P.Wildcard("Z"))), P.Map(P.Wildcard("Z")))))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to the elements of <p>a</p>, <p>b</p>, <p>c</p>, <p>d</p> in lock-step and return a result for row.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises a "misaligned maps" error if <p>a</p>, <p>b</p>, <p>c</p>, <p>d</p> do not all have the same keys.</error>
      </doc>
    def errcodeBase = 26370
    def apply[A <: AnyRef, B <: AnyRef, Z <: AnyRef](a: PFAMap[A], b: PFAMap[B], fcn: (A, B) => Z): PFAMap[Z] = {
      val aa = a.toMap
      val bb = b.toMap
      if (aa.keySet != bb.keySet)
        throw new PFARuntimeException("misaligned maps", errcodeBase + 0, name, pos)
      PFAMap.fromMap(aa.keys map {k => (k, fcn(aa(k), bb(k)))} toMap)
    }
    def apply[A <: AnyRef, B <: AnyRef, C <: AnyRef, Z <: AnyRef](a: PFAMap[A], b: PFAMap[B], c: PFAMap[C], fcn: (A, B, C) => Z): PFAMap[Z] = {
      val aa = a.toMap
      val bb = b.toMap
      val cc = c.toMap
      if (aa.keySet != bb.keySet  ||  bb.keySet != cc.keySet)
        throw new PFARuntimeException("misaligned maps", errcodeBase + 0, name, pos)
      PFAMap.fromMap(aa.keys map {k => (k, fcn(aa(k), bb(k), cc(k)))} toMap)
    }
    def apply[A <: AnyRef, B <: AnyRef, C <: AnyRef, D <: AnyRef, Z <: AnyRef](a: PFAMap[A], b: PFAMap[B], c: PFAMap[C], d: PFAMap[D], fcn: (A, B, C, D) => Z): PFAMap[Z] = {
      val aa = a.toMap
      val bb = b.toMap
      val cc = c.toMap
      val dd = d.toMap
      if (aa.keySet != bb.keySet  ||  bb.keySet != cc.keySet  ||  cc.keySet != dd.keySet)
        throw new PFARuntimeException("misaligned maps", errcodeBase + 0, name, pos)
      PFAMap.fromMap(aa.keys map {k => (k, fcn(aa(k), bb(k), cc(k), dd(k)))} toMap)
    }
  }
  provide(new ZipMap)

  ////   zipmapWithKey (ZipMapWithKey)
  class ZipMapWithKey(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "zipmapWithKey"
    def sig = Sigs(List(Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("B")), "fcn" -> P.Fcn(List(P.String, P.Wildcard("A"), P.Wildcard("B")), P.Wildcard("Z"))), P.Map(P.Wildcard("Z"))),
                        Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("B")), "c" -> P.Map(P.Wildcard("C")), "fcn" -> P.Fcn(List(P.String, P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C")), P.Wildcard("Z"))), P.Map(P.Wildcard("Z"))),
                        Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("B")), "c" -> P.Map(P.Wildcard("C")), "d" -> P.Map(P.Wildcard("D")), "fcn" -> P.Fcn(List(P.String, P.Wildcard("A"), P.Wildcard("B"), P.Wildcard("C"), P.Wildcard("D")), P.Wildcard("Z"))), P.Map(P.Wildcard("Z")))))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to the keys and elements of <p>a</p>, <p>b</p>, <p>c</p>, <p>d</p> in lock-step and return a result for row.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises a "misaligned maps" error if <p>a</p>, <p>b</p>, <p>c</p>, <p>d</p> do not all have the same keys.</error>
      </doc>
    def errcodeBase = 26380
    def apply[A <: AnyRef, B <: AnyRef, Z <: AnyRef](a: PFAMap[A], b: PFAMap[B], fcn: (String, A, B) => Z): PFAMap[Z] = {
      val aa = a.toMap
      val bb = b.toMap
      if (aa.keySet != bb.keySet)
        throw new PFARuntimeException("misaligned maps", errcodeBase + 0, name, pos)
      PFAMap.fromMap(aa.keys map {k => (k, fcn(k, aa(k), bb(k)))} toMap)
    }
    def apply[A <: AnyRef, B <: AnyRef, C <: AnyRef, Z <: AnyRef](a: PFAMap[A], b: PFAMap[B], c: PFAMap[C], fcn: (String, A, B, C) => Z): PFAMap[Z] = {
      val aa = a.toMap
      val bb = b.toMap
      val cc = c.toMap
      if (aa.keySet != bb.keySet  ||  bb.keySet != cc.keySet)
        throw new PFARuntimeException("misaligned maps", errcodeBase + 0, name, pos)
      PFAMap.fromMap(aa.keys map {k => (k, fcn(k, aa(k), bb(k), cc(k)))} toMap)
    }
    def apply[A <: AnyRef, B <: AnyRef, C <: AnyRef, D <: AnyRef, Z <: AnyRef](a: PFAMap[A], b: PFAMap[B], c: PFAMap[C], d: PFAMap[D], fcn: (String, A, B, C, D) => Z): PFAMap[Z] = {
      val aa = a.toMap
      val bb = b.toMap
      val cc = c.toMap
      val dd = d.toMap
      if (aa.keySet != bb.keySet  ||  bb.keySet != cc.keySet  ||  cc.keySet != dd.keySet)
        throw new PFARuntimeException("misaligned maps", errcodeBase + 0, name, pos)
      PFAMap.fromMap(aa.keys map {k => (k, fcn(k, aa(k), bb(k), cc(k), dd(k)))} toMap)
    }
  }
  provide(new ZipMapWithKey)

  //////////////////////////////////////////////////////////////////// functional tests

  ////   corresponds (Corresponds)
  class Corresponds(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "corresponds"
    def sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("B")), "fcn" -> P.Fcn(List(P.Wildcard("A"), P.Wildcard("B")), P.Boolean)), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>fcn</p> is <c>true</c> when applied to all pairs of values, one from <p>a</p> and the other from <p>b</p> (logical relation).</desc>
        <detail>The number of <p>fcn</p> calls is not guaranteed.</detail>
        <detail>If the key sets of <p>a</p> and <p>b</p> are not equal, this function returns <c>false</c>.</detail>
      </doc>
    def errcodeBase = 26390
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
  provide(new Corresponds)

  ////   correspondsWithKey (CorrespondsWithKey)
  class CorrespondsWithKey(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "correspondsWithKey"
    def sig = Sig(List("a" -> P.Map(P.Wildcard("A")), "b" -> P.Map(P.Wildcard("B")), "fcn" -> P.Fcn(List(P.String, P.Wildcard("A"), P.Wildcard("B")), P.Boolean)), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>fcn</p> is <c>true</c> when applied to all triples of key, value from <p>a</p>, value from <p>b</p> (logical relation).</desc>
        <detail>The number of <p>fcn</p> calls is not guaranteed.</detail>
        <detail>If the key sets of <p>a</p> and <p>b</p> are not equal, this function returns <c>false</c>.</detail>
      </doc>
    def errcodeBase = 26400
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
  provide(new CorrespondsWithKey)

}
