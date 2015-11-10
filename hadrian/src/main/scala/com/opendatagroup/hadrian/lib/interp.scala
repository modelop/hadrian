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

package com.opendatagroup.hadrian.lib

import scala.language.postfixOps
import scala.collection.immutable.ListMap

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAEnumSymbol
import com.opendatagroup.hadrian.data.PFAFixed
import com.opendatagroup.hadrian.data.PFAMap
import com.opendatagroup.hadrian.data.PFARecord

import com.opendatagroup.hadrian.jvmcompiler.javaType

import com.opendatagroup.hadrian.options.EngineOptions

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

package object interp {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "interp."

  ////   bin (Bin)
  class Bin(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "bin"
    def sig = Sigs(List(Sig(List("x" -> P.Double, "numbins" -> P.Int, "low" -> P.Double, "high" -> P.Double), P.Int),
                        Sig(List("x" -> P.Double, "origin" -> P.Double, "width" -> P.Double), P.Int)))
    def doc =
      <doc>
        <desc>Finds the bin that contains <p>x</p>, declared either as <p>numbins</p> between two endpoints or a bin <p>width</p> starting at some <p>origin</p>.</desc>
        <detail>Bins are inclusive on the low end and exclusive on the high end, so if <p>x</p> equal <p>low</p> or <p>origin</p>, the resulting bin is <c>0</c>, but if <p>x</p> is equal to <p>high</p>, it is out of range.</detail>
        <detail>If the first signature is used, the resulting bin must be between <c>0</c> (inclusive) and <p>numbins</p> (exclusive). If the second signature is used, the resulting bin may be any integer, including negative numbers.</detail>
        <error code={s"${errcodeBase + 0}"}>If <p>low</p> is greater or equal to <p>high</p> or <p>origin</p> is not finite, raises "bad histogram range"</error>
        <error code={s"${errcodeBase + 1}"}>If <p>numbins</p> is less than <c>1</c> or <p>width</p> is less or equal to <c>0</c>, raises "bad histogram scale"</error>
        <error code={s"${errcodeBase + 2}"}>Raises "x out of range" if <p>x</p> is less than <p>low</p> or greater or equal to <p>high</p>.</error>
      </doc>
    def errcodeBase = 22000
    def apply(x: Double, numbins: Int, low: Double, high: Double): Int = {
      if (low >= high  ||  java.lang.Double.isNaN(low)  ||  java.lang.Double.isNaN(high))
        throw new PFARuntimeException("bad histogram range", errcodeBase + 0, name, pos)
      if (numbins < 1)
        throw new PFARuntimeException("bad histogram scale", errcodeBase + 1, name, pos)
      if (java.lang.Double.isNaN(x)  ||  x < low  ||  x >= high)
        throw new PFARuntimeException("x out of range", errcodeBase + 2, name, pos)

      val out = Math.floor(numbins * (x - low)/(high - low)).toInt

      if (out < 0  ||  out >= numbins)
        throw new PFARuntimeException("x out of range", errcodeBase + 2, name, pos)
      out
    }
    def apply(x: Double, origin: Double, width: Double): Int = {
      if (java.lang.Double.isNaN(origin)  ||  java.lang.Double.isInfinite(origin))
        throw new PFARuntimeException("bad histogram range", errcodeBase + 0, name, pos)
      if (width <= 0.0  ||  java.lang.Double.isNaN(width))
        throw new PFARuntimeException("bad histogram scale", errcodeBase + 1, name, pos)
      if (java.lang.Double.isNaN(x)  ||  java.lang.Double.isInfinite(x))
        throw new PFARuntimeException("x out of range", errcodeBase + 2, name, pos)
      Math.floor((x - origin)/width).toInt
    }
  }
  provide(new Bin)

  ////   nearest (Nearest)
  class Nearest(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "nearest"
    def sig = Sigs(List(Sig(List("x" -> P.Double, "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Double, "to" -> P.Wildcard("T"))))), P.Wildcard("T")),
                        Sig(List("x" -> P.Array(P.Double), "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Array(P.Double), "to" -> P.Wildcard("T"))))), P.Wildcard("T")),
                        Sig(List("x" -> P.Wildcard("X1"), "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Wildcard("X2"), "to" -> P.Wildcard("T")))), "metric" -> P.Fcn(List(P.Wildcard("X1"), P.Wildcard("X2")), P.Double)), P.Wildcard("T"))))
    def doc =
      <doc>
        <desc>Finds the closest <pf>x</pf> value in the <p>table</p> to the input <p>x</p> and returns the corresponding <pf>to</pf> value.</desc>
        <detail>Any ties in distance are resolved in favor of the first instance in the <p>table</p>.</detail>
        <error code={s"${errcodeBase + 0}"}>Raises a "table must have at least one entry" error if <p>table</p> has fewer than one entry.</error>
        <error code={s"${errcodeBase + 1}"}>Raises an "inconsistent dimensionality" error if any input <p>x</p> and record <pf>x</pf> have different numbers of dimensions.</error>
      </doc>
    def errcodeBase = 22010

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("((%s)(%s.apply(%s)))",
        javaType(retType, true, true, false),
        javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
        args.map(_.toString).mkString(", "))

    def apply(datum: Double, table: PFAArray[PFARecord]): AnyRef = {
      val vector = table.toVector
      if (vector.isEmpty)
        throw new PFARuntimeException("table must have at least one entry", errcodeBase + 0, name, pos)
      var one: PFARecord = null
      var oned: Double = -1.0
      vector foreach {item =>
        val x = item.get("x").asInstanceOf[Double]
        val d = Math.abs(datum - x)
        if (one == null  ||  d < oned) {
          one = item
          oned = d
        }
      }
      one.get("to")
    }

    private val squaredEuclidean = {(x: PFAArray[Double], y: PFAArray[Double]) =>
      val xv = x.toVector
      val yv = y.toVector
      if (xv.size != yv.size)
        throw new PFARuntimeException("inconsistent dimensionality", errcodeBase + 1, name, pos)
      (xv zip yv) map {case (x, y) => (x - y)*(x - y)} sum
    }

    def apply(datum: PFAArray[Double], table: PFAArray[PFARecord]): AnyRef = apply(datum, table, squaredEuclidean)

    def apply[X1, X2](datum: X1, table: PFAArray[PFARecord], metric: (X1, X2) => Double): AnyRef = {
      val vector = table.toVector
      if (vector.isEmpty)
        throw new PFARuntimeException("table must have at least one entry", errcodeBase + 0, name, pos)
      var one: PFARecord = null
      var oned: Double = -1.0
      vector foreach {item =>
        val x = item.get("x").asInstanceOf[X2]
        val d = metric(datum, x)
        if (one == null  ||  d < oned) {
          one = item
          oned = d
        }
      }
      one.get("to")
    }
  }
  provide(new Nearest)

  ////   linear (Linear)
  object Linear {
    def closest(datum: Double, table: PFAArray[PFARecord], code: Int, name: String, pos: Option[String]): (PFARecord, PFARecord, Boolean) = {
      val vector = table.toVector
      // try to find the closest pair that straddles datum (returning true)
      var below: PFARecord = null
      var above: PFARecord = null
      var belowd: Double = -1.0
      var aboved: Double = -1.0
      vector foreach {item =>
        val x = item.get("x").asInstanceOf[Double]
        if (x <= datum  &&  (below == null  ||  datum - x < belowd)) {
          below = item
          belowd = datum - x
        }
        if (datum < x  &&  (above == null  ||  x - datum < aboved)) {
          above = item
          aboved = x - datum
        }
      }
      if (below != null  &&  above != null)
        (below, above, true)
      else {
        // try to find the closest pair (returning false)
        var one: PFARecord = null
        var two: PFARecord = null
        var oned: Double = -1.0
        var twod: Double = -1.0
        vector foreach {item =>
          val x = item.get("x").asInstanceOf[Double]
          val d = Math.abs(datum - x)
          if (one == null  ||  d < oned) {
            two = one
            twod = oned
            one = item
            oned = d
          }
          else if ((two == null  ||  d < twod)  &&  d != oned) {
            two = item
            twod = d
          }
        }
        if (two == null)
          throw new PFARuntimeException("table must have at least two distinct x values", code, name, pos)
        (one, two, false)
      }
    }

    class SingleLinear(code: Int, name: String, pos: Option[String]) {
      def apply(datum: Double, table: PFAArray[PFARecord]): Double = {
        val (one, two, between) = closest(datum, table, code, name, pos)
        interpolate(datum, one, two)
      }
      def interpolate(datum: Double, one: PFARecord, two: PFARecord): Double = {
        val onex = one.get("x").asInstanceOf[Double]
        val twox = two.get("x").asInstanceOf[Double]
        val oney = one.get("to").asInstanceOf[Double]
        val twoy = two.get("to").asInstanceOf[Double]
        val unitless = (datum - onex) / (twox - onex)
        (1.0 - unitless)*oney + unitless*twoy
      }
    }

    class MultiLinear(code1: Int, code2: Int, name: String, pos: Option[String]) {
      def apply(datum: Double, table: PFAArray[PFARecord]): PFAArray[Double] = {
        val (one, two, between) = closest(datum, table, code1, name, pos)
        interpolate(datum, one, two)
      }
      def interpolate(datum: Double, one: PFARecord, two: PFARecord): PFAArray[Double] = {
        val onex = one.get("x").asInstanceOf[Double]
        val twox = two.get("x").asInstanceOf[Double]
        val oney = one.get("to").asInstanceOf[PFAArray[Double]].toVector
        val twoy = two.get("to").asInstanceOf[PFAArray[Double]].toVector
        val unitless = (datum - onex) / (twox - onex)
        if (oney.size != twoy.size)
          throw new PFARuntimeException("inconsistent dimensionality", code2, name, pos)
        PFAArray.fromVector((0 until oney.size).toVector map {i => (1.0 - unitless)*oney(i) + unitless*twoy(i)})
      }
    }
  }
  class Linear(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "linear"
    def sig = Sigs(List(Sig(List("x" -> P.Double, "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Double, "to" -> P.Double)))), P.Double),
                        Sig(List("x" -> P.Double, "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Double, "to" -> P.Array(P.Double))))), P.Array(P.Double))))
    def doc =
      <doc>
        <desc>Finds the closest <pf>x</pf> values in the <p>table</p> that are below and above the input <p>x</p> and linearly projects their <pf>to</pf> values to the input <p>x</p>.</desc>
        <detail>Any ties in distance are resolved in favor of the first instance in the <p>table</p>.</detail>
        <detail>If the <pf>to</pf> values are arrays, each component will be interpolated.</detail>
        <error code={s"${errcodeBase + 0}"}>Raises a "table must have at least two distinct x values" error if fewer than two of the <p>table</p> <pf>x</pf> entries are unique.</error>
        <error code={s"${errcodeBase + 1}"}>Raises an "inconsistent dimensionality" error if the <pf>to</pf> values of the two closest entries have different numbers of dimensions.</error>
      </doc>
    def errcodeBase = 22020
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.ret match {
      case AvroDouble()            => JavaCode("(new " + getClass.getName + "$SingleLinear(" + (errcodeBase + 0).toString + ", \"" + name + "\", " + posToJava + "))")
      case AvroArray(AvroDouble()) => JavaCode("(new " + getClass.getName + "$MultiLinear(" + (errcodeBase + 0).toString + ", " + (errcodeBase + 1).toString + ", \"" + name + "\", " + posToJava + "))")
    }
  }
  provide(new Linear)

  ////   linearFlat (LinearFlat)
  object LinearFlat {
    class SingleLinear(code: Int, name: String, pos: Option[String]) {
      def apply(datum: Double, table: PFAArray[PFARecord]): Double = {
        val (one, two, between) = Linear.closest(datum, table, code, name, pos)
        if (!between)
          one.get("to").asInstanceOf[Double]
        else
          (new Linear.SingleLinear(code, name, pos)).interpolate(datum, one, two)
      }
    }

    class MultiLinear(code1: Int, code2: Int, name: String, pos: Option[String]) {
      def apply(datum: Double, table: PFAArray[PFARecord]): PFAArray[Double] = {
        val (one, two, between) = Linear.closest(datum, table, code1, name, pos)
        if (!between)
          one.get("to").asInstanceOf[PFAArray[Double]]
        else
          (new Linear.MultiLinear(code1, code2, name, pos)).interpolate(datum, one, two)
      }
    }
  }
  class LinearFlat(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "linearFlat"
    def sig = Sigs(List(Sig(List("x" -> P.Double, "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Double, "to" -> P.Double)))), P.Double),
                        Sig(List("x" -> P.Double, "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Double, "to" -> P.Array(P.Double))))), P.Array(P.Double))))
    def doc =
      <doc>
        <desc>Like <f>interp.linear</f>, but returns the closest entry's <pf>to</pf> if the input <p>x</p> is beyond the <p>table</p>.</desc>
        <detail>Any ties in distance are resolved in favor of the first instance in the <p>table</p>.</detail>
        <detail>If the <pf>to</pf> values are arrays, each component will be interpolated.</detail>
        <error code={s"${errcodeBase + 0}"}>Raises a "table must have at least two distinct x values" error if <p>table</p> has fewer than two entries.</error>
        <error code={s"${errcodeBase + 1}"}>Raises an "inconsistent dimensionality" error if the <pf>to</pf> values of the two closest entries have different numbers of dimensions.</error>
      </doc>
    def errcodeBase = 22030
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.ret match {
      case AvroDouble()            => JavaCode("(new " + getClass.getName + "$SingleLinear(" + (errcodeBase + 0).toString + ", \"" + name + "\", " + posToJava + "))")
      case AvroArray(AvroDouble()) => JavaCode("(new " + getClass.getName + "$MultiLinear(" + (errcodeBase + 0).toString + ", " + (errcodeBase + 1).toString + ", \"" + name + "\", " + posToJava + "))")
    }
  }
  provide(new LinearFlat)

  ////   linearMissing (LinearMissing)
  object LinearMissing {
    class SingleLinear(code: Int, name: String, pos: Option[String]) {
      def apply(datum: Double, table: PFAArray[PFARecord]): java.lang.Double = {
        val (one, two, between) = Linear.closest(datum, table, code, name, pos)
        if (!between  &&  one.get("x").asInstanceOf[Double] != datum)
          null.asInstanceOf[java.lang.Double]
        else
          java.lang.Double.valueOf((new Linear.SingleLinear(code, name, pos)).interpolate(datum, one, two))
      }
    }

    class MultiLinear(code1: Int, code2: Int, name: String, pos: Option[String]) {
      def apply(datum: Double, table: PFAArray[PFARecord]): PFAArray[Double] = {
        val (one, two, between) = Linear.closest(datum, table, code1, name, pos)
        if (!between  &&  one.get("x").asInstanceOf[Double] != datum)
          null.asInstanceOf[PFAArray[Double]]
        else
          (new Linear.MultiLinear(code1, code2, name, pos)).interpolate(datum, one, two)
      }
    }
  }
  class LinearMissing(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "linearMissing"
    def sig = Sigs(List(Sig(List("x" -> P.Double, "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Double, "to" -> P.Double)))), P.Union(List(P.Null, P.Double))),
                        Sig(List("x" -> P.Double, "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Double, "to" -> P.Array(P.Double))))), P.Union(List(P.Null, P.Array(P.Double))))))
    def doc =
      <doc>
        <desc>Like <f>interp.linear</f>, but returns a missing value (<c>null</c>) if the input <p>x</p> is beyond the <p>table</p>.</desc>
        <detail>Any ties in distance are resolved in favor of the first instance in the <p>table</p>.</detail>
        <detail>If the <pf>to</pf> values are arrays, each component will be interpolated.</detail>
        <error code={s"${errcodeBase + 0}"}>Raises a "table must have at least two distinct x values" error if <p>table</p> has fewer than two entries.</error>
        <error code={s"${errcodeBase + 1}"}>Raises an "inconsistent dimensionality" error if the <pf>to</pf> values of the two closest entries have different numbers of dimensions.</error>
      </doc>
    def errcodeBase = 22040
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.ret match {
      case AvroUnion(List(AvroNull(), AvroDouble()))            => JavaCode("(new " + getClass.getName + "$SingleLinear(" + (errcodeBase + 0).toString + ", \"" + name + "\", " + posToJava + "))")
      case AvroUnion(List(AvroNull(), AvroArray(AvroDouble()))) => JavaCode("(new " + getClass.getName + "$MultiLinear(" + (errcodeBase + 0).toString + ", " + (errcodeBase + 1).toString + ", \"" + name + "\", " + posToJava + "))")
    }
  }
  provide(new LinearMissing)

  // the barycentric implementation sketched below is a 2-d case, to be solved by triangular area
  // but an arbitrary-dimensional one with hypervolumes is possible

  // ////   barycentric (Barycentric)
  // class Barycentric(val pos: Option[String] = None) extends LibFcn {
  //   def name = prefix + "barycentric"
  //   def sig = Sigs(List(Sig(List("x" -> P.Double, "y" -> P.Double, "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Double, "y" -> P.Double, "to" -> P.Double)))), P.Double),
  //                       Sig(List("x" -> P.Double, "y" -> P.Double, "table" -> P.Array(P.WildRecord("R", ListMap("x" -> P.Double, "y" -> P.Double, "to" -> P.Array(P.Double))))), P.Array(P.Double))))
  //   def doc =
  //     <doc>
  //       <desc>Finds the three closest <pf>x</pf>, <pf>y</pf> points in the <p>table</p> to the input <p>x</p>, <p>y</p> and linearly projects their <pf>to</pf> values to the input <p>x</p>, <p>y</p>.</desc>
  //       <detail>Any ties in distance are resolved in favor of the first instance in the <p>table</p>.</detail>
  //       <detail>If the <pf>to</pf> values are arrays, each component will be interpolated.</detail>
  //       <error code={s"${errcodeBase + 0}"}>Raises a "table must have at least two distinct x values" error if <p>table</p> has fewer than two entries.</error>
  //       <error code={s"${errcodeBase + 1}"}>Raises an "inconsistent dimensionality" error if the <pf>to</pf> values of the two closest entries have different numbers of dimensions.</error>
  //     </doc>
  //   def errcodeBase = 22050

  //   override def javaRef(fcnType: FcnType): JavaCode = fcnType.ret match {
  //     case AvroDouble()            => JavaCode(SingleLinear.getClass.getName)
  //     case AvroArray(AvroDouble()) => JavaCode(MultiLinear.getClass.getName)
  //   }

  //   object SingleLinear {
  //     def apply(datumx: Double, datumy: Double, table: PFAArray[PFARecord]): Double = {
  //     }
  //   }

  //   object MultiLinear {
  //     def apply(datumx: Double, datumy: Double, table: PFAArray[PFARecord]): Double = {
  //     }
  //   }
  // }
  // provide(Barycentric)

  // bilinear (requires a 2-d grid)
  // trilinear (requires a 3-d grid)
  // spline (also 1-d)
  // cubic (also 1-d)
  // loess (also 1-d)

}
