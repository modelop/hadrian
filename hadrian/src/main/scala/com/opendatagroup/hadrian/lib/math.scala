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

import scala.collection.immutable.ListMap

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema
import com.opendatagroup.hadrian.options.EngineOptions

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

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

package object math {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "m."

  //////////////////////////////////////////////////////////////////// constants (0-arity side-effect free functions)

  ////   pi (Pi)
  class Pi(val pos: Option[String] = None) extends LibFcn with Function0[Double] {
    def name = prefix + "pi"
    def sig = Sig(List(), P.Double)
    def doc =
      <doc>
        <desc>The double-precision number that is closer than any other to <m>\pi</m>, the ratio of a circumference of a circle to its diameter.</desc>
      </doc>
    def errcodeBase = 27000
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.PI")
    def apply(): Double = java.lang.Math.PI
  }
  provide(new Pi)

  ////   e (E)
  class E(val pos: Option[String] = None) extends LibFcn with Function0[Double] {
    def name = prefix + "e"
    def sig = Sig(List(), P.Double)
    def doc =
      <doc>
        <desc>The double-precision number that is closer than any other to <m>e</m>, the base of natural logarithms.</desc>
      </doc>
    def errcodeBase = 27010
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.E")
    def apply(): Double = java.lang.Math.E
  }
  provide(new E)

  //////////////////////////////////////////////////////////////////// basic functions (alphabetical order)

  def domain(low: String, high: String, lowInclusive: String = "", highInclusive: String = " (inclusive)",
    result: Array[scala.xml.Node] = <x>Beyond this domain, the result is <c>NaN</c>, not an exception (see IEEE 754).</x>.child.toArray,
    ensureFinite: Array[scala.xml.Node] = <x>Use <f>impute.ensureFinite</f> to produce errors from infinite or <c>NaN</c> values."</x>.child.toArray) =
      <detail>The domain of this function is from {low}{lowInclusive} to {high}{highInclusive}.  {result}  {ensureFinite}</detail>

  def wholeLine(tpe: String = " real") = <detail>The domain of this function is the whole{tpe} line; no input is invalid.</detail>
  def wholePlane(tpe: String = " real") = <detail>The domain of this function is the whole{tpe} plane; no pair of inputs is invalid.</detail>

  val avoidsRoundoff = <detail>Avoids round-off or overflow errors in the intermediate steps.</detail>

  val anyNumber = Set[Type](AvroInt(), AvroLong(), AvroFloat(), AvroDouble())

  ////   abs (Abs)
  class Abs(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "abs"
    def sig = Sig(List("x" -> P.Wildcard("A", anyNumber)), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the absolute value of <p>x</p>.</desc>{wholeLine()}
        <error code={s"${errcodeBase + 0}"}>For exactly one integer value, {java.lang.Integer.MIN_VALUE}, this function produces an "int overflow" runtime error.</error>
        <error code={s"${errcodeBase + 1}"}>For exactly one long value, {java.lang.Long.MIN_VALUE}, this function produces a "long overflow" runtime error.</error>
      </doc>
    def errcodeBase = 27020
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      if (retType.accepts(AvroFloat()))
        JavaCode("Math.abs(%s)", wrapArg(0, args, paramTypes, false))
      else
        super.javaCode(args, argContext, paramTypes, retType, engineOptions)
    def apply(x: Double): Double = java.lang.Math.abs(x)
    def apply(x: Float): Float = java.lang.Math.abs(x)
    def apply(x: Long): Long = {
      if (x == java.lang.Long.MIN_VALUE)
        throw new PFARuntimeException("long overflow", errcodeBase + 1, name, pos)
      java.lang.Math.abs(x)
    }
    def apply(x: Int): Int = {
      if (x == java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("int overflow", errcodeBase + 0, name, pos)
      java.lang.Math.abs(x)
    }
  }
  provide(new Abs)

  ////   acos (ACos)
  class ACos(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "acos"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the arc-cosine (inverse of the cosine function) of <p>x</p> as an angle in radians between <m>0</m> and <m>\pi</m>.</desc>{domain("-1", "1")}
      </doc>
    def errcodeBase = 27030
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.acos(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.acos(x)
  }
  provide(new ACos)

  ////   asin (ASin)
  class ASin(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "asin"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the arc-sine (inverse of the sine function) of <p>x</p> as an angle in radians between <m>-\pi/2</m> and <m>\pi/2</m>.</desc>{domain("-1", "1")}
      </doc>
    def errcodeBase = 27040
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.asin(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.asin(x)
  }
  provide(new ASin)

  ////   atan (ATan)
  class ATan(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "atan"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the arc-tangent (inverse of the tangent function) of <p>x</p> as an angle in radians between <m>-\pi/2</m> and <m>\pi/2</m>.</desc>{wholeLine()}
      </doc>
    def errcodeBase = 27050
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.atan(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.atan(x)
  }
  provide(new ATan)

  ////   atan2 (ATan2)
  class ATan2(val pos: Option[String] = None) extends LibFcn with Function2[Double, Double, Double] {
    def name = prefix + "atan2"
    def sig = Sig(List("y" -> P.Double, "x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the arc-tangent (inverse of the tangent function) of <p>y</p>/<p>x</p> without loss of precision for small <p>x</p>.</desc>{wholePlane()}
        <detail>Note that <p>y</p> is the first parameter and <p>x</p> is the second parameter.</detail>
      </doc>
    def errcodeBase = 27060
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.atan2(%s, %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: Double, y: Double): Double = java.lang.Math.atan2(x, y)
  }
  provide(new ATan2)

  ////   ceil (Ceil)
  class Ceil(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "ceil"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the smallest (closest to negative infinity, not closest to zero) whole number that is greater than or equal to the input.</desc>{wholeLine()}
      </doc>
    def errcodeBase = 27070
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.ceil(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.ceil(x)
  }
  provide(new Ceil)

  ////   copysign (CopySign)
  class CopySign(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "copysign"
    def sig = Sig(List("mag" -> P.Wildcard("A", anyNumber), "sign" -> P.Wildcard("A")), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return a number with the magnitude of <p>mag</p> and the sign of <p>sign</p>.</desc>{wholePlane(" real or integer")}
        <details>The return value is positive if <p>mag</p> is not zero and <p>sign</p> is zero.</details>
      </doc>
    def errcodeBase = 27080
    def apply(mag: Double, sign: Double): Double = Math.copySign(mag, sign)
    def apply(mag: Float, sign: Float): Float = Math.copySign(mag, sign).toFloat
    def apply(mag: Long, sign: Long): Long = Math.abs(mag) * (if (sign < 0) -1 else 1)
    def apply(mag: Int, sign: Int): Int = Math.abs(mag) * (if (sign < 0) -1 else 1)
  }
  provide(new CopySign)

  ////   cos (Cos)
  class Cos(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "cos"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the trigonometric cosine of <p>x</p>, which is assumed to be in radians.</desc>{wholeLine()}
      </doc>
    def errcodeBase = 27090
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.cos(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.cos(x)
  }
  provide(new Cos)

  ////   cosh (CosH)
  class CosH(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "cosh"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the hyperbolic cosine of <p>x</p>, which is equal to <m>{"""\frac{e^x + e^{-x}}{2}"""}</m></desc>{wholeLine()}
      </doc>
    def errcodeBase = 27100
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.cosh(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.cosh(x)
  }
  provide(new CosH)

  ////   exp (Exp)
  class Exp(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "exp"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return <f>m.e</f> raised to the power of <p>x</p>.</desc>{wholeLine()}
      </doc>
    def errcodeBase = 27110
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.exp(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.exp(x)
  }
  provide(new Exp)

  ////   expm1 (ExpM1)
  class ExpM1(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "expm1"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return <m>e^x - 1</m>.</desc>{avoidsRoundoff}{wholeLine()}
      </doc>
    def errcodeBase = 27120
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.expm1(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.expm1(x)
  }
  provide(new ExpM1)

  ////   floor (Floor)
  class Floor(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "floor"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the largest (closest to positive infinity) whole number that is less than or equal to the input.</desc>{wholeLine()}
      </doc>
    def errcodeBase = 27130
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.floor(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.floor(x)
  }
  provide(new Floor)

  ////   hypot (Hypot)
  class Hypot(val pos: Option[String] = None) extends LibFcn with Function2[Double, Double, Double] {
    def name = prefix + "hypot"
    def sig = Sig(List("x" -> P.Double, "y" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return <m>{"""\sqrt{x^2 + y^2}"""}</m>.</desc>{avoidsRoundoff}{wholeLine()}
      </doc>
    def errcodeBase = 27140
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.hypot(%s, %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: Double, y: Double): Double = java.lang.Math.hypot(x, y)
  }
  provide(new Hypot)

  ////   ln (Ln)
  class Ln(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "ln"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the natural logarithm of <p>x</p>.</desc>
        {domain("0", "infinity", "", " (exclusive)", <x>Given zero, the result is negative infinity, and below zero, the result is <c>NaN</c>, not an exception (see IEEE 754).</x>.child.toArray)}
      </doc>
    def errcodeBase = 27150
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.log(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.log(x)
  }
  provide(new Ln)

  ////   log10 (Log10)
  class Log10(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "log10"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the logarithm base 10 of <p>x</p>.</desc>
        {domain("0", "infinity", "", " (exclusive)", <x>Given zero, the result is negative infinity, and below zero, the result is <c>NaN</c>, not an exception (see IEEE 754).</x>.child.toArray)}
      </doc>
    def errcodeBase = 27160
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.log10(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.log10(x)
  }
  provide(new Log10)

  ////   log (Log)
  class Log(val pos: Option[String] = None) extends LibFcn with Function2[Double, Int, Double] {
    def name = prefix + "log"
    def sig = Sig(List("x" -> P.Double, "base" -> P.Int), P.Double)
    def doc =
      <doc>
        <desc>Return the logarithm of <p>x</p> with a given <p>base</p>.</desc>
        {domain("0", "infinity", "", " (exclusive)", <x>Given zero, the result is negative infinity, and below zero, the result is <c>NaN</c>, not an exception (see IEEE 754).</x>.child.toArray)}
        <error code={s"${errcodeBase + 0}"}>If <p>base</p> is less than or equal to zero, this function produces a "base must be positive" runtime error.</error>
      </doc>
    def errcodeBase = 27170
    def apply(x: Double, base: Int): Double = {
      if (base <= 0)
        throw new PFARuntimeException("base must be positive", errcodeBase + 0, name, pos)
      java.lang.Math.log(x) / java.lang.Math.log(base)
    }
  }
  provide(new Log)

  ////   ln1p (Ln1p)
  class Ln1p(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "ln1p"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return <m>ln(x^2 + 1)</m>.</desc>{avoidsRoundoff}
        {domain("-1", "infinity", "", " (exclusive)", <x>Given -1, the result is negative infinity, and below -1, the result is <c>NaN</c>, not an exception (see IEEE 754).</x>.child.toArray)}
      </doc>
    def errcodeBase = 27180
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.log1p(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.log1p(x)
  }
  provide(new Ln1p)

  ////   round (Round)
  class Round(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "round"
    def sig = Sigs(List(Sig(List("x" -> P.Float), P.Int),
                        Sig(List("x" -> P.Double), P.Long)))
    def doc =
      <doc>
        <desc>Return the closest whole number to <p>x</p>, rounding up if the fractional part is exactly one-half.</desc>
        <detail>Equal to <f>m.floor</f> of (<p>x</p> + 0.5).</detail>
        <error code={s"${errcodeBase + 0}"}>Integer results outside of {java.lang.Integer.MIN_VALUE} and {java.lang.Integer.MAX_VALUE} (inclusive) produce an "int overflow" runtime error.</error>
        <error code={s"${errcodeBase + 1}"}>Long-integer results outside of {java.lang.Long.MIN_VALUE} and {java.lang.Long.MAX_VALUE} (inclusive) produce a "long overflow" runtime error.</error>
      </doc>
    def errcodeBase = 27190
    def apply(x: Double): Long =
      if (java.lang.Double.isNaN(x)  ||  x > java.lang.Long.MAX_VALUE  ||  x < java.lang.Long.MIN_VALUE)
        throw new PFARuntimeException("long overflow", errcodeBase + 1, name, pos)
      else
        java.lang.Math.round(x)
    def apply(x: Float): Int =
      if (java.lang.Float.isNaN(x)  ||  x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("int overflow", errcodeBase + 0, name, pos)
      else
        java.lang.Math.round(x)
  }
  provide(new Round)

  ////   rint (RInt)
  class RInt(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "rint"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the closest whole number to <p>x</p>, rounding toward the nearest even number if the fractional part is exactly one-half.</desc>
      </doc>
    def errcodeBase = 27200
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.rint(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.rint(x)
  }
  provide(new RInt)

  ////   signum (Signum)
  class Signum(val pos: Option[String] = None) extends LibFcn with Function1[Double, Int] {
    def name = prefix + "signum"
    def sig = Sig(List("x" -> P.Double), P.Int)
    def doc =
      <doc>
        <desc>Return 0 if <p>x</p> is zero, 1 if <p>x</p> is positive, and -1 if <p>x</p> is negative.</desc>{wholeLine()}
      </doc>
    def errcodeBase = 27210
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("((int)(Math.signum(%s)))", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Int = java.lang.Math.signum(x).toInt
  }
  provide(new Signum)

  ////   sin (Sin)
  class Sin(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "sin"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the trigonometric sine of <p>x</p>, which is assumed to be in radians.</desc>{wholeLine()}
      </doc>
    def errcodeBase = 27220
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.sin(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.sin(x)
  }
  provide(new Sin)

  ////   sinh (SinH)
  class SinH(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "sinh"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the hyperbolic sine of <p>x</p>, which is equal to <m>{"""\frac{e^x - e^{-x}}{2}"""}</m>.</desc>{wholeLine()}
      </doc>
    def errcodeBase = 27230
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.sinh(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.sinh(x)
  }
  provide(new SinH)

  ////   sqrt (Sqrt)
  class Sqrt(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "sqrt"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the positive square root of <p>x</p>.</desc>{domain("0", "infinity", lowInclusive = " (inclusive)", highInclusive = "")}
      </doc>
    def errcodeBase = 27240
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.sqrt(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.sqrt(x)
  }
  provide(new Sqrt)

  ////   tan (Tan)
  class Tan(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "tan"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the trigonometric tangent of <p>x</p>, which is assumed to be in radians.</desc>{wholeLine()}
      </doc>
    def errcodeBase = 27250
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.tan(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.tan(x)
  }
  provide(new Tan)

  ////   tanh (TanH)
  class TanH(val pos: Option[String] = None) extends LibFcn with Function1[Double, Double] {
    def name = prefix + "tanh"
    def sig = Sig(List("x" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return the hyperbolic tangent of <p>x</p>, which is equal to <m>{"""\frac{e^x - e^{-x}}{e^x + e^{-x}}"""}</m>.</desc>{wholeLine()}
      </doc>
    def errcodeBase = 27260
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("Math.tanh(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.tanh(x)
  }
  provide(new TanH)
}
