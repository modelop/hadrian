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
import com.opendatagroup.hadrian.jvmcompiler.javaType
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

import com.opendatagroup.hadrian.lib.math.Round

package object core {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  def describeOverflowErrors(code1: Int, code2: Int) = (
    <error code={s"${code1}"}>Integer results above or below {java.lang.Integer.MIN_VALUE} and {java.lang.Integer.MAX_VALUE} (inclusive) produce an "int overflow" runtime error.</error>
    <error code={s"${code2}"}>Long-integer results above or below {java.lang.Long.MIN_VALUE} and {java.lang.Long.MAX_VALUE} (inclusive) produce a "long overflow" runtime error.</error>
    <detail>Float and double overflows do not produce runtime errors but result in positive or negative infinity, which would be carried through any subsequent calculations (see IEEE 754).  Use <f>impute.ensureFinite</f> to produce errors from infinite or NaN values.</detail>
  )

  val anyNumber = Set[Type](AvroInt(), AvroLong(), AvroFloat(), AvroDouble())

  //////////////////////////////////////////////////////////////////// basic arithmetic

  ////   + (Plus)
  class Plus(val pos: Option[String] = None) extends LibFcn {
    def name = "+"
    def sig = Sig(List("x" -> P.Wildcard("A", anyNumber), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Add <p>x</p> and <p>y</p>.</desc>{describeOverflowErrors(errcodeBase + 0, errcodeBase + 1)}
      </doc>
    def errcodeBase = 18000
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      if (retType.accepts(AvroFloat()))
        JavaCode("(%s + %s)", wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
      else
        super.javaCode(args, argContext, paramTypes, retType, engineOptions)
    def apply(x: Double, y: Double): Double = x + y
    def apply(x: Float, y: Float): Float = x + y
    def apply(x: Long, y: Long): Long =
      if ((y == java.lang.Long.MIN_VALUE)  ||
          (x > 0  &&  java.lang.Long.MAX_VALUE - x < y)  ||
          (x < 0  &&  x - java.lang.Long.MIN_VALUE < -y))
        throw new PFARuntimeException("long overflow", errcodeBase + 1, name, pos)
      else
        x + y
    def apply(x: Int, y: Int): Int =
      if ((y == java.lang.Integer.MIN_VALUE)  ||
          (x > 0  &&  java.lang.Integer.MAX_VALUE - x < y)  ||
          (x < 0  &&  x - java.lang.Integer.MIN_VALUE < -y))
        throw new PFARuntimeException("int overflow", errcodeBase + 0, name, pos)
      else
        x + y
  }
  provide(new Plus)

  ////   - (Minus)
  class Minus(val pos: Option[String] = None) extends LibFcn {
    def name = "-"
    def sig = Sig(List("x" -> P.Wildcard("A", anyNumber), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Subtract <p>y</p> from <p>x</p>.</desc>{describeOverflowErrors(errcodeBase + 0, errcodeBase + 1)}
      </doc>
    def errcodeBase = 18010
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      if (retType.accepts(AvroFloat()))
        JavaCode("(%s - %s)", wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
      else
        super.javaCode(args, argContext, paramTypes, retType, engineOptions)
    def apply(x: Double, y: Double): Double = x - y
    def apply(x: Float, y: Float): Float = x - y
    def apply(x: Long, y: Long): Long =
      if ((y == java.lang.Long.MIN_VALUE)  ||
          (x > 0  &&  java.lang.Long.MAX_VALUE - x < -y)  ||
          (x < 0  &&  x - java.lang.Long.MIN_VALUE < y))
        throw new PFARuntimeException("long overflow", errcodeBase + 1, name, pos)
      else
        x - y
    def apply(x: Int, y: Int): Int =
      if ((y == java.lang.Integer.MIN_VALUE)  ||
          (x > 0  &&  java.lang.Integer.MAX_VALUE - x < -y)  ||
          (x < 0  &&  x - java.lang.Integer.MIN_VALUE < y))
        throw new PFARuntimeException("int overflow", errcodeBase + 0, name, pos)
      else
        x - y
  }
  provide(new Minus)

  ////   * (Times)
  class Times(val pos: Option[String] = None) extends LibFcn {
    def name = "*"
    def sig = Sig(List("x" -> P.Wildcard("A", anyNumber), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Multiply <p>x</p> and <p>y</p>.</desc>{describeOverflowErrors(errcodeBase + 0, errcodeBase + 1)}
      </doc>
    def errcodeBase = 18020
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      if (retType.accepts(AvroFloat()))
        JavaCode("(%s * %s)", wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
      else
        super.javaCode(args, argContext, paramTypes, retType, engineOptions)
    def apply(x: Double, y: Double): Double = x * y
    def apply(x: Float, y: Float): Float = x * y
    def apply(x: Long, y: Long): Long = {
      val maximum =
        if (java.lang.Long.signum(x) == java.lang.Long.signum(y))
          java.lang.Long.MAX_VALUE
        else
          java.lang.Long.MIN_VALUE
      if ((x == -1L  &&  y == java.lang.Long.MIN_VALUE)  ||
          (x != -1L  &&  x != 0L  &&  ((y > 0L  &&  y > maximum / x)  ||  (y < 0L  &&  y < maximum / x))))
        throw new PFARuntimeException("long overflow", errcodeBase + 1, name, pos)
      else
        x * y
    }
    def apply(x: Int, y: Int): Int = {
      val maximum =
        if (java.lang.Integer.signum(x) == java.lang.Integer.signum(y))
          java.lang.Integer.MAX_VALUE
        else
          java.lang.Integer.MIN_VALUE
      if ((x == -1  &&  y == java.lang.Integer.MIN_VALUE)  ||
          (x != -1  &&  x != 0  &&  ((y > 0  &&  y > maximum / x)  ||  (y < 0  &&  y < maximum / x))))
        throw new PFARuntimeException("int overflow", errcodeBase + 0, name, pos)
      else
        x * y
    }
  }
  provide(new Times)

  ////   / (Divide)
  class Divide(val pos: Option[String] = None) extends LibFcn with Function2[Double, Double, Double] {
    def name = "/"
    def sig = Sig(List("x" -> P.Double, "y" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Divide <p>y</p> from <p>x</p>, returning a floating-point number (even if <p>x</p> and <p>y</p> are integers).</desc>
        <detail>This function returns an infinite value if <p>x</p> is non-zero and <p>y</p> is zero and NaN if both are zero.</detail>
      </doc>
    def errcodeBase = 18030
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      if (retType.accepts(AvroFloat()))
        JavaCode("(%s / (double)%s)", wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
      else
        super.javaCode(args, argContext, paramTypes, retType, engineOptions)
    def apply(x: Double, y: Double): Double = x / y
  }
  provide(new Divide)

  ////   // (FloorDivide)
  class FloorDivide(val pos: Option[String] = None) extends LibFcn {
    def name = "//"
    def sig = Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong())), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Divide <p>y</p> from <p>x</p>, returning the largest whole number <c>N</c> for which <c>N</c> {"\u2264"} <p>x</p>/<p>y</p> (integral floor division).</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>y</p> is zero, this function raises a "integer division by zero" runtime error.</error>
      </doc>
    def errcodeBase = 18040
    def apply(x: Int, y: Int): Int =
      if (y == 0)
        throw new PFARuntimeException("integer division by zero", errcodeBase + 0, name, pos)
      else
        x / y
    def apply(x: Long, y: Long): Long = 
      if (y == 0)
        throw new PFARuntimeException("integer division by zero", errcodeBase + 0, name, pos)
      else
        x / y
  }
  provide(new FloorDivide)

  ////   u- (Negative)
  class Negative(val pos: Option[String] = None) extends LibFcn {
    def name = "u-"
    def sig = Sig(List("x" -> P.Wildcard("A", anyNumber)), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the additive inverse of <p>x</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>For exactly one integer value, {java.lang.Integer.MIN_VALUE}, this function raises an "int overflow" runtime error.</error>
        <error code={s"${errcodeBase + 1}"}>For exactly one long value, {java.lang.Long.MIN_VALUE}, this function raises a "long overflow" runtime error.</error>
      </doc>
    def errcodeBase = 18050
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      if (retType.accepts(AvroFloat()))
        JavaCode("(-(%s))", wrapArg(0, args, paramTypes, true))
      else
        super.javaCode(args, argContext, paramTypes, retType, engineOptions)
    def apply(x: Double): Double = -x
    def apply(x: Float): Float = -x
    def apply(x: Long): Long =
      if (x == java.lang.Long.MIN_VALUE)
        throw new PFARuntimeException("long overflow", errcodeBase + 1, name, pos)
      else
        -x
    def apply(x: Int): Int =
      if (x == java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("int overflow", errcodeBase + 0, name, pos)
      else
        -x
  }
  provide(new Negative)

  ////   % (Modulo)
  class Modulo(val pos: Option[String] = None) extends LibFcn {
    def name = "%"
    def sig = Sig(List("k" -> P.Wildcard("A", anyNumber), "n" -> P.Wildcard("A")), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return <p>k</p> modulo <p>n</p>; the result has the same sign as the modulus <p>n</p>.</desc>
        <detail>This is the behavior of the <c>%</c> operator in Python, <c>mod</c>/<c>modulo</c> in Ada, Haskell, and Scheme.</detail>
        <error code={s"${errcodeBase + 0}"}>If <p>n</p> is zero and <p>k</p> and <p>n</p> are int or long, this function raises a "integer division by zero" runtime error.</error>
      </doc>
    def errcodeBase = 18060
    def apply(k: Double, n: Double): Double =
      if (!java.lang.Double.isNaN(k)  &&  !java.lang.Double.isInfinite(k)  &&  java.lang.Double.isInfinite(n)) {
        if ((k >= 0.0  &&  n > 0.0)  ||  (k <= 0.0  &&  n < 0.0))
          k
        else
          n
      }
      else
        ((k % n) + n) % n
    def apply(k: Float, n: Float): Float =
      if (!java.lang.Double.isNaN(k)  &&  !java.lang.Double.isInfinite(k)  &&  java.lang.Double.isInfinite(n)) {
        if ((k >= 0.0  &&  n > 0.0)  ||  (k <= 0.0  &&  n < 0.0))
          k
        else
          n
      }
      else
        ((k % n) + n) % n
    def apply(k: Long, n: Long): Long =
      if (n == 0)
        throw new PFARuntimeException("integer division by zero", errcodeBase + 0, name, pos)
      else
        ((k % n) + n) % n
    def apply(k: Int, n: Int): Int =
      if (n == 0)
        throw new PFARuntimeException("integer division by zero", errcodeBase + 0, name, pos)
      else
        ((k % n) + n) % n
  }
  provide(new Modulo)

  ////   %% (Remainder)
  class Remainder(val pos: Option[String] = None) extends LibFcn {
    def name = "%%"
    def sig = Sig(List("k" -> P.Wildcard("A", anyNumber), "n" -> P.Wildcard("A")), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return the remainder of <p>k</p> divided by <p>n</p>; the result has the same sign as the dividend <p>k</p>.</desc>
        <detail>This is the behavior of the <c>%</c> operator in Fortran, C/C++, and Java, <c>rem</c>/<c>remainder</c> in Ada, Haskell, and Scheme.</detail>
        <error code={s"${errcodeBase + 0}"}>If <p>n</p> is zero and <p>k</p> and <p>n</p> are int or long, this function raises a "integer division by zero" runtime error.</error>
      </doc>
    def errcodeBase = 18070
    def apply(k: Double, n: Double): Double = k % n
    def apply(k: Float, n: Float): Float = k % n
    def apply(k: Long, n: Long): Long =
      if (n == 0)
        throw new PFARuntimeException("integer division by zero", errcodeBase + 0, name, pos)
      else
        k % n
    def apply(k: Int, n: Int): Int =
      if (n == 0)
        throw new PFARuntimeException("integer division by zero", errcodeBase + 0, name, pos)
      else
        k % n
  }
  provide(new Remainder)

  ////   ** (Pow)
  class Pow(val pos: Option[String] = None) extends LibFcn {
    def name = "**"
    def sig = Sig(List("x" -> P.Wildcard("A", anyNumber), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Raise <p>x</p> to the power <p>n</p>.</desc>{describeOverflowErrors(errcodeBase + 0, errcodeBase + 1)}
      </doc>
    def errcodeBase = 18080
    def apply(x: Double, n: Double): Double =
      if (x == -0.0  &&  n == -1.0)            // handle weird case that only comes up with negative zero
        java.lang.Double.POSITIVE_INFINITY
      else
        java.lang.Math.pow(x, n)
    def apply(x: Float, n: Float): Float =
      if (x == -0.0F  &&  n == -1.0F)          // handle weird case that only comes up with negative zero
        java.lang.Float.POSITIVE_INFINITY
      else
        java.lang.Math.pow(x, n).toFloat
    def apply(x: Long, n: Long): Long = {
      val out = java.lang.Math.pow(x, n)
      if (java.lang.Double.isNaN(out)  ||  out > java.lang.Long.MAX_VALUE  ||  out < java.lang.Long.MIN_VALUE)
        throw new PFARuntimeException("long overflow", errcodeBase + 1, name, pos)
      else
        out.toLong
    }
    def apply(x: Int, n: Int): Int = {
      val out = java.lang.Math.pow(x, n).toFloat
      if (java.lang.Double.isNaN(out)  ||  out > java.lang.Integer.MAX_VALUE  ||  out < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("int overflow", errcodeBase + 0, name, pos)
      else
        out.toInt
    }
  }
  provide(new Pow)

  //////////////////////////////////////////////////////////////////// generic comparison operators

  ////   cmp (Comparison)
  class Comparison(val pos: Option[String] = None) extends LibFcn {
    def name = "cmp"
    def sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Int)
    def doc =
      <doc>
        <desc>Return <c>1</c> if <p>x</p> is greater than <p>y</p>, <c>-1</c> if <p>x</p> is less than <p>y</p>, and <c>0</c> if <p>x</p> and <p>y</p> are equal.</desc>
      </doc>
    def errcodeBase = 18090
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalComparison")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.Comparison(%s))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("%s.apply(%s)", javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString, wrapArgs(args, paramTypes, false))
  }
  provide(new Comparison)

  ////   == (Equal)
  class Equal(val pos: Option[String] = None) extends LibFcn {
    def name = "=="
    def sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is equal to <p>y</p>, <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 18100
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalEQ")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonOperator(%s, 1))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new Equal)

  ////   >= (GreaterOrEqual)
  class GreaterOrEqual(val pos: Option[String] = None) extends LibFcn {
    def name = ">="
    def sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is greater than or equal to <p>y</p>, <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 18110
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalGE")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonOperator(%s, 2))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new GreaterOrEqual)

  ////   > (GreaterThan)
  class GreaterThan(val pos: Option[String] = None) extends LibFcn {
    def name = ">"
    def sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is greater than <p>y</p>, <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 18120
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalGT")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonOperator(%s, 3))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new GreaterThan)

  ////   != (NotEqual)
  class NotEqual(val pos: Option[String] = None) extends LibFcn {
    def name = "!="
    def sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is not equal to <p>y</p>, <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 18130
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalNE")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonOperator(%s, -1))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new NotEqual)

  ////   < (LessThan)
  class LessThan(val pos: Option[String] = None) extends LibFcn {
    def name = "<"
    def sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is less than <p>y</p>, <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 18140
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalLT")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonOperator(%s, -2))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new LessThan)

  ////   <= (LessOrEqual)
  class LessOrEqual(val pos: Option[String] = None) extends LibFcn {
    def name = "<="
    def sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is less than or equal to <p>y</p>, <c>false</c> otherwise.</desc>
      </doc>
    def errcodeBase = 18150
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalLE")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonOperator(%s, -3))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(new LessOrEqual)

  //////////////////////////////////////////////////////////////////// max and min

  ////   max (Max)
  class Max(val pos: Option[String] = None) extends LibFcn {
    def name = "max"
    def sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return <p>x</p> if <p>x</p> {"\u2265"} <p>y</p>, <p>y</p> otherwise.</desc>
        <detail>For the maximum of more than two values, see <f>a.max</f></detail>
      </doc>
    def errcodeBase = 18160
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.ret match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble =>
        JavaCode("com.opendatagroup.hadrian.data.NumericalMax")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonMax(%s))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode = retType match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble =>
        super.javaCode(args, argContext, paramTypes, retType, engineOptions)
      case _ =>
        JavaCode("((%s)(%s.apply((Object)%s, (Object)%s)))",
          javaType(retType, true, true, false),
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
    }
  }
  provide(new Max)

  ////   min (Min)
  class Min(val pos: Option[String] = None) extends LibFcn {
    def name = "min"
    def sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return <p>x</p> if <p>x</p> &lt; <p>y</p>, <p>y</p> otherwise.</desc>
        <detail>For the minimum of more than two values, see <f>a.min</f></detail>
      </doc>
    def errcodeBase = 18170
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.ret match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble =>
        JavaCode("com.opendatagroup.hadrian.data.NumericalMin")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonMin(%s))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode = retType match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble =>
        super.javaCode(args, argContext, paramTypes, retType, engineOptions)
      case _ =>
        JavaCode("((%s)(%s.apply((Object)%s, (Object)%s)))",
          javaType(retType, true, true, false),
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
    }
  }
  provide(new Min)

  //////////////////////////////////////////////////////////////////// logical operators

  ////   && (LogicalAnd)
  class LogicalAnd(val pos: Option[String] = None) extends LibFcn {
    def name = "&&"
    def sig = Sig(List("x" -> P.Boolean, "y" -> P.Boolean), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> and <p>y</p> are both <c>true</c>, <c>false</c> otherwise.</desc>
        <detail>If <p>x</p> is <c>false</c>, <p>y</p> won't be evaluated.  (Only relevant for arguments with side effects.)</detail>
      </doc>
    def errcodeBase = 18180
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("(%s && %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: => Boolean, y: => Boolean): Boolean = x && y
  }
  provide(new LogicalAnd)

  ////   || (LogicalOr)
  class LogicalOr(val pos: Option[String] = None) extends LibFcn {
    def name = "||"
    def sig = Sig(List("x" -> P.Boolean, "y" -> P.Boolean), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if either <p>x</p> or <p>y</p> (or both) are <c>true</c>, <c>false</c> otherwise.</desc>
        <detail>If <p>x</p> is <c>true</c>, <p>y</p> won't be evaluated.  (Only relevant for arguments with side effects.)</detail>
      </doc>
    def errcodeBase = 18190
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("(%s || %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: => Boolean, y: => Boolean): Boolean = x || y
  }
  provide(new LogicalOr)

  ////   ^^ (LogicalXOr)
  class LogicalXOr(val pos: Option[String] = None) extends LibFcn {
    def name = "^^"
    def sig = Sig(List("x" -> P.Boolean, "y" -> P.Boolean), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is <c>true</c> and <p>y</p> is <c>false</c> or if <p>x</p> is <c>false</c> and <p>y</p> is <c>true</c>, but return <c>false</c> for any other case.</desc>
      </doc>
    def errcodeBase = 18200
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("(((%s ? 1 : 0) ^ (%s ? 1 : 0)) != 0)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: Boolean, y: Boolean): Boolean = (x && !y) || (y && !x)
  }
  provide(new LogicalXOr)

  ////   ! (LogicalNot)
  class LogicalNot(val pos: Option[String] = None) extends LibFcn {
    def name = "!"
    def sig = Sig(List("x" -> P.Boolean), P.Boolean)
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is <c>false</c> and <c>false</c> if <p>x</p> is <c>true</c>.</desc>
      </doc>
    def errcodeBase = 18210
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("(!%s)", wrapArg(0, args, paramTypes, false).toString)
    def apply(x: Boolean): Boolean = !x
  }
  provide(new LogicalNot)

  //////////////////////////////////////////////////////////////////// Kleene (three-state) logical operators

  ////   &&& (KleeneAnd)
  class KleeneAnd(val pos: Option[String] = None) extends LibFcn {
    def name = "&&&"
    def sig = Sig(List("x" -> P.Union(List(P.Boolean, P.Null)), "y" -> P.Union(List(P.Boolean, P.Null))), P.Union(List(P.Boolean, P.Null)))
    def doc =
      <doc>
        <desc>Return <c>false</c> if <p>x</p> or <p>y</p> is <c>false</c>, <c>true</c> if <p>x</p> and <p>y</p> are <c>true</c>, and <c>null</c> otherwise.</desc>
        <detail>This corresponds to Kleene's three-state logic, in which <c>null</c> represents a boolean quantity whose value is unknown.</detail>
        <detail>If <p>x</p> is <c>false</c>, <p>y</p> won't be evaluated.  (Only relevant for arguments with side effects.)</detail>
      </doc>
    def errcodeBase = 18220
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("""%s.apply(new scala.runtime.AbstractFunction0<java.lang.Boolean>() {
public java.lang.Boolean apply() { return (java.lang.Boolean)%s; }
}, new scala.runtime.AbstractFunction0<java.lang.Boolean>() {
public java.lang.Boolean apply() { return (java.lang.Boolean)%s; }
})""", javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString, wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: => java.lang.Boolean, y: => java.lang.Boolean): java.lang.Boolean = {
      val xval = x
      if (xval == java.lang.Boolean.FALSE)
        java.lang.Boolean.FALSE
      else {
        val yval = y
        if (yval == java.lang.Boolean.FALSE)
          java.lang.Boolean.FALSE
        else if (xval == java.lang.Boolean.TRUE  &&  yval == java.lang.Boolean.TRUE)
          java.lang.Boolean.TRUE
        else
          null
      }
    }
  }
  provide(new KleeneAnd)

  ////   ||| (KleeneOr)
  class KleeneOr(val pos: Option[String] = None) extends LibFcn {
    def name = "|||"
    def sig = Sig(List("x" -> P.Union(List(P.Boolean, P.Null)), "y" -> P.Union(List(P.Boolean, P.Null))), P.Union(List(P.Boolean, P.Null)))
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> or <p>y</p> is <c>true</c>, <c>false</c> if both <p>x</p> and <p>y</p> is <c>false</c>, or <c>null</c> otherwise.</desc>
        <detail>This corresponds to Kleene's three-state logic, in which <c>null</c> represents a boolean quantity whose value is unknown.</detail>
        <detail>If <p>x</p> is <c>true</c>, <p>y</p> won't be evaluated.  (Only relevant for arguments with side effects.)</detail>
      </doc>
    def errcodeBase = 18230
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("""%s.apply(new scala.runtime.AbstractFunction0<java.lang.Boolean>() {
public java.lang.Boolean apply() { return (java.lang.Boolean)%s; }
}, new scala.runtime.AbstractFunction0<java.lang.Boolean>() {
public java.lang.Boolean apply() { return (java.lang.Boolean)%s; }
})""", javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString, wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: => java.lang.Boolean, y: => java.lang.Boolean): java.lang.Boolean = {
      val xval = x
      if (xval == java.lang.Boolean.TRUE)
        java.lang.Boolean.TRUE
      else {
        val yval = y
        if (yval == java.lang.Boolean.TRUE)
          java.lang.Boolean.TRUE
        else if (xval == java.lang.Boolean.FALSE  &&  yval == java.lang.Boolean.FALSE)
          java.lang.Boolean.FALSE
        else
          null
      }
    }
  }
  provide(new KleeneOr)

  ////   !!! (KleeneNot)
  class KleeneNot(val pos: Option[String] = None) extends LibFcn {
    def name = "!!!"
    def sig = Sig(List("x" -> P.Union(List(P.Boolean, P.Null))), P.Union(List(P.Boolean, P.Null)))
    def doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is <c>false</c>, <c>false</c> if <p>x</p> is <c>true</c>, or <c>null</c> if <p>x</p> is <c>null</c>.</desc>
        <detail>This corresponds to Kleene's three-state logic, in which <c>null</c> represents a boolean quantity whose value is unknown.</detail>
      </doc>
    def errcodeBase = 18240
    def apply(x: AnyRef): java.lang.Boolean =
      if (x == java.lang.Boolean.TRUE)
        java.lang.Boolean.FALSE
      else if (x == java.lang.Boolean.FALSE)
        java.lang.Boolean.TRUE
      else
        null
  }
  provide(new KleeneNot)

  //////////////////////////////////////////////////////////////////// bitwise arithmetic

  ////   & (BitwiseAnd)
  class BitwiseAnd(val pos: Option[String] = None) extends LibFcn with Function2[Long, Long, Long] {
    def name = "&"
    def sig = Sigs(List(Sig(List("x" -> P.Int, "y" -> P.Int), P.Int),
                        Sig(List("x" -> P.Long, "y" -> P.Long), P.Long)))
    def doc =
      <doc>
        <desc>Calculate the bitwise-and of <p>x</p> and <p>y</p>.</desc>
      </doc>
    def errcodeBase = 18250
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("(%s & %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: Int, y: Int): Int = x & y
    def apply(x: Long, y: Long): Long = x & y
  }
  provide(new BitwiseAnd)

  ////   | (BitwiseOr)
  class BitwiseOr(val pos: Option[String] = None) extends LibFcn with Function2[Long, Long, Long] {
    def name = "|"
    def sig = Sigs(List(Sig(List("x" -> P.Int, "y" -> P.Int), P.Int),
                        Sig(List("x" -> P.Long, "y" -> P.Long), P.Long)))
    def doc =
      <doc>
        <desc>Calculate the bitwise-or of <p>x</p> and <p>y</p>.</desc>
      </doc>
    def errcodeBase = 18260
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("(%s | %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: Int, y: Int): Int = x | y
    def apply(x: Long, y: Long): Long = x | y
  }
  provide(new BitwiseOr)

  ////   ^ (BitwiseXOr)
  class BitwiseXOr(val pos: Option[String] = None) extends LibFcn with Function2[Long, Long, Long] {
    def name = "^"
    def sig = Sigs(List(Sig(List("x" -> P.Int, "y" -> P.Int), P.Int),
                        Sig(List("x" -> P.Long, "y" -> P.Long), P.Long)))
    def doc =
      <doc>
        <desc>Calculate the bitwise-exclusive-or of <p>x</p> and <p>y</p>.</desc>
      </doc>
    def errcodeBase = 18270
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      JavaCode("(%s ^ %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: Int, y: Int): Int = x ^ y
    def apply(x: Long, y: Long): Long = x ^ y
  }
  provide(new BitwiseXOr)

  ////   ~ (BitwiseNot)
  class BitwiseNot(val pos: Option[String] = None) extends LibFcn {
    def name = "~"
    def sig = Sigs(List(Sig(List("x" -> P.Int), P.Int),
                        Sig(List("x" -> P.Long), P.Long)))
    def doc =
      <doc>
        <desc>Calculate the bitwise-not of <p>x</p>.</desc>
      </doc>
    def errcodeBase = 18280
    def apply(x: Int): Int = ~x
    def apply(x: Long): Long = ~x
  }
  provide(new BitwiseNot)

}
