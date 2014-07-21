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

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema
import com.opendatagroup.hadrian.jvmcompiler.javaType

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

import com.opendatagroup.hadrian.lib1.math.Round

package object core {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val describeOverflowErrors = (
    <error>Integer results above or below {java.lang.Integer.MIN_VALUE} and {java.lang.Integer.MAX_VALUE} (inclusive) produce an "int overflow" runtime error.</error>
    <error>Long-integer results above or below {java.lang.Long.MIN_VALUE} and {java.lang.Long.MAX_VALUE} (inclusive) produce a "long overflow" runtime error.</error>
    <detail>Float and double overflows do not produce runtime errors but result in positive or negative infinity, which would be carried through any subsequent calculations (see IEEE 754).  Use <f>impute.ensureFinite</f> to produce errors from infinite or NaN values.</detail>
  )

  val anyNumber = Set[Type](AvroInt(), AvroLong(), AvroFloat(), AvroDouble())

  //////////////////////////////////////////////////////////////////// basic arithmetic

  ////   + (Plus)
  object Plus extends LibFcn {
    val name = "+"
    val sig = Sig(List("x" -> P.Wildcard("A", anyNumber), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Add <p>x</p> and <p>y</p>.</desc>{describeOverflowErrors}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      if (retType.accepts(AvroFloat()))
        JavaCode("(%s + %s)", wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
      else
        super.javaCode(args, argContext, paramTypes, retType)
    def apply(x: Double, y: Double): Double = x + y
    def apply(x: Float, y: Float): Float = x + y
    def apply(x: Long, y: Long): Long =
      if ((y == java.lang.Long.MIN_VALUE)  ||
          (x > 0  &&  java.lang.Long.MAX_VALUE - x < y)  ||
          (x < 0  &&  x - java.lang.Long.MIN_VALUE < -y))
        throw new PFARuntimeException("long overflow")
      else
        x + y
    def apply(x: Int, y: Int): Int =
      if ((y == java.lang.Integer.MIN_VALUE)  ||
          (x > 0  &&  java.lang.Integer.MAX_VALUE - x < y)  ||
          (x < 0  &&  x - java.lang.Integer.MIN_VALUE < -y))
        throw new PFARuntimeException("int overflow")
      else
        x + y
  }
  provide(Plus)

  ////   - (Minus)
  object Minus extends LibFcn {
    val name = "-"
    val sig = Sig(List("x" -> P.Wildcard("A", anyNumber), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Subtract <p>y</p> from <p>x</p>.</desc>{describeOverflowErrors}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      if (retType.accepts(AvroFloat()))
        JavaCode("(%s - %s)", wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
      else
        super.javaCode(args, argContext, paramTypes, retType)
    def apply(x: Double, y: Double): Double = x - y
    def apply(x: Float, y: Float): Float = x - y
    def apply(x: Long, y: Long): Long =
      if ((y == java.lang.Long.MIN_VALUE)  ||
          (x > 0  &&  java.lang.Long.MAX_VALUE - x < -y)  ||
          (x < 0  &&  x - java.lang.Long.MIN_VALUE < y))
        throw new PFARuntimeException("long overflow")
      else
        x - y
    def apply(x: Int, y: Int): Int =
      if ((y == java.lang.Integer.MIN_VALUE)  ||
          (x > 0  &&  java.lang.Integer.MAX_VALUE - x < -y)  ||
          (x < 0  &&  x - java.lang.Integer.MIN_VALUE < y))
        throw new PFARuntimeException("int overflow")
      else
        x - y
  }
  provide(Minus)

  ////   * (Times)
  object Times extends LibFcn {
    val name = "*"
    val sig = Sig(List("x" -> P.Wildcard("A", anyNumber), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Multiply <p>x</p> and <p>y</p>.</desc>{describeOverflowErrors}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      if (retType.accepts(AvroFloat()))
        JavaCode("(%s * %s)", wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
      else
        super.javaCode(args, argContext, paramTypes, retType)
    def apply(x: Double, y: Double): Double = x * y
    def apply(x: Float, y: Float): Float = x * y
    def apply(x: Long, y: Long): Long = {
      val maximum =
        if (java.lang.Long.signum(x) == java.lang.Long.signum(y))
          java.lang.Long.MAX_VALUE
        else
          java.lang.Long.MIN_VALUE

      if (x != 0  &&  ((y > 0  &&  y > maximum / x)  ||  (y < 0  &&  y < maximum / x)))
        throw new PFARuntimeException("long overflow")
      else
        x * y
    }
    def apply(x: Int, y: Int): Int = {
      val maximum =
        if (java.lang.Integer.signum(x) == java.lang.Integer.signum(y))
          java.lang.Integer.MAX_VALUE
        else
          java.lang.Integer.MIN_VALUE

      if (x != 0  &&  ((y > 0  &&  y > maximum / x)  ||  (y < 0  &&  y < maximum / x)))
        throw new PFARuntimeException("int overflow")
      else
        x * y
    }
  }
  provide(Times)

  ////   / (Divide)
  object Divide extends LibFcn {
    val name = "/"
    val sig = Sig(List("x" -> P.Double, "y" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Divide <p>y</p> from <p>x</p>, returning a floating-point number (even if <p>x</p> and <p>y</p> are integers).</desc>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      if (retType.accepts(AvroFloat()))
        JavaCode("(%s / (double)%s)", wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
      else
        super.javaCode(args, argContext, paramTypes, retType)
    def apply(x: Double, y: Double): Double = x / y
  }
  provide(Divide)

  ////   // (FloorDivide)
  object FloorDivide extends LibFcn {
    val name = "//"
    val sig = Sig(List("x" -> P.Wildcard("A", Set(AvroInt(), AvroLong())), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Divide <p>y</p> from <p>x</p>, returning the largest whole number <c>N</c> for which <c>N</c> {"\u2264"} <p>x</p>/<p>y</p> (integral floor division).</desc>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("(%s / %s)", wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
    def apply(x: Int, y: Int): Int = x / y
    def apply(x: Long, y: Long): Long = x / y
  }
  provide(FloorDivide)

  ////   u- (Negative)
  object Negative extends LibFcn {
    val name = "u-"
    val sig = Sig(List("x" -> P.Wildcard("A", anyNumber)), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return the additive inverse of <p>x</p>.</desc>
        <error>For exactly one integer value, {java.lang.Integer.MIN_VALUE}, this function produces an "int overflow" runtime error.</error>
        <error>For exactly one long value, {java.lang.Long.MIN_VALUE}, this function produces a "long overflow" runtime error.</error>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      if (retType.accepts(AvroFloat()))
        JavaCode("(-(%s))", wrapArg(0, args, paramTypes, true))
      else
        super.javaCode(args, argContext, paramTypes, retType)
    def apply(x: Double): Double = -x
    def apply(x: Float): Float = -x
    def apply(x: Long): Long =
      if (x == java.lang.Long.MIN_VALUE)
        throw new PFARuntimeException("long overflow")
      else
        -x
    def apply(x: Int): Int =
      if (x == java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("int overflow")
      else
        -x
  }
  provide(Negative)

  ////   % (Modulo)
  object Modulo extends LibFcn {
    val name = "%"
    val sig = Sig(List("k" -> P.Wildcard("A", anyNumber), "n" -> P.Wildcard("A")), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return <p>k</p> modulo <p>n</p>; the result has the same sign as the modulus <p>n</p>.</desc>
        <detail>This is the behavior of the <c>%</c> operator in Python, <c>mod</c>/<c>modulo</c> in Ada, Haskell, and Scheme.</detail>
      </doc>
    def apply(k: Double, n: Double): Double = ((k % n) + n) % n
    def apply(k: Float, n: Float): Float = ((k % n) + n) % n
    def apply(k: Long, n: Long): Long = ((k % n) + n) % n
    def apply(k: Int, n: Int): Int = ((k % n) + n) % n
  }
  provide(Modulo)

  ////   %% (Remainder)
  object Remainder extends LibFcn {
    val name = "%%"
    val sig = Sig(List("k" -> P.Wildcard("A", anyNumber), "n" -> P.Wildcard("A")), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return the remainder of <p>k</p> divided by <p>n</p>; the result has the same sign as the dividend <p>k</p>.</desc>
        <detail>This is the behavior of the <c>%</c> operator in Fortran, C/C++, and Java, <c>rem</c>/<c>remainder</c> in Ada, Haskell, and Scheme.</detail>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("(%s %% %s)", wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
    def apply(k: Double, n: Double): Double = k % n
    def apply(k: Float, n: Float): Float = k % n
    def apply(k: Long, n: Long): Long = k % n
    def apply(k: Int, n: Int): Int = k % n
  }
  provide(Remainder)

  ////   ** (Pow)
  object Pow extends LibFcn {
    val name = "**"
    val sig = Sig(List("x" -> P.Wildcard("A", anyNumber), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Raise <p>x</p> to the power <p>n</p>.</desc>{describeOverflowErrors}
      </doc>
    def apply(x: Double, n: Double): Double = java.lang.Math.pow(x, n)
    def apply(x: Float, n: Float): Float = java.lang.Math.pow(x, n).toFloat
    def apply(x: Long, n: Long): Long = Round.apply(java.lang.Math.pow(x, n))
    def apply(x: Int, n: Int): Int = Round.apply(java.lang.Math.pow(x, n).toFloat).toInt
  }
  provide(Pow)

  //////////////////////////////////////////////////////////////////// generic comparison operators

  ////   cmp (Comparison)
  object Comparison extends LibFcn {
    val name = "cmp"
    val sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Int)
    val doc =
      <doc>
        <desc>Return <c>1</c> if <p>x</p> is greater than <p>y</p>, <c>-1</c> if <p>x</p> is less than <p>y</p>, and <c>0</c> if <p>x</p> and <p>y</p> are equal.</desc>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalComparison")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.Comparison(%s))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("%s.apply(%s)", javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString, wrapArgs(args, paramTypes, false))
  }
  provide(Comparison)

  ////   == (Equal)
  object Equal extends LibFcn {
    val name = "=="
    val sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is equal to <p>y</p>, <c>false</c> otherwise.</desc>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalEQ")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonOperator(%s, 1))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(Equal)

  ////   >= (GreaterOrEqual)
  object GreaterOrEqual extends LibFcn {
    val name = ">="
    val sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is greater than or equal to <p>y</p>, <c>false</c> otherwise.</desc>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalGE")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonOperator(%s, 2))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(GreaterOrEqual)

  ////   > (GreaterThan)
  object GreaterThan extends LibFcn {
    val name = ">"
    val sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is greater than <p>y</p>, <c>false</c> otherwise.</desc>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalGT")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonOperator(%s, 3))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(GreaterThan)

  ////   != (NotEqual)
  object NotEqual extends LibFcn {
    val name = "!="
    val sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is not equal to <p>y</p>, <c>false</c> otherwise.</desc>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalNE")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonOperator(%s, -1))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(NotEqual)

  ////   < (LessThan)
  object LessThan extends LibFcn {
    val name = "<"
    val sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is less than <p>y</p>, <c>false</c> otherwise.</desc>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalLT")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonOperator(%s, -2))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(LessThan)

  ////   <= (LessOrEqual)
  object LessOrEqual extends LibFcn {
    val name = "<="
    val sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is less than or equal to <p>y</p>, <c>false</c> otherwise.</desc>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.params.head match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => 
        JavaCode("com.opendatagroup.hadrian.data.NumericalLE")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonOperator(%s, -3))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
  }
  provide(LessOrEqual)

  //////////////////////////////////////////////////////////////////// max and min

  ////   max (Max)
  object Max extends LibFcn {
    val name = "max"
    val sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return <p>x</p> if <p>x</p> {"\u2265"} <p>y</p>, <p>y</p> otherwise.</desc>
        <detail>For the maximum of more than two values, see <f>a.max</f></detail>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.ret match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble =>
        JavaCode("com.opendatagroup.hadrian.data.NumericalMax")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonMax(%s))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode = retType match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble =>
        super.javaCode(args, argContext, paramTypes, retType)
      case _ =>
        JavaCode("((%s)(%s.apply((Object)%s, (Object)%s)))",
          javaType(retType, true, true, false),
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
    }
  }
  provide(Max)

  ////   min (Min)
  object Min extends LibFcn {
    val name = "min"
    val sig = Sig(List("x" -> P.Wildcard("A"), "y" -> P.Wildcard("A")), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return <p>x</p> if <p>x</p> &lt; <p>y</p>, <p>y</p> otherwise.</desc>
        <detail>For the minimum of more than two values, see <f>a.min</f></detail>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.ret match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble =>
        JavaCode("com.opendatagroup.hadrian.data.NumericalMin")
      case x: AvroType =>
        JavaCode("(new com.opendatagroup.hadrian.data.ComparisonMin(%s))", javaSchema(x, false))
      case _ =>
        throw new Exception  // should never be called
    }
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode = retType match {
      case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble =>
        super.javaCode(args, argContext, paramTypes, retType)
      case _ =>
        JavaCode("((%s)(%s.apply((Object)%s, (Object)%s)))",
          javaType(retType, true, true, false),
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true), wrapArg(1, args, paramTypes, true))
    }
  }
  provide(Min)

  //////////////////////////////////////////////////////////////////// logical operators

  ////   && (LogicalAnd)
  object LogicalAnd extends LibFcn {
    val name = "&&"
    val sig = Sig(List("x" -> P.Boolean, "y" -> P.Boolean), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> and <p>y</p> are both <c>true</c>, <c>false</c> otherwise.</desc>
        <detail>If <p>x</p> is <c>false</c>, <p>y</p> won't be evaluated.  (Only relevant for arguments with side effects.)</detail>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("(%s && %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: => Boolean, y: => Boolean): Boolean = x && y
  }
  provide(LogicalAnd)

  ////   || (LogicalOr)
  object LogicalOr extends LibFcn {
    val name = "||"
    val sig = Sig(List("x" -> P.Boolean, "y" -> P.Boolean), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if either <p>x</p> or <p>y</p> (or both) are <c>true</c>, <c>false</c> otherwise.</desc>
        <detail>If <p>x</p> is <c>true</c>, <p>y</p> won't be evaluated.  (Only relevant for arguments with side effects.)</detail>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("(%s || %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: => Boolean, y: => Boolean): Boolean = x || y
  }
  provide(LogicalOr)

  ////   ^^ (LogicalXOr)
  object LogicalXOr extends LibFcn {
    val name = "^^"
    val sig = Sig(List("x" -> P.Boolean, "y" -> P.Boolean), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is <c>true</c> and <p>y</p> is <c>false</c> or if <p>x</p> is <c>false</c> and <p>y</p> is <c>true</c>, but return <c>false</c> for any other case.</desc>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("(((%s ? 1 : 0) ^ (%s ? 1 : 0)) != 0)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: Boolean, y: Boolean): Boolean = (x && !y) || (y && !x)
  }
  provide(LogicalXOr)

  ////   ! (LogicalNot)
  object LogicalNot extends LibFcn {
    val name = "!"
    val sig = Sig(List("x" -> P.Boolean), P.Boolean)
    val doc =
      <doc>
        <desc>Return <c>true</c> if <p>x</p> is <c>false</c> and <c>false</c> if <p>x</p> is <c>true</c>.</desc>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("(!%s)", wrapArg(0, args, paramTypes, false).toString)
    def apply(x: Boolean): Boolean = !x
  }
  provide(LogicalNot)

  //////////////////////////////////////////////////////////////////// bitwise arithmetic

  ////   & (BitwiseAnd)
  object BitwiseAnd extends LibFcn {
    val name = "&"
    val sig = Sigs(List(Sig(List("x" -> P.Int, "y" -> P.Int), P.Int),
                        Sig(List("x" -> P.Long, "y" -> P.Long), P.Long)))
    val doc =
      <doc>
        <desc>Calculate the bitwise-and of <p>x</p> and <p>y</p>.</desc>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("(%s & %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: Int, y: Int): Int = x & y
    def apply(x: Long, y: Long): Long = x & y
  }
  provide(BitwiseAnd)

  ////   | (BitwiseOr)
  object BitwiseOr extends LibFcn {
    val name = "|"
    val sig = Sigs(List(Sig(List("x" -> P.Int, "y" -> P.Int), P.Int),
                        Sig(List("x" -> P.Long, "y" -> P.Long), P.Long)))
    val doc =
      <doc>
        <desc>Calculate the bitwise-or of <p>x</p> and <p>y</p>.</desc>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("(%s | %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: Int, y: Int): Int = x | y
    def apply(x: Long, y: Long): Long = x | y
  }
  provide(BitwiseOr)

  ////   ^ (BitwiseXOr)
  object BitwiseXOr extends LibFcn {
    val name = "^"
    val sig = Sigs(List(Sig(List("x" -> P.Int, "y" -> P.Int), P.Int),
                        Sig(List("x" -> P.Long, "y" -> P.Long), P.Long)))
    val doc =
      <doc>
        <desc>Calculate the bitwise-exclusive-or of <p>x</p> and <p>y</p>.</desc>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("(%s ^ %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: Int, y: Int): Int = x ^ y
    def apply(x: Long, y: Long): Long = x ^ y
  }
  provide(BitwiseXOr)

  ////   ~ (BitwiseNot)
  object BitwiseNot extends LibFcn {
    val name = "~"
    val sig = Sigs(List(Sig(List("x" -> P.Int), P.Int),
                        Sig(List("x" -> P.Long), P.Long)))
    val doc =
      <doc>
        <desc>Calculate the bitwise-not of <p>x</p>.</desc>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("(~%s)", wrapArg(0, args, paramTypes, false).toString)
    def apply(x: Int): Int = ~x
    def apply(x: Long): Long = ~x
  }
  provide(BitwiseNot)

}
