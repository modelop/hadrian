package com.opendatagroup.hadrian.lib1

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema

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
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "m."

  //////////////////////////////////////////////////////////////////// constants (0-arity side-effect free functions)

  ////   pi (Pi)
  object Pi extends LibFcn {
    val name = prefix + "pi"
    val sig = Sig(List(), P.Double)
    val doc =
      <doc>
        <desc>The double-precision number that is closer than any other to <m>\pi</m>, the ratio of a circumference of a circle to its diameter.</desc>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.PI")
    def apply(): Double = java.lang.Math.PI
  }
  provide(Pi)

  ////   e (E)
  object E extends LibFcn {
    val name = prefix + "e"
    val sig = Sig(List(), P.Double)
    val doc =
      <doc>
        <desc>The double-precision number that is closer than any other to <m>e</m>, the base of natural logarithms.</desc>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.E")
    def apply(): Double = java.lang.Math.E
  }
  provide(E)

  //////////////////////////////////////////////////////////////////// functions (alphabetical order)

  def domain(low: String, high: String, lowInclusive: String = "", highInclusive: String = " (inclusive)",
    result: scala.xml.Node = <x>Beyond this domain, the result is <c>NaN</c>, not an exception (see IEEE 754).</x>.child.head,
    ensureFinite: scala.xml.Node = <x>Use <f>impute.ensureFinite</f> to produce errors from infinite or <c>NaN</c> values."</x>.child.head) =
      <detail>The domain of this function is from {low}{lowInclusive} to {high}{highInclusive}.  {result}  {ensureFinite}</detail>

  def wholeLine(tpe: String = " real") = <detail>The domain of this function is the whole{tpe} line; no input is invalid.</detail>
  def wholePlane(tpe: String = " real") = <detail>The domain of this function is the whole{tpe} plane; no pair of inputs is invalid.</detail>

  val avoidsRoundoff = <detail>Avoids round-off or overflow errors in the intermediate steps.</detail>

  val anyNumber = Set[Type](AvroInt(), AvroLong(), AvroFloat(), AvroDouble())

  ////   abs (Abs)
  object Abs extends LibFcn {
    val name = prefix + "abs"
    val sig = Sig(List("x" -> P.Wildcard("A", anyNumber)), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return the absolute value of <p>x</p>.</desc>{wholeLine()}
        <error>For exactly one integer value, {java.lang.Integer.MIN_VALUE}, this function produces an "int overflow" runtime error.</error>
        <error>For exactly one long value, {java.lang.Long.MIN_VALUE}, this function produces a "long overflow" runtime error.</error>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      if (retType.accepts(AvroFloat()))
        JavaCode("Math.abs(%s)", wrapArg(0, args, paramTypes, false))
      else
        super.javaCode(args, argContext, paramTypes, retType)
    def apply(x: Double): Double = java.lang.Math.abs(x)
    def apply(x: Float): Float = java.lang.Math.abs(x)
    def apply(x: Long): Long = {
      if (x == java.lang.Long.MIN_VALUE)
        throw new PFARuntimeException("long overflow")
      java.lang.Math.abs(x)
    }
    def apply(x: Int): Int = {
      if (x == java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("int overflow")
      java.lang.Math.abs(x)
    }
  }
  provide(Abs)

  ////   acos (ACos)
  object ACos extends LibFcn {
    val name = prefix + "acos"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the arc-cosine (inverse of the cosine function) of <p>x</p> as an angle in radians between <m>0</m> and <m>\pi</m>.</desc>{domain("-1", "1")}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.acos(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.acos(x)
  }
  provide(ACos)

  ////   asin (ASin)
  object ASin extends LibFcn {
    val name = prefix + "asin"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the arc-sine (inverse of the sine function) of <p>x</p> as an angle in radians between <m>-\pi/2</m> and <m>\pi/2</m>.</desc>{domain("-1", "1")}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.asin(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.asin(x)
  }
  provide(ASin)

  ////   atan (ATan)
  object ATan extends LibFcn {
    val name = prefix + "atan"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the arc-tangent (inverse of the tangent function) of <p>x</p> as an angle in radians between <m>-\pi/2</m> and <m>\pi/2</m>.</desc>{wholeLine()}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.atan(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.atan(x)
  }
  provide(ATan)

  ////   atan2 (ATan2)
  object ATan2 extends LibFcn {
    val name = prefix + "atan2"
    val sig = Sig(List("y" -> P.Double, "x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the arc-tangent (inverse of the tangent function) of <p>y</p>/<p>x</p> without loss of precision for small <p>x</p>.</desc>{wholePlane()}
        <detail>Note that <p>y</p> is the first parameter and <p>x</p> is the second parameter.</detail>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.atan2(%s, %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: Double, y: Double): Double = java.lang.Math.atan2(x, y)
  }
  provide(ATan2)

  ////   ceil (Ceil)
  object Ceil extends LibFcn {
    val name = prefix + "ceil"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the smallest (closest to negative infinity, not closest to zero) whole number that is greater than or equal to the input.</desc>{wholeLine()}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.ceil(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.ceil(x)
  }
  provide(Ceil)

  ////   copysign (CopySign)
  object CopySign extends LibFcn {
    val name = prefix + "copysign"
    val sig = Sig(List("mag" -> P.Wildcard("A", anyNumber), "sign" -> P.Wildcard("A")), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Return a number with the magnitude of <p>mag</p> and the sign of <p>sign</p>.</desc>{wholePlane(" real or integer")}
        <details>The return value is positive if <p>mag</p> is not zero and <p>sign</p> is zero.</details>
      </doc>
    def apply(mag: Double, sign: Double): Double = Math.copySign(mag, sign)
    def apply(mag: Float, sign: Float): Float = Math.copySign(mag, sign).toFloat
    def apply(mag: Long, sign: Long): Long = Math.abs(mag) * (if (sign < 0) -1 else 1)
    def apply(mag: Int, sign: Int): Int = Math.abs(mag) * (if (sign < 0) -1 else 1)
  }
  provide(CopySign)

  ////   cos (Cos)
  object Cos extends LibFcn {
    val name = prefix + "cos"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the trigonometric cosine of <p>x</p>, which is assumed to be in radians.</desc>{wholeLine()}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.cos(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.cos(x)
  }
  provide(Cos)

  ////   cosh (CosH)
  object CosH extends LibFcn {
    val name = prefix + "cosh"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the hyperbolic cosine of <p>x</p>, which is equal to <m>{"""\frac{e^x + e^{-x}}{2}"""}</m></desc>{wholeLine()}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.cosh(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.cosh(x)
  }
  provide(CosH)

  ////   exp (Exp)
  object Exp extends LibFcn {
    val name = prefix + "exp"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return <f>m.e</f> raised to the power of <p>x</p>.</desc>{wholeLine()}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.exp(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.exp(x)
  }
  provide(Exp)

  ////   expm1 (ExpM1)
  object ExpM1 extends LibFcn {
    val name = prefix + "expm1"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return <m>e^x - 1</m>.</desc>{avoidsRoundoff}{wholeLine()}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.expm1(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.expm1(x)
  }
  provide(ExpM1)

  ////   floor (Floor)
  object Floor extends LibFcn {
    val name = prefix + "floor"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the largest (closest to positive infinity) whole number that is less than or equal to the input.</desc>{wholeLine()}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.floor(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.floor(x)
  }
  provide(Floor)

  ////   hypot (Hypot)
  object Hypot extends LibFcn {
    val name = prefix + "hypot"
    val sig = Sig(List("x" -> P.Double, "y" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return <m>{"""\sqrt{x^2 + y^2}"""}</m>.</desc>{avoidsRoundoff}{wholeLine()}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.hypot(%s, %s)", wrapArg(0, args, paramTypes, false), wrapArg(1, args, paramTypes, false))
    def apply(x: Double, y: Double): Double = java.lang.Math.hypot(x, y)
  }
  provide(Hypot)

  ////   ln (Ln)
  object Ln extends LibFcn {
    val name = prefix + "ln"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the natural logarithm of <p>x</p>.</desc>
        {domain("0", "infinity", "", " (exclusive)", <x>Given zero, the result is negative infinity, and below zero, the result is <c>NaN</c>, not an exception (see IEEE 754).</x>.child.head)}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.log(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.log(x)
  }
  provide(Ln)

  ////   log10 (Log10)
  object Log10 extends LibFcn {
    val name = prefix + "log10"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the logarithm base 10 of <p>x</p>.</desc>
        {domain("0", "infinity", "", " (exclusive)", <x>Given zero, the result is negative infinity, and below zero, the result is <c>NaN</c>, not an exception (see IEEE 754).</x>.child.head)}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.log10(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.log10(x)
  }
  provide(Log10)

  ////   log (Log)
  object Log extends LibFcn {
    val name = prefix + "log"
    val sig = Sig(List("x" -> P.Double, "base" -> P.Int), P.Double)
    val doc =
      <doc>
        <desc>Return the logarithm of <p>x</p> with a given <p>base</p>.</desc>
        {domain("0", "infinity", "", " (exclusive)", <x>Given zero, the result is negative infinity, and below zero, the result is <c>NaN</c>, not an exception (see IEEE 754).</x>.child.head)}
        <error>If <p>base</p> is less than or equal to zero, this function produces a "base must be positive" runtime error.</error>
      </doc>
    def apply(x: Double, base: Int): Double = {
      if (base <= 0)
        throw new PFARuntimeException("base must be positive")
      java.lang.Math.log(x) / java.lang.Math.log(base)
    }
  }
  provide(Log)

  ////   ln1p (Ln1p)
  object Ln1p extends LibFcn {
    val name = prefix + "ln1p"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return <m>ln(x^2 + 1)</m>.</desc>{avoidsRoundoff}
        {domain("-1", "infinity", "", " (exclusive)", <x>Given -1, the result is negative infinity, and below -1, the result is <c>NaN</c>, not an exception (see IEEE 754).</x>.child.head)}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.log1p(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.log1p(x)
  }
  provide(Ln1p)

  ////   round (Round)
  object Round extends LibFcn {
    val name = prefix + "round"
    val sig = Sigs(List(Sig(List("x" -> P.Float), P.Int),
                        Sig(List("x" -> P.Double), P.Long)))
    val doc =
      <doc>
        <desc>Return the closest whole number to <p>x</p>, rounding up if the fractional part is exactly one-half.</desc>
        <detail>Equal to <f>m.floor</f> of (<p>x</p> + 0.5).</detail>
        <error>Integer results outside of {java.lang.Integer.MIN_VALUE} and {java.lang.Integer.MAX_VALUE} (inclusive) produce an "int overflow" runtime error.</error>
        <error>Long-integer results outside of {java.lang.Long.MIN_VALUE} and {java.lang.Long.MAX_VALUE} (inclusive) produce a "long overflow" runtime error.</error>
      </doc>
    def apply(x: Double): Long =
      if (x > java.lang.Long.MAX_VALUE  ||  x < java.lang.Long.MIN_VALUE)
        throw new PFARuntimeException("long overflow")
      else
        java.lang.Math.round(x)
    def apply(x: Float): Int =
      if (x > java.lang.Integer.MAX_VALUE  ||  x < java.lang.Integer.MIN_VALUE)
        throw new PFARuntimeException("int overflow")
      else
        java.lang.Math.round(x)
  }
  provide(Round)

  ////   rint (RInt)
  object RInt extends LibFcn {
    val name = prefix + "rint"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the closest whole number to <p>x</p>, rounding toward the nearest even number if the fractional part is exactly one-half.</desc>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.rint(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.rint(x)
  }
  provide(RInt)

  ////   signum (Signum)
  object Signum extends LibFcn {
    val name = prefix + "signum"
    val sig = Sig(List("x" -> P.Double), P.Int)
    val doc =
      <doc>
        <desc>Return 0 if <p>x</p> is zero, 1 if <p>x</p> is positive, and -1 if <p>x</p> is negative.</desc>{wholeLine()}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("((int)(Math.signum(%s)))", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Int = java.lang.Math.signum(x).toInt
  }
  provide(Signum)

  ////   sin (Sin)
  object Sin extends LibFcn {
    val name = prefix + "sin"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the trigonometric sine of <p>x</p>, which is assumed to be in radians.</desc>{wholeLine()}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.sin(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.sin(x)
  }
  provide(Sin)

  ////   sinh (SinH)
  object SinH extends LibFcn {
    val name = prefix + "sinh"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the hyperbolic sine of <p>x</p>, which is equal to <m>{"""\frac{e^x - e^{-x}}{2}"""}</m>.</desc>{wholeLine()}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.sinh(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.sinh(x)
  }
  provide(SinH)

  ////   sqrt (Sqrt)
  object Sqrt extends LibFcn {
    val name = prefix + "sqrt"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the positive square root of <p>x</p>.</desc>{domain("0", "infinity", lowInclusive = " (inclusive)", highInclusive = "")}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.sqrt(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.sqrt(x)
  }
  provide(Sqrt)

  ////   tan (Tan)
  object Tan extends LibFcn {
    val name = prefix + "tan"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the trigonometric tangent of <p>x</p>, which is assumed to be in radians.</desc>{wholeLine()}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.tan(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.tan(x)
  }
  provide(Tan)

  ////   tanh (TanH)
  object TanH extends LibFcn {
    val name = prefix + "tanh"
    val sig = Sig(List("x" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return the hyperbolic tangent of <p>x</p>, which is equal to <m>{"""\frac{e^x - e^{-x}}{e^x + e^{-x}}"""}</m>.</desc>{wholeLine()}
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("Math.tanh(%s)", wrapArg(0, args, paramTypes, false))
    def apply(x: Double): Double = java.lang.Math.tanh(x)
  }
  provide(TanH)

}
