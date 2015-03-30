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

import scala.util.Random
import scala.language.postfixOps

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

package object rand {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "rand."

  /////////////////////////////////////////////////////////// raw numbers of various types

  ////   int (RandomInt)
  object RandomInt extends LibFcn {
    val name = prefix + "int"
    val sig = Sigs(List(Sig(List(), P.Int),
                        Sig(List("low" -> P.Int, "high" -> P.Int), P.Int)))
    val doc =
      <doc>
        <desc>Return a random integer, either on the entire entire 32-bit range or between <p>low</p> (inclusive) and <p>high</p> (exclusive).</desc>
        <error>Raises a "high must be greater than low" error if <p>high</p> is less than or equal to <p>low</p>.</error>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "Selector(randomGenerator()))")
    class Selector(randomGenerator: Random) {
      def apply(): Int = randomGenerator.nextInt()
      def apply(low: Int, high: Int): Int = {
        if (high <= low) throw new PFARuntimeException("high must be greater than low")
        low + randomGenerator.nextInt(high - low)
      }
    }
  }
  provide(RandomInt)

  ////   long (RandomLong)
  object RandomLong extends LibFcn {
    val name = prefix + "long"
    val sig = Sigs(List(Sig(List(), P.Long),
                        Sig(List("low" -> P.Long, "high" -> P.Long), P.Long)))
    val doc =
      <doc>
        <desc>Return a random long integer, either on the entire 64-bit range or between <p>low</p> (inclusive) and <p>high</p> (exclusive).</desc>
        <error>Raises a "high must be greater than low" error if <p>high</p> is less than or equal to <p>low</p>.</error>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "Selector(randomGenerator()))")
    class Selector(randomGenerator: Random) {
      def apply(): Long = randomGenerator.nextLong()
      def apply(low: Long, high: Long): Long = {
        if (high <= low) throw new PFARuntimeException("high must be greater than low")
        low + Math.abs(randomGenerator.nextLong()) % (high - low)
      }
    }
  }
  provide(RandomLong)

  ////   float (RandomFloat)
  object RandomFloat extends LibFcn {
    val name = prefix + "float"
    val sig = Sig(List("low" -> P.Float, "high" -> P.Float), P.Float)
    val doc =
      <doc>
        <desc>Return a random float between <p>low</p> and <p>high</p>.</desc>
        <error>Raises a "high must be greater than low" error if <p>high</p> is less than or equal to <p>low</p>.</error>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "Selector(randomGenerator()))")
    class Selector(randomGenerator: Random) {
      def apply(low: Float, high: Float): Float = {
        if (high <= low) throw new PFARuntimeException("high must be greater than low")
        low + randomGenerator.nextFloat() * (high - low)
      }
    }
  }
  provide(RandomFloat)

  ////   double (RandomDouble)
  object RandomDouble extends LibFcn {
    val name = prefix + "double"
    val sig = Sig(List("low" -> P.Double, "high" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return a random double between <p>low</p> and <p>high</p>.</desc>
        <error>Raises a "high must be greater than low" error if <p>high</p> is less than or equal to <p>low</p>.</error>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "Selector(randomGenerator()))")
    class Selector(randomGenerator: Random) {
      def apply(low: Double, high: Double): Double = {
        if (high <= low) throw new PFARuntimeException("high must be greater than low")
        low + randomGenerator.nextDouble() * (high - low)
      }
    }
  }
  provide(RandomDouble)

  /////////////////////////////////////////////////////////// strings and byte arrays

  ////   string (RandomString)
  object RandomString extends LibFcn {
    val name = prefix + "string"
    val sig = Sigs(List(Sig(List("size" -> P.Int), P.String),
                        Sig(List("size" -> P.Int, "population" -> P.String), P.String),
                        Sig(List("size" -> P.Int, "low" -> P.Int, "high" -> P.Int), P.String)))
    val doc =
      <doc>
        <desc>Return a random string with <p>size</p> characters from a range, if provided.</desc>
        <param name="size">Number of characters in the resulting string.</param>
        <param name="population">Bag of characters to choose from.  Characters repeated <m>N</m> times in the <p>population</p> have probability <m>N</m>/<p>size</p>, but order is irrelevant.</param>
        <param name="low">Minimum code-point to sample (inclusive).</param>
        <param name="high">Maximum code-point to sample (exclusive).</param>
        <detail>Without a range, this function samples the entire Unicode table up to and including <c>0xD800</c>; ASCII characters are rare.</detail>
        <detail>The ASCII printable range is <p>low</p> = 33, <p>high</p> = 127.</detail>
        <error>Raises a "size must be positive" error if <p>size</p> is less than or equal to zero.</error>
        <error>Raises a "high must be greater than low" error if <p>high</p> is less than or equal to <p>low</p>.</error>
        <error>Raises an "invalid char" error if <p>low</p> is less than 1 or greater than <c>0xD800</c> or if <p>high</p> is less than 1 or greater than <c>0xD800</c>.</error>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "Selector(randomGenerator()))")
    class Selector(randomGenerator: Random) {
      def apply(size: Int): String = {
        if (size <= 0) throw new PFARuntimeException("size must be positive")
        List.fill(size)((randomGenerator.nextInt(0xD800 - 1) + 1).toChar).mkString
      }
      def apply(size: Int, population: String): String = {
        if (size <= 0) throw new PFARuntimeException("size must be positive")
        List.fill(size)(population(randomGenerator.nextInt(population.size))).mkString
      }
      def apply(size: Int, low: Int, high: Int): String = {
        if (size <= 0) throw new PFARuntimeException("size must be positive")
        if (high <= low) throw new PFARuntimeException("high must be greater than low")
        List.fill(size)((randomGenerator.nextInt(high - low) + low).toChar).mkString
      }
    }
  }
  provide(RandomString)

  ////   bytes (RandomBytes)
  object RandomBytes extends LibFcn {
    val name = prefix + "bytes"
    val sig = Sigs(List(Sig(List("size" -> P.Int), P.Bytes),
                        Sig(List("size" -> P.Int, "population" -> P.Bytes), P.Bytes),
                        Sig(List("size" -> P.Int, "low" -> P.Int, "high" -> P.Int), P.Bytes)))
    val doc =
      <doc>
        <desc>Return <p>size</p> random bytes from a range, if provided.</desc>
        <param name="size">Number of bytes in the result.</param>
        <param name="population">Bag of bytes to choose from.  Bytes repeated <m>N</m> times in the <p>population</p> have probability <m>N</m>/<p>size</p>, but order is irrelevant.</param>
        <param name="low">Minimum byte value to sample (inclusive).</param>
        <param name="high">Maximum byte value to sample (exclusive).</param>
        <error>Raises a "size must be positive" error if <p>size</p> is less than or equal to zero.</error>
        <error>Raises a "high must be greater than low" error if <p>high</p> is less than or equal to <p>low</p>.</error>
        <error>Raises an "invalid byte" error if <p>low</p> is less than 0 or greater than 255 or if <p>high</p> is less than 0 or greater than 256.</error>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "Selector(randomGenerator()))")
    class Selector(randomGenerator: Random) {
      def apply(size: Int): Array[Byte] = {
        if (size <= 0) throw new PFARuntimeException("size must be positive")
        val out = Array.fill[Byte](size)(0)
        randomGenerator.nextBytes(out)
        out
      }
      def apply(size: Int, population: Array[Byte]): Array[Byte] = {
        if (size <= 0) throw new PFARuntimeException("size must be positive")
        Array.fill(size)(population(randomGenerator.nextInt(population.size)))
      }
      def apply(size: Int, low: Int, high: Int): Array[Byte] = {
        if (size <= 0) throw new PFARuntimeException("size must be positive")
        if (high <= low) throw new PFARuntimeException("high must be greater than low")
        if (low < 0  ||  low > 255  ||  high < 0  ||  high > 256)
           throw new PFARuntimeException("invalid byte")
        Array.fill(size)((randomGenerator.nextInt(high - low) + low).toByte)
      }
    }
  }
  provide(RandomBytes)

  ////   uuid (RandomUUID)
  object RandomUUID extends LibFcn {
    val name = prefix + "uuid"
    val sig = Sig(List(), P.String)
    val doc =
      <doc>
        <desc>Return a random (type 4) UUID with IETF variant (8).</desc>
        <ret>The return value is a string with the form <c>xxxxxxxx-xxxx-4xxx-8xxx-xxxxxxxxxxxx</c> where <c>x</c> are random, lowercase hexidecimal digits (0-9a-f), 4 is the version, and 8 is the IETF variant.</ret>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "Selector(randomGenerator()))")
    class Selector(randomGenerator: Random) {
      def apply(): String = {
        val tmp = randomGenerator.nextLong.toHexString + randomGenerator.nextLong.toHexString
        tmp.substring(0, 8) + "-" + tmp.substring(8, 12) + "-4" + tmp.substring(13, 16) + "-8" + tmp.substring(17, 20) + "-" + tmp.substring(20)
      }
    }
  }
  provide(RandomUUID)

  /////////////////////////////////////////////////////////// common probability distributions

  ////   gaussian (Gaussian)
  object Gaussian extends LibFcn {
    val name = prefix + "gaussian"
    val sig = Sig(List("mu" -> P.Double, "sigma" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Return a random number from a Gaussian (normal) distribution with mean <p>mu</p> and standard deviation <p>sigma</p>.</desc>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "Selector(randomGenerator()))")
    class Selector(randomGenerator: Random) {
      def apply(mu: Double, sigma: Double): Double = (randomGenerator.nextGaussian() * sigma) + mu
    }
  }
  provide(Gaussian)

}
