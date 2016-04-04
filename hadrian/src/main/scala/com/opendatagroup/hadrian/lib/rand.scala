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

import scala.util.Random
import scala.language.postfixOps
import scala.collection.mutable
import scala.collection.immutable.ListMap

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
import com.opendatagroup.hadrian.signature.PFAVersion
import com.opendatagroup.hadrian.signature.Lifespan

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFARecord

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
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "rand."

  /////////////////////////////////////////////////////////// raw numbers of various types

  ////   int (RandomInt)
  object RandomInt {
    def name = prefix + "int"
    def errcodeBase = 34000
    class Selector(randomGenerator: Random, pos: Option[String]) {
      def apply(): Int = randomGenerator.nextInt()
      def apply(low: Int, high: Int): Int = {
        if (high <= low) throw new PFARuntimeException("high must be greater than low", errcodeBase + 0, name, pos)
        low + randomGenerator.nextInt(high - low)
      }
    }
  }
  class RandomInt(val pos: Option[String] = None) extends LibFcn {
    def name = RandomInt.name
    def sig = Sigs(List(Sig(List(), P.Int),
                        Sig(List("low" -> P.Int, "high" -> P.Int), P.Int)))
    def doc =
      <doc>
        <desc>Return a random integer, either on the entire entire 32-bit range or between <p>low</p> (inclusive) and <p>high</p> (exclusive).</desc>
        <error code={s"${errcodeBase + 0}"}>Raises a "high must be greater than low" error if <p>high</p> is less than or equal to <p>low</p>.</error>
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = RandomInt.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$Selector(randomGenerator(), " + posToJava + "))")
  }
  provide(new RandomInt)

  ////   long (RandomLong)
  object RandomLong {
    def name = prefix + "long"
    def errcodeBase = 34010
    class Selector(randomGenerator: Random, pos: Option[String]) {
      def apply(): Long = randomGenerator.nextLong()
      def apply(low: Long, high: Long): Long = {
        if (high <= low) throw new PFARuntimeException("high must be greater than low", errcodeBase + 0, name, pos)
        low + Math.abs(randomGenerator.nextLong()) % (high - low)
      }
    }
  }
  class RandomLong(val pos: Option[String] = None) extends LibFcn {
    def name = RandomLong.name
    def sig = Sigs(List(Sig(List(), P.Long),
                        Sig(List("low" -> P.Long, "high" -> P.Long), P.Long)))
    def doc =
      <doc>
        <desc>Return a random long integer, either on the entire 64-bit range or between <p>low</p> (inclusive) and <p>high</p> (exclusive).</desc>
        <error code={s"${errcodeBase + 0}"}>Raises a "high must be greater than low" error if <p>high</p> is less than or equal to <p>low</p>.</error>
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = RandomLong.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$Selector(randomGenerator(), " + posToJava + "))")
  }
  provide(new RandomLong)

  ////   float (RandomFloat)
  object RandomFloat {
    def name = prefix + "float"
    def errcodeBase = 34020
    class Selector(randomGenerator: Random, pos: Option[String]) {
      def apply(low: Float, high: Float): Float = {
        if (high <= low) throw new PFARuntimeException("high must be greater than low", errcodeBase + 0, name, pos)
        low + randomGenerator.nextFloat() * (high - low)
      }
    }
  }
  class RandomFloat(val pos: Option[String] = None) extends LibFcn {
    def name = RandomFloat.name
    def sig = Sig(List("low" -> P.Float, "high" -> P.Float), P.Float)
    def doc =
      <doc>
        <desc>Return a random float between <p>low</p> and <p>high</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises a "high must be greater than low" error if <p>high</p> is less than or equal to <p>low</p>.</error>
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = RandomFloat.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$Selector(randomGenerator(), " + posToJava + "))")
  }
  provide(new RandomFloat)

  ////   double (RandomDouble)
  object RandomDouble {
    def name = prefix + "double"
    def errcodeBase = 34030
    class Selector(randomGenerator: Random, pos: Option[String]) {
      def apply(low: Double, high: Double): Double = {
        if (high <= low) throw new PFARuntimeException("high must be greater than low", errcodeBase + 0, name, pos)
        low + randomGenerator.nextDouble() * (high - low)
      }
    }
  }
  class RandomDouble(val pos: Option[String] = None) extends LibFcn {
    def name = RandomDouble.name
    def sig = Sig(List("low" -> P.Double, "high" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return a random double between <p>low</p> and <p>high</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises a "high must be greater than low" error if <p>high</p> is less than or equal to <p>low</p>.</error>
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = RandomDouble.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$Selector(randomGenerator(), " + posToJava + "))")
  }
  provide(new RandomDouble)

  /////////////////////////////////////////////////////////// items from arrays

  ////   choice (RandomChoice)
  object RandomChoice {
    def name = prefix + "choice"
    def errcodeBase = 34040
    class Selector(randomGenerator: Random, pos: Option[String]) {
      def apply[X](population: PFAArray[X]): X = {
        val vector = population.toVector
        if (vector.isEmpty)
          throw new PFARuntimeException("population must not be empty", errcodeBase + 0, name, pos)
        vector(randomGenerator.nextInt(vector.size))
      }
    }
  }
  class RandomChoice(val pos: Option[String] = None) extends LibFcn {
    def name = RandomChoice.name
    def sig = Sig(List("population" -> P.Array(P.Wildcard("A"))), P.Wildcard("A"))
    def doc =
      <doc>
        <desc>Return a random item from a bag of items.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises a "population must not be empty" error if <p>population</p> is empty.</error>
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = RandomChoice.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$Selector(randomGenerator(), " + posToJava + "))")
  }
  provide(new RandomChoice)

  ////   choices (RandomChoices)
  object RandomChoices {
    def name = prefix + "choices"
    def errcodeBase = 34050
    class Selector(randomGenerator: Random, pos: Option[String]) {
      def apply[X](size: Int, population: PFAArray[X]): PFAArray[X] = {
        val vector = population.toVector
        if (vector.isEmpty)
          throw new PFARuntimeException("population must not be empty", errcodeBase + 0, name, pos)
        PFAArray.fromVector(Vector.fill(size)(vector(randomGenerator.nextInt(vector.size))))
      }
    }
  }
  class RandomChoices(val pos: Option[String] = None) extends LibFcn {
    def name = RandomChoices.name
    def sig = Sig(List("size" -> P.Int, "population" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return an array of random items (with replacement) from a bag of items.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises a "population must not be empty" error if <p>population</p> is empty.</error>
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = RandomChoices.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$Selector(randomGenerator(), " + posToJava + "))")
  }
  provide(new RandomChoices)

  ////   sample (RandomSample)
  object RandomSample {
    def name = prefix + "sample"
    def errcodeBase = 34060
    class Selector(randomGenerator: Random, pos: Option[String]) {
      def apply[X](size: Int, population: PFAArray[X]): PFAArray[X] = {
        val vector = population.toVector
        if (vector.isEmpty)
          throw new PFARuntimeException("population must not be empty", errcodeBase + 0, name, pos)
        if (vector.size < size)
          throw new PFARuntimeException("population smaller than requested subsample", errcodeBase + 1, name, pos)
        val indexes = mutable.Set[Int]()
        while (indexes.size < size)
          indexes += randomGenerator.nextInt(vector.size)
        PFAArray.fromVector(indexes.toVector.map(vector(_)))
      }
    }
  }
  class RandomSample(val pos: Option[String] = None) extends LibFcn {
    def name = RandomSample.name
    def sig = Sig(List("size" -> P.Int, "population" -> P.Array(P.Wildcard("A"))), P.Array(P.Wildcard("A")))
    def doc =
      <doc>
        <desc>Return an array of random items (without replacement) from a bag of items.</desc>
        <error code={s"${errcodeBase + 0}"}>Raises a "population must not be empty" error if <p>population</p> is empty.</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "population smaller than requested subsample" error if the size of <p>population</p> is less than <p>size</p>.</error>
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = RandomSample.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$Selector(randomGenerator(), " + posToJava + "))")
  }
  provide(new RandomSample)

  /////////////////////////////////////////////////////////// deviates from a histogram

  ////   histogram (RandomHistogram)
  object RandomHistogram {
    def name = prefix + "histogram"
    def errcodeBase = 34070
    def selectIndex(distribution: Vector[Double], randomGenerator: Random, pos: Option[String]): Int = {
      val cumulativeSum = distribution.scanLeft(0.0)({(x, y) =>
        if (java.lang.Double.isInfinite(y)  ||  java.lang.Double.isNaN(y))
          throw new PFARuntimeException("distribution must be finite", errcodeBase + 1, name, pos)
        else if (y < 0.0)
          throw new PFARuntimeException("distribution must be non-negative", errcodeBase + 2, name, pos)
        else
          x + y})
      val total = cumulativeSum.last
      if (total == 0.0)
        throw new PFARuntimeException("distribution must be non-empty", errcodeBase + 0, name, pos)
      val position = randomGenerator.nextDouble() * total
      cumulativeSum.indexWhere(position < _) - 1
    }
    class SelectorIndex(randomGenerator: Random, pos: Option[String]) {
      def apply(distribution: PFAArray[Double]): Int =
        selectIndex(distribution.toVector, randomGenerator, pos)
    }
    class SelectorRecord(randomGenerator: Random, pos: Option[String]) {
      def apply(distribution: PFAArray[PFARecord]): PFARecord = {
        val vector = distribution.toVector
        val probs = vector map {_.get("prob").asInstanceOf[Double]}
        val index = selectIndex(probs, randomGenerator, pos)
        vector(index)
      }
    }
  }
  class RandomHistogram(val pos: Option[String] = None) extends LibFcn {
    def name = RandomHistogram.name
    def sig = Sigs(List(Sig(List("distribution" -> P.Array(P.Double)), P.Int),
                        Sig(List("distribution" -> P.Array(P.WildRecord("A", ListMap("prob" -> P.Double)))), P.Wildcard("A"))))
    def doc =
      <doc>
        <desc>Return a random index of <p>distribution</p> with probability proportional to the value of that index or a random item from <p>distribution</p> with probability proportional to the <pf>prob</pf> field.</desc>
        <detail>If the probabilities do not sum to 1.0, they will be normalized first.</detail>
        <error code={s"${errcodeBase + 0}"}>Raises a "distribution must be non-empty" error if no items of <p>distribution</p> are non-zero.</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "distribution must be finite" error if any items of <p>distribution</p> are infinite or <c>NaN</c>.</error>
        <error code={s"${errcodeBase + 2}"}>Raises a "distribution must be non-negative" error if any items of <p>distribution</p> are negative.</error>
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = RandomHistogram.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.ret match {
      case AvroInt() =>
        JavaCode("(new " + this.getClass.getName + "$SelectorIndex(randomGenerator(), " + posToJava + "))")
      case AvroRecord(_, _, _, _, _) =>
        JavaCode("(new " + this.getClass.getName + "$SelectorRecord(randomGenerator(), " + posToJava + "))")
    }
  }
  provide(new RandomHistogram)

  /////////////////////////////////////////////////////////// strings and byte arrays

  ////   string (RandomString)
  object RandomString {
    def name = prefix + "string"
    def errcodeBase = 34080
    class Selector(randomGenerator: Random, pos: Option[String]) {
      def apply(size: Int): String = {
        if (size <= 0) throw new PFARuntimeException("size must be positive", errcodeBase + 0, name, pos)
        List.fill(size)((randomGenerator.nextInt(0xD800 - 1) + 1).toChar).mkString
      }
      def apply(size: Int, population: String): String = {
        if (size <= 0) throw new PFARuntimeException("size must be positive", errcodeBase + 0, name, pos)
        if (population.isEmpty) throw new PFARuntimeException("population must be non-empty", errcodeBase + 3, name, pos)
        List.fill(size)(population(randomGenerator.nextInt(population.size))).mkString
      }
      def apply(size: Int, low: Int, high: Int): String = {
        if (size <= 0) throw new PFARuntimeException("size must be positive", errcodeBase + 0, name, pos)
        if (high <= low) throw new PFARuntimeException("high must be greater than low", errcodeBase + 1, name, pos)
        if (low < 1  ||  low > 0xD800  ||  high < 1  || high > 0xD800) throw new PFARuntimeException("invalid char", errcodeBase + 2, name, pos)
        List.fill(size)((randomGenerator.nextInt(high - low) + low).toChar).mkString
      }
    }
  }
  class RandomString(val pos: Option[String] = None) extends LibFcn {
    def name = RandomString.name
    def sig = Sigs(List(Sig(List("size" -> P.Int), P.String),
                        Sig(List("size" -> P.Int, "population" -> P.String), P.String),
                        Sig(List("size" -> P.Int, "low" -> P.Int, "high" -> P.Int), P.String)))
    def doc =
      <doc>
        <desc>Return a random string with <p>size</p> characters from a range, if provided.</desc>
        <param name="size">Number of characters in the resulting string.</param>
        <param name="population">Bag of characters to choose from.  Characters repeated <m>N</m> times in the <p>population</p> have probability <m>N</m>/<p>size</p>, but order is irrelevant.</param>
        <param name="low">Minimum code-point to sample (inclusive).</param>
        <param name="high">Maximum code-point to sample (exclusive).</param>
        <detail>Without a range, this function samples the entire Unicode table up to and including <c>0xD800</c>; ASCII characters are rare.</detail>
        <detail>The ASCII printable range is <p>low</p> = 33, <p>high</p> = 127.</detail>
        <error code={s"${errcodeBase + 0}"}>Raises a "size must be positive" error if <p>size</p> is less than or equal to zero.</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "high must be greater than low" error if <p>high</p> is less than or equal to <p>low</p>.</error>
        <error code={s"${errcodeBase + 2}"}>Raises an "invalid char" error if <p>low</p> is less than 1 or greater than <c>0xD800</c> or if <p>high</p> is less than 1 or greater than <c>0xD800</c>.</error>
        <error code={s"${errcodeBase + 3}"}>Raises an "population must be non-empty" error if <p>population</p> is empty.</error>
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = RandomString.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$Selector(randomGenerator(), " + posToJava + "))")
  }
  provide(new RandomString)

  ////   bytes (RandomBytes)
  object RandomBytes {
    def name = prefix + "bytes"
    def errcodeBase = 34090
    class Selector(randomGenerator: Random, pos: Option[String]) {
      def apply(size: Int): Array[Byte] = {
        if (size <= 0) throw new PFARuntimeException("size must be positive", errcodeBase + 0, name, pos)
        val out = Array.fill[Byte](size)(0)
        randomGenerator.nextBytes(out)
        out
      }
      def apply(size: Int, population: Array[Byte]): Array[Byte] = {
        if (size <= 0) throw new PFARuntimeException("size must be positive", errcodeBase + 0, name, pos)
        if (population.isEmpty) throw new PFARuntimeException("population must be non-empty", errcodeBase + 3, name, pos)
        Array.fill(size)(population(randomGenerator.nextInt(population.size)))
      }
      def apply(size: Int, low: Int, high: Int): Array[Byte] = {
        if (size <= 0) throw new PFARuntimeException("size must be positive", errcodeBase + 0, name, pos)
        if (high <= low) throw new PFARuntimeException("high must be greater than low", errcodeBase + 1, name, pos)
        if (low < 0  ||  low > 255  ||  high < 0  ||  high > 256)
           throw new PFARuntimeException("invalid byte", errcodeBase + 2, name, pos)
        Array.fill(size)((randomGenerator.nextInt(high - low) + low).toByte)
      }
    }
  }
  class RandomBytes(val pos: Option[String] = None) extends LibFcn {
    def name = RandomBytes.name
    def sig = Sigs(List(Sig(List("size" -> P.Int), P.Bytes),
                        Sig(List("size" -> P.Int, "population" -> P.Bytes), P.Bytes),
                        Sig(List("size" -> P.Int, "low" -> P.Int, "high" -> P.Int), P.Bytes)))
    def doc =
      <doc>
        <desc>Return <p>size</p> random bytes from a range, if provided.</desc>
        <param name="size">Number of bytes in the result.</param>
        <param name="population">Bag of bytes to choose from.  Bytes repeated <m>N</m> times in the <p>population</p> have probability <m>N</m>/<p>size</p>, but order is irrelevant.</param>
        <param name="low">Minimum byte value to sample (inclusive).</param>
        <param name="high">Maximum byte value to sample (exclusive).</param>
        <error code={s"${errcodeBase + 0}"}>Raises a "size must be positive" error if <p>size</p> is less than or equal to zero.</error>
        <error code={s"${errcodeBase + 1}"}>Raises a "high must be greater than low" error if <p>high</p> is less than or equal to <p>low</p>.</error>
        <error code={s"${errcodeBase + 2}"}>Raises an "invalid byte" error if <p>low</p> is less than 0 or greater than 255 or if <p>high</p> is less than 0 or greater than 256.</error>
        <error code={s"${errcodeBase + 3}"}>Raises an "population must be non-empty" error if <p>population</p> is empty.</error>
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = RandomBytes.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$Selector(randomGenerator(), " + posToJava + "))")
  }
  provide(new RandomBytes)

  ////   uuid (RandomUUID)
  object RandomUUID {
    def name = prefix + "uuid"
    def errcodeBase = 34100
    class Selector(randomGenerator: Random, pos: Option[String]) {
      def apply(): String = {
        val tmp = randomGenerator.nextLong.toHexString + randomGenerator.nextLong.toHexString
        tmp.substring(0, 8) + "-" + tmp.substring(8, 12) + "-4" + tmp.substring(13, 16) + "-8" + tmp.substring(17, 20) + "-" + tmp.substring(20)
      }
    }
  }
  class RandomUUID(val pos: Option[String] = None) extends LibFcn {
    def name = RandomUUID.name
    def sig = Sig(List(), P.String, Lifespan(None, Some(PFAVersion(0, 7, 2)), Some(PFAVersion(0, 9, 0)), Some("use rand.uuid4 instead")))
    def doc =
      <doc>
        <desc>Return a random (type 4) UUID with IETF variant (8).</desc>
        <ret>The return value is a string with the form <c>xxxxxxxx-xxxx-4xxx-8xxx-xxxxxxxxxxxx</c> where <c>x</c> are random, lowercase hexidecimal digits (0-9a-f), 4 is the version, and 8 is the IETF variant.</ret>
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = RandomUUID.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$Selector(randomGenerator(), " + posToJava + "))")
  }
  provide(new RandomUUID)

  ////   uuid4 (RandomUUID4)
  object RandomUUID4 {
    def name = prefix + "uuid4"
    def errcodeBase = 34110
    class Selector(randomGenerator: Random, pos: Option[String]) {
      def apply(): String = {
        val tmp = randomGenerator.nextLong.toHexString + randomGenerator.nextLong.toHexString
        tmp.substring(0, 8) + "-" + tmp.substring(8, 12) + "-4" + tmp.substring(13, 16) + "-8" + tmp.substring(17, 20) + "-" + tmp.substring(20)
      }
    }
  }
  class RandomUUID4(val pos: Option[String] = None) extends LibFcn {
    def name = RandomUUID4.name
    def sig = Sig(List(), P.String)
    def doc =
      <doc>
        <desc>Return a random (type 4) UUID with IETF variant (8).</desc>
        <ret>The return value is a string with the form <c>xxxxxxxx-xxxx-4xxx-8xxx-xxxxxxxxxxxx</c> where <c>x</c> are random, lowercase hexidecimal digits (0-9a-f), 4 is the version, and 8 is the IETF variant.</ret>
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = RandomUUID4.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$Selector(randomGenerator(), " + posToJava + "))")
  }
  provide(new RandomUUID4)

  /////////////////////////////////////////////////////////// common probability distributions

  ////   gaussian (Gaussian)
  object Gaussian {
    def name = prefix + "gaussian"
    def errcodeBase = 34120
    class Selector(randomGenerator: Random, pos: Option[String]) {
      def apply(mu: Double, sigma: Double): Double = (randomGenerator.nextGaussian() * sigma) + mu
    }
  }
  class Gaussian(val pos: Option[String] = None) extends LibFcn {
    def name = Gaussian.name
    def sig = Sig(List("mu" -> P.Double, "sigma" -> P.Double), P.Double)
    def doc =
      <doc>
        <desc>Return a random number from a Gaussian (normal) distribution with mean <p>mu</p> and standard deviation <p>sigma</p>.</desc>
        <nondeterministic type="pseudorandom" />
      </doc>
    def errcodeBase = Gaussian.errcodeBase
    override def javaRef(fcnType: FcnType): JavaCode =
      JavaCode("(new " + this.getClass.getName + "$Selector(randomGenerator(), " + posToJava + "))")
  }
  provide(new Gaussian)

}
