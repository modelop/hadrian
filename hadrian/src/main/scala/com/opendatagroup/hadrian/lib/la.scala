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

import org.ejml.simple.SimpleMatrix
import org.ejml.factory.DecompositionFactory
import org.ejml.alg.dense.mult.MatrixDimensionException

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

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAMap

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

package object la {
  private var fcns = ListMap[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "la."

  trait EJMLInterface {
    def vecKeys(x: PFAMap[java.lang.Double]): Set[String] = x.toMap.keySet

    def rowKeys(x: PFAMap[PFAMap[java.lang.Double]]): Set[String] = x.toMap.keySet

    def colKeys(x: PFAMap[PFAMap[java.lang.Double]]): Set[String] =
      if (x.toMap.isEmpty) Set[String]() else (x.toMap.values map {_.toMap.keySet} reduce {_ union _})

    def ragged(x: PFAArray[PFAArray[Double]]): Boolean = {
      val maxcols = (x.toVector map {_.toVector.size}).max
      val mincols = (x.toVector map {_.toVector.size}).min
      maxcols != mincols
    }

    def ragged(x: PFAMap[PFAMap[java.lang.Double]]): Boolean = {
      val xm = x.toMap
      if (xm.isEmpty)
        false
      else {
        val keys = xm.values.head.toMap.keySet
        xm.values.tail.exists {_.toMap.keySet != keys}
      }
    }

    def toDenseVector(x: PFAArray[Double]): SimpleMatrix =
      toDenseVector(x.toVector)

    def toDenseVector(x: Vector[Double]): SimpleMatrix =
      new SimpleMatrix(x map {Array(_)} toArray)

    def toDense(x: PFAArray[PFAArray[Double]]): SimpleMatrix =
      new SimpleMatrix(x.toVector map {_.toVector.toArray} toArray)

    def toDenseVector(x: PFAMap[java.lang.Double], rows: Seq[String]): SimpleMatrix =
      toDenseVector(x.toMap, rows)

    def toDenseVector(x: Map[String, java.lang.Double], rows: Seq[String]): SimpleMatrix =
      new SimpleMatrix(
        (for (r <- rows) yield x.get(r) match {
          case None => Array(0.0)
          case Some(xx) => Array(xx.doubleValue)
        }).toArray)

    def toDense(x: PFAMap[PFAMap[java.lang.Double]], rows: Seq[String], cols: Seq[String]): SimpleMatrix =
      new SimpleMatrix(
        (for (r <- rows) yield
          (for (c <- cols) yield x.toMap.get(r) match {
            case None => 0.0
            case Some(xx) => xx.toMap.get(c) match {
              case None => 0.0
              case Some(xxx) => xxx.doubleValue
            }
          }).toArray).toArray)

    def toArray(x: SimpleMatrix): PFAArray[Double] =
      PFAArray.fromVector(
        (for (i <- 0 until x.numRows) yield
          x.get(i, 0)).toVector)

    def toArrayArray(x: SimpleMatrix): PFAArray[PFAArray[Double]] =
      PFAArray.fromVector(
        (for (i <- 0 until x.numRows) yield
          PFAArray.fromVector(
            (for (j <- 0 until x.numCols) yield
              x.get(i, j)).toVector)).toVector)

    def toMap(x: SimpleMatrix, rows: Seq[String]): PFAMap[java.lang.Double] =
      PFAMap.fromMap(
        (for ((r, i) <- rows.zipWithIndex) yield
          (r, java.lang.Double.valueOf(x.get(i, 0)))).toMap)

    def toMapMap(x: SimpleMatrix, rows: Seq[String], cols: Seq[String]): PFAMap[PFAMap[java.lang.Double]] =
      PFAMap.fromMap(
        (for ((r, i) <- rows.zipWithIndex) yield
          (r, PFAMap.fromMap(
            (for ((c, j) <- cols.zipWithIndex) yield
              (c, java.lang.Double.valueOf(x.get(i, j)))).toMap))).toMap)
  }

  ////////////////////////////////////////////////////////////////////

  ////   map (MapApply)
  class MapApply(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "map"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double)), "fcn" -> P.Fcn(List(P.Double), P.Double)), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "fcn" -> P.Fcn(List(P.Double), P.Double)), P.Map(P.Map(P.Double)))))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element from <p>x</p>.</desc>
        <detail>This can be used to perform scalar multiplication on a matrix: supply a function that multiplies each element by a constant.</detail>
        <detail>The order in which elements are computed is not specified, and may be in parallel.</detail>
      </doc>
    def errcodeBase = 24000
    def apply(x: PFAArray[PFAArray[Double]], fcn: (Double) => Double): PFAArray[PFAArray[Double]] = {
      val xv = x.toVector map {_.toVector}

      val sizes = xv map {_.size}

      PFAArray.fromVector(
        for ((size, i) <- sizes.zipWithIndex) yield
          PFAArray.fromVector(
            (for (j <- 0 until size) yield
              fcn(xv(i)(j))).toVector))
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]], fcn: (Double) => Double): PFAMap[PFAMap[java.lang.Double]] = {
      val xv = x.toMap map {case (k, v) => (k, v.toMap)}

      val indexes = xv.iterator flatMap {case (i, v) => v.iterator map {case (j, _) => (i, j)}} toList

      PFAMap.fromMap(xv map { case (i, v) => (i, PFAMap.fromMap(v map {case (j, _) => (j, java.lang.Double.valueOf(fcn(xv(i)(j))))})) })
    }
  }
  provide(new MapApply)

  ////   scale (Scale) could be seen as a special case of map (MapApply), but it's too common to not have an implementation
  class Scale(val pos: Option[String] = None) extends LibFcn {
    def name = prefix + "scale"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Double), "alpha" -> P.Double), P.Array(P.Double)),
                        Sig(List("x" -> P.Array(P.Array(P.Double)), "alpha" -> P.Double), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Double), "alpha" -> P.Double), P.Map(P.Double)),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "alpha" -> P.Double), P.Map(P.Map(P.Double)))))
    def doc =
      <doc>
        <desc>Scale vector or matrix <p>x</p> by factor <p>alpha</p>.</desc>
        <detail>The order in which elements are computed is not specified, and may be in parallel.</detail>
    </doc>
    def errcodeBase = 24010
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      if (retType.accepts(AvroArray(AvroDouble()))  ||  retType.accepts(AvroMap(AvroDouble())))
        JavaCode("%s.applyVec(%s, %s)",
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true),
          wrapArg(1, args, paramTypes, true))
      else
        JavaCode("%s.applyMat(%s, %s)",
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true),
          wrapArg(1, args, paramTypes, true))
    def applyVec(x: PFAArray[Double], alpha: Double): PFAArray[Double] =
      PFAArray.fromVector(x.toVector.map(_ * alpha))
    def applyMat(x: PFAArray[PFAArray[Double]], alpha: Double): PFAArray[PFAArray[Double]] =
      PFAArray.fromVector(x.toVector.map(y => PFAArray.fromVector(y.toVector.map(_ * alpha))))
    def applyVec(x: PFAMap[java.lang.Double], alpha: Double): PFAMap[java.lang.Double] =
      PFAMap.fromMap(x.toMap map {case (k, v) => (k, java.lang.Double.valueOf(v.doubleValue * alpha))})
    def applyMat(x: PFAMap[PFAMap[java.lang.Double]], alpha: Double): PFAMap[PFAMap[java.lang.Double]] =
      PFAMap.fromMap(x.toMap map {case (k1, v1) => (k1, PFAMap.fromMap(v1.toMap map {case (k2, v2) => (k2, java.lang.Double.valueOf(v2.doubleValue * alpha))}))})
  }
  provide(new Scale)

  ////   zipmap (ZipMap)
  class ZipMap(val pos: Option[String] = None) extends LibFcn with EJMLInterface {
    def name = prefix + "zipmap"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double)), "y" -> P.Array(P.Array(P.Double)), "fcn" -> P.Fcn(List(P.Double, P.Double), P.Double)), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "y" -> P.Map(P.Map(P.Double)), "fcn" -> P.Fcn(List(P.Double, P.Double), P.Double)), P.Map(P.Map(P.Double)))))
    def doc =
      <doc>
        <desc>Apply <p>fcn</p> to each pair of elements from <p>x</p> and <p>y</p>.</desc>
        <detail>This can be used to perform matrix addition: supply a function that adds each pair of elements.</detail>
        <detail>Like most functions that deal with matrices, this function has an array signature and a map signature.  In the array signature, the number of rows and columns in <p>x</p> must be equal to the number of rows and columns of <p>y</p>, respectively (dense matrix).  In the map signature, missing row-column combinations are assumed to be zero (sparse matrix).</detail>
        <detail>The order in which elements are computed is not specified, and may be in parallel.</detail>
        <error code={s"${errcodeBase + 0}"}>In the array signature, if any element in <p>x</p> does not have a corresponding element in <p>y</p> (or vice-versa), this function raises a "misaligned matrices" error.</error>
      </doc>
    def errcodeBase = 24020
    def apply(x: PFAArray[PFAArray[Double]], y: PFAArray[PFAArray[Double]], fcn: (Double, Double) => Double): PFAArray[PFAArray[Double]] = {
      val xv = x.toVector map {_.toVector}
      val yv = y.toVector map {_.toVector}

      val sizes = xv map {_.size}
      if (sizes != (yv map {_.size}))
        throw new PFARuntimeException("misaligned matrices", errcodeBase + 0, name, pos)

      PFAArray.fromVector(
        for ((size, i) <- sizes.zipWithIndex) yield
          PFAArray.fromVector(
            (for (j <- 0 until size) yield
              fcn(xv(i)(j), yv(i)(j))).toVector))
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]], y: PFAMap[PFAMap[java.lang.Double]], fcn: (Double, Double) => Double): PFAMap[PFAMap[java.lang.Double]] = {
      val xv = x.toMap map {case (k, v) => (k, v.toMap)}
      val yv = y.toMap map {case (k, v) => (k, v.toMap)}

      val rows = rowKeys(x) union rowKeys(y)
      val cols = colKeys(x) union colKeys(y)

      PFAMap.fromMap(
        (for (r <- rows) yield
          (r, PFAMap.fromMap(
            (for (c <- cols) yield {
              val xi = x.toMap.get(r) match {
                case None => 0.0
                case Some(xx) => xx.toMap.get(c) match {
                  case None => 0.0
                  case Some(xxx) => xxx.doubleValue
                }
              }
              val yi = y.toMap.get(r) match {
                case None => 0.0
                case Some(yy) => yy.toMap.get(c) match {
                  case None => 0.0
                  case Some(yyy) => yyy.doubleValue
                }
              }
              (c, java.lang.Double.valueOf(fcn(xi, yi)))
            }).toMap))).toMap)
    }
  }
  provide(new ZipMap)

  ////   add (Add) could be seen as a special case of zipmap (ZipMap), but it's too common to not have an implementation
  class Add(val pos: Option[String] = None) extends LibFcn with EJMLInterface {
    def name = prefix + "add"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Double), "y" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Array(P.Array(P.Double)), "y" -> P.Array(P.Array(P.Double))), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Double), "y" -> P.Map(P.Double)), P.Map(P.Double)),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "y" -> P.Map(P.Map(P.Double))), P.Map(P.Map(P.Double)))))
    def doc =
      <doc>
        <desc>Add two vectors or matrices <p>x</p> and <p>y</p>.</desc>
        <detail>The order in which elements are computed is not specified, and may be in parallel.</detail>
        <detail>Like most functions that deal with matrices, this function has an array signature and a map signature.  In the array signature, the number of rows and/or columns in <p>x</p> must be equal to the number of rows and/or columns of <p>y</p>, respectively (dense matrix).  In the map signature, missing row-column combinations are assumed to be zero (sparse matrix).</detail>
        <error code={s"${errcodeBase + 0}"}>In the array signature, if any element in <p>x</p> does not have a corresponding element in <p>y</p> (or vice-versa), this function raises a "misaligned matrices" error.</error>
    </doc>
    def errcodeBase = 24030
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      if (retType.accepts(AvroArray(AvroDouble()))  ||  retType.accepts(AvroMap(AvroDouble())))
        JavaCode("%s.applyVec(%s, %s)",
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true),
          wrapArg(1, args, paramTypes, true))
      else
        JavaCode("%s.applyMat(%s, %s)",
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true),
          wrapArg(1, args, paramTypes, true))

    def applyVec(x: PFAArray[Double], y: PFAArray[Double]): PFAArray[Double] = {
      val xv = x.toVector
      val yv = y.toVector
      if (xv.size != yv.size)
        throw new PFARuntimeException("misaligned matrices", errcodeBase + 0, name, pos)
      PFAArray.fromVector((xv zip yv) map {case (xi, yi) => xi + yi})
    }

    def applyMat(x: PFAArray[PFAArray[Double]], y: PFAArray[PFAArray[Double]]): PFAArray[PFAArray[Double]] = {
      val xv = x.toVector map {_.toVector}
      val yv = y.toVector map {_.toVector}

      val sizes = xv map {_.size}
      if (sizes != (yv map {_.size}))
        throw new PFARuntimeException("misaligned matrices", errcodeBase + 0, name, pos)

      PFAArray.fromVector(
        for ((size, i) <- sizes.zipWithIndex) yield
          PFAArray.fromVector(
            (for (j <- 0 until size) yield
              xv(i)(j) + yv(i)(j)).toVector))
    }

    def applyVec(x: PFAMap[java.lang.Double], y: PFAMap[java.lang.Double]): PFAMap[java.lang.Double] = {
      val xv = x.toMap
      val yv = y.toMap
      val rows = xv.keySet union yv.keySet

      PFAMap.fromMap((for (row <- rows) yield
        (row, java.lang.Double.valueOf((xv.get(row) match {
          case None => 0.0
          case Some(xx) => xx.doubleValue
        }) + (yv.get(row) match {
          case None => 0.0
          case Some(xx) => xx.doubleValue
        })))).toMap)
    }

    def applyMat(x: PFAMap[PFAMap[java.lang.Double]], y: PFAMap[PFAMap[java.lang.Double]]): PFAMap[PFAMap[java.lang.Double]] = {
      val xv = x.toMap map {case (k, v) => (k, v.toMap)}
      val yv = y.toMap map {case (k, v) => (k, v.toMap)}

      val rows = rowKeys(x) union rowKeys(y)
      val cols = colKeys(x) union colKeys(y)

      PFAMap.fromMap(
        (for (r <- rows) yield
          (r, PFAMap.fromMap(
            (for (c <- cols) yield {
              val xi = x.toMap.get(r) match {
                case None => 0.0
                case Some(xx) => xx.toMap.get(c) match {
                  case None => 0.0
                  case Some(xxx) => xxx.doubleValue
                }
              }
              val yi = y.toMap.get(r) match {
                case None => 0.0
                case Some(yy) => yy.toMap.get(c) match {
                  case None => 0.0
                  case Some(yyy) => yyy.doubleValue
                }
              }
              (c, java.lang.Double.valueOf(xi + yi))
            }).toMap))).toMap)
    }
  }
  provide(new Add)

  ////   sub (Sub) could be seen as a special case of zipmap (ZipMap), but it's too common to not have an implementation
  class Sub(val pos: Option[String] = None) extends LibFcn with EJMLInterface {
    def name = prefix + "sub"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Double), "y" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Array(P.Array(P.Double)), "y" -> P.Array(P.Array(P.Double))), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Double), "y" -> P.Map(P.Double)), P.Map(P.Double)),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "y" -> P.Map(P.Map(P.Double))), P.Map(P.Map(P.Double)))))
    def doc =
      <doc>
        <desc>Subtract vector or matrix <p>y</p> from <p>x</p> (returns <m>x - y</m>).</desc>
        <detail>The order in which elements are computed is not specified, and may be in parallel.</detail>
        <detail>Like most functions that deal with matrices, this function has an array signature and a map signature.  In the array signature, the number of rows and/or columns in <p>x</p> must be equal to the number of rows and/or columns of <p>y</p>, respectively (dense matrix).  In the map signature, missing row-column combinations are assumed to be zero (sparse matrix).</detail>
        <error code={s"${errcodeBase + 0}"}>In the array signature, if any element in <p>x</p> does not have a corresponding element in <p>y</p> (or vice-versa), this function raises a "misaligned matrices" error.</error>
    </doc>
    def errcodeBase = 24040
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode =
      if (retType.accepts(AvroArray(AvroDouble()))  ||  retType.accepts(AvroMap(AvroDouble())))
        JavaCode("%s.applyVec(%s, %s)",
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true),
          wrapArg(1, args, paramTypes, true))
      else
        JavaCode("%s.applyMat(%s, %s)",
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true),
          wrapArg(1, args, paramTypes, true))

    def applyVec(x: PFAArray[Double], y: PFAArray[Double]): PFAArray[Double] = {
      val xv = x.toVector
      val yv = y.toVector
      if (xv.size != yv.size)
        throw new PFARuntimeException("misaligned matrices", errcodeBase + 0, name, pos)
      PFAArray.fromVector((xv zip yv) map {case (xi, yi) => xi - yi})
    }

    def applyMat(x: PFAArray[PFAArray[Double]], y: PFAArray[PFAArray[Double]]): PFAArray[PFAArray[Double]] = {
      val xv = x.toVector map {_.toVector}
      val yv = y.toVector map {_.toVector}

      val sizes = xv map {_.size}
      if (sizes != (yv map {_.size}))
        throw new PFARuntimeException("misaligned matrices", errcodeBase + 0, name, pos)

      PFAArray.fromVector(
        for ((size, i) <- sizes.zipWithIndex) yield
          PFAArray.fromVector(
            (for (j <- 0 until size) yield
              xv(i)(j) - yv(i)(j)).toVector))
    }

    def applyVec(x: PFAMap[java.lang.Double], y: PFAMap[java.lang.Double]): PFAMap[java.lang.Double] = {
      val xv = x.toMap
      val yv = y.toMap
      val rows = xv.keySet union yv.keySet

      PFAMap.fromMap((for (row <- rows) yield
        (row, java.lang.Double.valueOf((xv.get(row) match {
          case None => 0.0
          case Some(xx) => xx.doubleValue
        }) - (yv.get(row) match {
          case None => 0.0
          case Some(xx) => xx.doubleValue
        })))).toMap)
    }

    def applyMat(x: PFAMap[PFAMap[java.lang.Double]], y: PFAMap[PFAMap[java.lang.Double]]): PFAMap[PFAMap[java.lang.Double]] = {
      val xv = x.toMap map {case (k, v) => (k, v.toMap)}
      val yv = y.toMap map {case (k, v) => (k, v.toMap)}

      val rows = rowKeys(x) union rowKeys(y)
      val cols = colKeys(x) union colKeys(y)

      PFAMap.fromMap(
        (for (r <- rows) yield
          (r, PFAMap.fromMap(
            (for (c <- cols) yield {
              val xi = x.toMap.get(r) match {
                case None => 0.0
                case Some(xx) => xx.toMap.get(c) match {
                  case None => 0.0
                  case Some(xxx) => xxx.doubleValue
                }
              }
              val yi = y.toMap.get(r) match {
                case None => 0.0
                case Some(yy) => yy.toMap.get(c) match {
                  case None => 0.0
                  case Some(yyy) => yyy.doubleValue
                }
              }
              (c, java.lang.Double.valueOf(xi - yi))
            }).toMap))).toMap)
    }
  }
  provide(new Sub)

  ////   dot (Dot)
  class Dot(val pos: Option[String] = None) extends LibFcn with EJMLInterface {
    def name = prefix + "dot"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double)), "y" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "y" -> P.Map(P.Double)), P.Map(P.Double)),
                        Sig(List("x" -> P.Array(P.Array(P.Double)), "y" -> P.Array(P.Array(P.Double))), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "y" -> P.Map(P.Map(P.Double))), P.Map(P.Map(P.Double)))))
    def doc =
      <doc>
        <desc>Multiply two matrices or a matrix and a vector, which may be represented as dense arrays or potentially sparse maps.</desc>
        <detail>Like most functions that deal with matrices, this function has an array signature and a map signature.  In the array signature, the number of columns of <p>x</p> must be equal to the number of rows (or the number of elements) of <p>y</p> (dense matrix).  In the map signature, missing values are assumed to be zero (sparse matrix).</detail>
        <detail>Matrices supplied as maps may be computed using sparse methods.</detail>
        <error code={s"${errcodeBase + 0}"}>In the array signature, if the dimensions of <p>x</p> do not correspond to the dimension(s) of <p>y</p>, this function raises a "misaligned matrices" error.</error>
        <error code={s"${errcodeBase + 1}"}>If <p>x</p> or <p>y</p> has fewer than 1 row or fewer than 1 column, this function raises a "too few rows/cols" error.</error>
        <error code={s"${errcodeBase + 2}"}>If <p>x</p> or <p>y</p> contains any non-finite values, this function raises a "contains non-finite value" error.</error>
      </doc>
    def errcodeBase = 24050

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType, engineOptions: EngineOptions): JavaCode = paramTypes(1) match {
      case AvroArray(AvroDouble()) | AvroMap(AvroDouble()) =>
        JavaCode("%s.applyVector(%s, %s)",
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true),
          wrapArg(1, args, paramTypes, true))
      case AvroArray(AvroArray(AvroDouble())) | AvroMap(AvroMap(AvroDouble())) =>
        JavaCode("%s.apply(%s, %s)",
          javaRef(FcnType(argContext collect {case x: ExpressionContext => x.retType}, retType)).toString,
          wrapArg(0, args, paramTypes, true),
          wrapArg(1, args, paramTypes, true))
    }
    def applyVector(x: PFAArray[PFAArray[Double]], y: PFAArray[Double]): PFAArray[Double] = {
      val xrows = x.toVector.size
      val yrows = y.toVector.size
      if (xrows == 0  ||  yrows == 0)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 1, name, pos)
      val xcols = x.toVector.head.toVector.size
      if (x.toVector.exists(_.toVector.size != xcols))
        throw new PFARuntimeException("misaligned matrices", errcodeBase + 0, name, pos)
      if (xcols == 0)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 1, name, pos)
      if (x.toVector.exists(_.toVector.exists(z => java.lang.Double.isNaN(z)  ||  java.lang.Double.isInfinite(z)))  ||  y.toVector.exists(z => java.lang.Double.isNaN(z)  ||  java.lang.Double.isInfinite(z)))
        throw new PFARuntimeException("contains non-finite value", errcodeBase + 2, name, pos)
      val xm = toDense(x)
      val ym = toDenseVector(y)
      val result =
        try {
          xm.mult(ym)
        }
        catch {
          case _: MatrixDimensionException => throw new PFARuntimeException("misaligned matrices", errcodeBase + 0, name, pos)
        }
      toArray(result)
    }
    def applyVector(x: PFAMap[PFAMap[java.lang.Double]], y: PFAMap[java.lang.Double]): PFAMap[java.lang.Double] = {
      val rows = rowKeys(x).toSeq
      val inter = (colKeys(x) union vecKeys(y)).toSeq
      if (rows.isEmpty  ||  inter.isEmpty)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 1, name, pos)
      if (x.toMap.values.exists(_.toMap.values.exists(z => z.isNaN  ||  z.isInfinite))  ||  y.toMap.values.exists(z => z.isNaN  ||  z.isInfinite))
        throw new PFARuntimeException("contains non-finite value", errcodeBase + 2, name, pos)
      val xm = toDense(x, rows, inter)
      val ym = toDenseVector(y, inter)
      val result = xm.mult(ym)
      toMap(result, rows)
    }
    def apply(x: PFAArray[PFAArray[Double]], y: PFAArray[PFAArray[Double]]): PFAArray[PFAArray[Double]] = {
      val xrows = x.toVector.size
      val yrows = y.toVector.size
      if (xrows == 0  ||  yrows == 0)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 1, name, pos)
      val xcols = x.toVector.head.toVector.size
      val ycols = y.toVector.head.toVector.size
      if (x.toVector.exists(_.toVector.size != xcols)  ||  y.toVector.exists(_.toVector.size != ycols))
        throw new PFARuntimeException("misaligned matrices", errcodeBase + 0, name, pos)
      if (xcols == 0  ||  ycols == 0)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 1, name, pos)
      if (x.toVector.exists(_.toVector.exists(z => java.lang.Double.isNaN(z)  ||  java.lang.Double.isInfinite(z)))  ||  y.toVector.exists(_.toVector.exists(z => java.lang.Double.isNaN(z)  ||  java.lang.Double.isInfinite(z))))
        throw new PFARuntimeException("contains non-finite value", errcodeBase + 2, name, pos)
      val xm = toDense(x)
      val ym = toDense(y)
      val result =
        try {
          xm.mult(ym)
        }
        catch {
          case _: MatrixDimensionException => throw new PFARuntimeException("misaligned matrices", errcodeBase + 0, name, pos)
        }
      toArrayArray(result)
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]], y: PFAMap[PFAMap[java.lang.Double]]): PFAMap[PFAMap[java.lang.Double]] = {
      val rows = rowKeys(x).toSeq
      val inter = (colKeys(x) union rowKeys(y)).toSeq
      val cols = colKeys(y).toSeq
      if (rows.isEmpty  ||  inter.isEmpty  ||  cols.isEmpty)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 1, name, pos)
      if (x.toMap.values.exists(_.toMap.values.exists(z => z.isNaN  ||  z.isInfinite))  ||  y.toMap.values.exists(_.toMap.values.exists(z => z.isNaN  ||  z.isInfinite)))
        throw new PFARuntimeException("contains non-finite value", errcodeBase + 2, name, pos)
      val xm = toDense(x, rows, inter)
      val ym = toDense(y, inter, cols)
      val result = xm.mult(ym)
      toMapMap(result, rows, cols)
    }
  }
  provide(new Dot)

  ////   transpose (Transpose)
  class Transpose(val pos: Option[String] = None) extends LibFcn with EJMLInterface {
    def name = prefix + "transpose"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double))), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double))), P.Map(P.Map(P.Double)))))
    def doc =
      <doc>
        <desc>Transpose a rectangular matrix.</desc>
        <error code={s"${errcodeBase + 0}"}>If the matrix has fewer than 1 row or fewer than 1 column, this function raises a "too few rows/cols" error.</error>
        <error code={s"${errcodeBase + 1}"}>If the columns are ragged (arrays of different lengths or maps with different sets of keys), this function raises a "ragged columns" error.</error>
      </doc>
    def errcodeBase = 24060
    def apply(x: PFAArray[PFAArray[Double]]): PFAArray[PFAArray[Double]] = {
      val xv = x.toVector
      val rows = xv.size
      if (rows < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      val cols = xv.head.toVector.size
      if (cols < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      if (ragged(x))
        throw new PFARuntimeException("ragged columns", errcodeBase + 1, name, pos)
      PFAArray.fromVector(
        (for (j <- 0 until cols) yield
          PFAArray.fromVector(
            (for (i <- 0 until rows) yield
              xv(i).toVector.apply(j)).toVector)).toVector)
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]]): PFAMap[PFAMap[java.lang.Double]] = {
      val rows = rowKeys(x)
      val cols = colKeys(x)
      if (rows.size < 1  ||  cols.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      if (ragged(x))
        throw new PFARuntimeException("ragged columns", errcodeBase + 1, name, pos)
      PFAMap.fromMap(
        (for (c <- cols) yield
          (c, PFAMap.fromMap(
            (for (r <- rows) yield
              (r, x.toMap.apply(r).toMap.apply(c))).toMap))).toMap)
    }
  }
  provide(new Transpose)

  ////   inverse (Inverse)
  class Inverse(val pos: Option[String] = None) extends LibFcn with EJMLInterface {
    def name = prefix + "inverse"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double))), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double))), P.Map(P.Map(P.Double)))))
    def doc =
      <doc>
        <desc>Compute the inverse (or Moore-Penrose pseudoinverse, if not square) of <p>x</p>.</desc>
        <error code={s"${errcodeBase + 0}"}>If the matrix has fewer than 1 row or fewer than 1 column, this function raises a "too few rows/cols" error.</error>
        <error code={s"${errcodeBase + 1}"}>If <p>x</p> is an array with ragged columns (arrays of different lengths), this function raises a "ragged columns" error.</error>
      </doc>
    def errcodeBase = 24070
    def apply(x: PFAArray[PFAArray[Double]]): PFAArray[PFAArray[Double]] = {
      if (x.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      if (ragged(x))
        throw new PFARuntimeException("ragged columns", errcodeBase + 1, name, pos)
      if (x.toVector.head.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      val xm = toDense(x)
      val inv = xm.pseudoInverse
      toArrayArray(inv)
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]]): PFAMap[PFAMap[java.lang.Double]] = {
      val rows = rowKeys(x).toSeq
      val cols = colKeys(x).toSeq
      if (rows.size < 1  |  cols.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      val xm = toDense(x, rows, cols)
      val inv = xm.pseudoInverse
      toMapMap(inv, cols, rows)
    }
  }
  provide(new Inverse)

  ////   trace (Trace)
  class Trace(val pos: Option[String] = None) extends LibFcn with EJMLInterface {
    def name = prefix + "trace"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double))), P.Double),
                        Sig(List("x" -> P.Map(P.Map(P.Double))), P.Double)))
    def doc =
      <doc>
        <desc>Compute the trace of a matrix (sum of diagonal elements).</desc>
        <error code={s"${errcodeBase + 0}"}>If <p>x</p> is an array with ragged columns (arrays of different lengths), this function raises a "ragged columns" error.</error>
      </doc>
    def errcodeBase = 24080
    def apply(x: PFAArray[PFAArray[Double]]): Double = {
      val rows = x.toVector.size
      if (rows == 0)
        0.0
      else {
        if (ragged(x))
          throw new PFARuntimeException("ragged columns", errcodeBase + 0, name, pos)
        val cols = x.toVector.head.toVector.size
        (for (i <- 0 until Array(rows, cols).min) yield
          x.toVector.apply(i).toVector.apply(i)).sum
      }
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]]): Double = {
      val keys = rowKeys(x) intersect colKeys(x)
      (for (k <- keys) yield
        x.toMap.apply(k).toMap.apply(k).doubleValue).sum
    }
  }
  provide(new Trace)

  ////   det (Det)
  class Det(val pos: Option[String] = None) extends LibFcn with EJMLInterface {
    def name = prefix + "det"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double))), P.Double),
                        Sig(List("x" -> P.Map(P.Map(P.Double))), P.Double)))
    def doc =
      <doc>
        <desc>Compute the determinant of a matrix.</desc>
        <error code={s"${errcodeBase + 0}"}>If the matrix has fewer than 1 row or fewer than 1 column, this function raises a "too few rows/cols" error.</error>
        <error code={s"${errcodeBase + 1}"}>If <p>x</p> is an array with ragged columns (arrays of different lengths), this function raises a "ragged columns" error.</error>
        <error code={s"${errcodeBase + 2}"}>In the array signature, if <p>x</p> is not a square matrix, this function raises a "non-square matrix" error.</error>
      </doc>
    def errcodeBase = 24090
    def apply(x: PFAArray[PFAArray[Double]]): Double = {
      if (x.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      if (ragged(x))
        throw new PFARuntimeException("ragged columns", errcodeBase + 1, name, pos)
      if (x.toVector.head.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      if (x.toVector.size != x.toVector.head.toVector.size)
        throw new PFARuntimeException("non-square matrix", errcodeBase + 2, name, pos)
      val xm = toDense(x)
      xm.determinant
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]]): Double = {
      val keys = (rowKeys(x) union colKeys(x)).toSeq
      if (keys.size < 1  ||  x.toMap.values.forall(_.toMap.isEmpty))
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      val xm = toDense(x, keys, keys)
      xm.determinant
    }
  }
  provide(new Det)

  ////   symmetric (Symmetric)
  class Symmetric(val pos: Option[String] = None) extends LibFcn with EJMLInterface {
    def name = prefix + "symmetric"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double)), "tolerance" -> P.Double), P.Boolean),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "tolerance" -> P.Double), P.Boolean)))
    def doc =
      <doc>
        <desc>Determine if a matrix is symmetric withing tolerance.</desc>
        <ret>Returns <c>true</c> if the absolute value of element <m>i</m>, <m>j</m> minus element <m>j</m>, <m>i</m> is less than <p>tolerance</p>.</ret>
        <error code={s"${errcodeBase + 0}"}>If the matrix has fewer than 1 row or fewer than 1 column, this function raises a "too few rows/cols" error.</error>
        <error code={s"${errcodeBase + 1}"}>If <p>x</p> is an array with ragged columns (arrays of different lengths), this function raises a "ragged columns" error.</error>
        <error code={s"${errcodeBase + 2}"}>If <p>x</p> is not a square matrix, this function raises a "non-square matrix" error.</error>
      </doc>
    def errcodeBase = 24100
    def same(x: Double, y: Double, tolerance: Double): Boolean =
      if (java.lang.Double.isInfinite(x)  &&  java.lang.Double.isInfinite(y)  &&  ((x > 0.0  &&  y > 0.0)  ||  (x < 0.0  &&  y < 0.0)))
        true
      else if (java.lang.Double.isNaN(x)  &&  java.lang.Double.isNaN(y))
        true
      else if (!java.lang.Double.isInfinite(x)  &&  !java.lang.Double.isNaN(x)  &&  !java.lang.Double.isInfinite(y)  &&  !java.lang.Double.isNaN(y))
        Math.abs(x - y) < tolerance
      else
        false
    def apply(x: PFAArray[PFAArray[Double]], tolerance: Double): Boolean = {
      if (x.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      if (ragged(x))
        throw new PFARuntimeException("ragged columns", errcodeBase + 1, name, pos)
      if (x.toVector.head.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      if (x.toVector.size != x.toVector.head.toVector.size)
        throw new PFARuntimeException("non-square matrix", errcodeBase + 2, name, pos)
      val size = x.toVector.size
      (0 until size) forall {i =>
        ((i+1) until size) forall {j =>
          same(x.toVector.apply(i).toVector.apply(j), x.toVector.apply(j).toVector.apply(i), tolerance)}}
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]], tolerance: Double): Boolean = {
      val keys = (rowKeys(x) union colKeys(x)).toSeq
      if (keys.size < 1  ||  x.toMap.values.forall(_.toMap.isEmpty))
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      val size = keys.size
      (0 until size) forall {i =>
        ((i+1) until size) forall {j =>
          val ij = x.toMap.get(keys(i)) match {
            case None => 0.0
            case Some(xx) => xx.toMap.get(keys(j)) match {
              case None => 0.0
              case Some(xxx) => xxx.doubleValue
            }
          }
          val ji = x.toMap.get(keys(j)) match {
            case None => 0.0
            case Some(xx) => xx.toMap.get(keys(i)) match {
              case None => 0.0
              case Some(xxx) => xxx.doubleValue
            }
          }
          same(ij, ji, tolerance)}}
    }
  }
  provide(new Symmetric)

  ////   eigenBasis (EigenBasis)
  class EigenBasis(val pos: Option[String] = None) extends LibFcn with EJMLInterface {
    def name = prefix + "eigenBasis"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double))), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double))), P.Map(P.Map(P.Double)))))
    def doc =
      <doc>
        <desc>Compute the eigenvalues and eigenvectors of a real, symmetric matrix <p>x</p> (which are all real).</desc>
        <ret>A matrix in which each row (first level of array or map hierarchy) is a normalized eigenvector of <p>x</p> divided by the square root of the corresponding eigenvalue (The sign is chosen such that the first component is positive.).  If provided as an array, the rows are in decreasing order of eigenvalue (increasing order of inverse square root eigenvalue).  If provided as a map, the rows are keyed by string representations of integers starting with <c>"0"</c>, and increasing row keys are in decreasing order of eigenvalue.</ret>
        <detail>If <p>x</p> is the covariance matrix of a zero-mean dataset, the matrix that this function returns would transform the dataset to one with unit variances and zero covariances.</detail>
        <detail>If <p>x</p> is not symmetric or not exactly symmetric, it will first be symmetrized (<m>{"(x + x^T)/2"}</m>).  For example, a matrix represented by only the upper triangle (other elements are zero or missing from the map) becomes a symmetric matrix with the upper triangle unchanged.</detail>
        <nondeterministic type="unstable" />
        <error code={s"${errcodeBase + 0}"}>If the matrix has fewer than 1 row or fewer than 1 column, this function raises a "too few rows/cols" error.</error>
        <error code={s"${errcodeBase + 1}"}>If <p>x</p> is an array with ragged columns (arrays of different lengths), this function raises a "ragged columns" error.</error>
        <error code={s"${errcodeBase + 2}"}>If <p>x</p> is not a square matrix, this function raises a "non-square matrix" error.</error>
        <error code={s"${errcodeBase + 3}"}>If <p>x</p> contains non-finite values, this function raises a "non-finite matrix" error.</error>
      </doc>
    def errcodeBase = 24110
    def argsort(x: Seq[Double]) = x.zipWithIndex sortWith {_._1 < _._1} map {_._2}

    def calculate(x: SimpleMatrix): SimpleMatrix = {
      val symm = x.plus(x.transpose).scale(0.5).getMatrix
      val size = x.numRows

      val evd = DecompositionFactory.eig(size, true, true)
      if (!evd.decompose(symm))
        throw new RuntimeException("report this error 1")
      if (evd.getNumberOfEigenvalues != size)
        throw new RuntimeException("report this error 2")

      val eigvalm2 = (0 until size) map {i => 1.0 / Math.sqrt(evd.getEigenvalue(i).getMagnitude)}
      val eigvec = (0 until size) map {i => SimpleMatrix.wrap(evd.getEigenVector(i))} map {y => if (y.get(0, 0) < 0.0) y.scale(-1.0) else y}
      val order = argsort(eigvalm2)

      val out = Array.fill(size)(Array.fill(size)(0.0))
      for (i <- 0 until size)
        for (j <- 0 until size)
          out(i)(j) = eigvec(order(i)).get(j, 0) * eigvalm2(order(i))
      new SimpleMatrix(out)
    }

    def apply(x: PFAArray[PFAArray[Double]]): PFAArray[PFAArray[Double]] = {
      if (x.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      if (ragged(x))
        throw new PFARuntimeException("ragged columns", errcodeBase + 1, name, pos)
      if (x.toVector.head.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      if (x.toVector.size != x.toVector.head.toVector.size)
        throw new PFARuntimeException("non-square matrix", errcodeBase + 2, name, pos)
      if (x.toVector.exists(a => a.toVector.exists(b => java.lang.Double.isInfinite(b)  ||  java.lang.Double.isNaN(b))))
        throw new PFARuntimeException("non-finite matrix", errcodeBase + 3, name, pos)
      val out = calculate(toDense(x))
      toArrayArray(out)
    }

    def apply(x: PFAMap[PFAMap[java.lang.Double]]): PFAMap[PFAMap[java.lang.Double]] = {
      val keys = (rowKeys(x) union colKeys(x)).toSeq
      if (keys.size < 1  ||  x.toMap.values.forall(_.toMap.isEmpty))
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      if (x.toMap.values.exists(a => a.toMap.values.exists(b => java.lang.Double.isInfinite(b)  ||  java.lang.Double.isNaN(b))))
        throw new PFARuntimeException("non-finite matrix", errcodeBase + 3, name, pos)
      val out = calculate(toDense(x, keys, keys))
      toMapMap(out, (0 until keys.size) map {_.toString}, keys)
    }
  }
  provide(new EigenBasis)

  ////   truncate (Truncate)
  class Truncate(val pos: Option[String] = None) extends LibFcn with EJMLInterface {
    def name = prefix + "truncate"
    def sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double)), "keep" -> P.Int), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "keep" -> P.Array(P.String)), P.Map(P.Map(P.Double)))))
    def doc =
      <doc>
        <desc>Remove rows from a matrix so that it becomes a projection operator.</desc>
        <param name="x">The matrix to truncate.</param>
        <param name="keep">If <p>x</p> is an array, this is the number of rows to keep, starting with the first row.  If <p>x</p> is a map, this is the set of keys to keep.  If <p>keep</p> is larger than the number of rows or is not a subset of the keys, the excess is ignored.</param>
        <detail>In Principle Component Analysis (PCA), this would be applied to the eigenbasis transformation (<f>la.eigenBasis</f>) to keep only a specified number (or set) of transformed components.</detail>
        <error code={s"${errcodeBase + 0}"}>If the matrix has fewer than 1 row or fewer than 1 column, this function raises a "too few rows/cols" error.</error>
        <error code={s"${errcodeBase + 1}"}>If <p>x</p> is an array with ragged columns (arrays of different lengths), this function raises a "ragged columns" error.</error>
      </doc>
    def errcodeBase = 24120
    def apply(x: PFAArray[PFAArray[Double]], keep: Int): PFAArray[PFAArray[Double]] = {
      if (x.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      if (ragged(x))
        throw new PFARuntimeException("ragged columns", errcodeBase + 1, name, pos)
      if (x.toVector.head.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      PFAArray.fromVector(x.toVector.take(keep))
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]], keep: PFAArray[String]): PFAMap[PFAMap[java.lang.Double]] = {
      val rows = rowKeys(x)
      val cols = colKeys(x)
      if (rows.size < 1  |  cols.size < 1)
        throw new PFARuntimeException("too few rows/cols", errcodeBase + 0, name, pos)
      val tofilter = rows intersect keep.toVector.toSet
      PFAMap.fromMap(x.toMap filterKeys {tofilter.contains(_)})
    }
  }
  provide(new Truncate)

}
