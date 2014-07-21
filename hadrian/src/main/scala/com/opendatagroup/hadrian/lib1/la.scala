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

import scala.language.postfixOps

import org.ejml.simple.SimpleMatrix
import org.ejml.factory.DecompositionFactory
import org.ejml.alg.dense.mult.MatrixDimensionException

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
  private var fcns = Map[String, LibFcn]()
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
      new SimpleMatrix(x.toVector map {Array(_)} toArray)

    def toDense(x: PFAArray[PFAArray[Double]]): SimpleMatrix =
      new SimpleMatrix(x.toVector map {_.toVector.toArray} toArray)

    def toDenseVector(x: PFAMap[java.lang.Double], rows: Seq[String]): SimpleMatrix =
      new SimpleMatrix(
        (for (r <- rows) yield x.toMap.get(r) match {
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
  object MapApply extends LibFcn {
    val name = prefix + "map"
    val sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double)), "fcn" -> P.Fcn(List(P.Double), P.Double)), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "fcn" -> P.Fcn(List(P.Double), P.Double)), P.Map(P.Map(P.Double)))))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element from <p>x</p>.</desc>
        <detail>This can be used to perform scalar multiplication on a matrix: supply a function that multiplies each element by a constant.</detail>
        <detail>The order in which elements are computed is not specified, and may be in parallel.</detail>
      </doc>
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
  provide(MapApply)

  ////   zipmap (ZipMap)
  object ZipMap extends LibFcn with EJMLInterface {
    val name = prefix + "zipmap"
    val sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double)), "y" -> P.Array(P.Array(P.Double)), "fcn" -> P.Fcn(List(P.Double, P.Double), P.Double)), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "y" -> P.Map(P.Map(P.Double)), "fcn" -> P.Fcn(List(P.Double, P.Double), P.Double)), P.Map(P.Map(P.Double)))))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each pair of elements from <p>x</p> and <p>y</p>.</desc>
        <detail>This can be used to perform matrix addition: supply a function that adds each pair of elements.</detail>
        <detail>Like most functions that deal with matrices, this function has an array signature and a map signature.  In the array signature, the number of rows and columns in <p>x</p> must be equal to the number of rows and columns of <p>y</p>, respectively.  In the map signature, missing row-column combinations are assumed to be zero.</detail>
        <detail>The order in which elements are computed is not specified, and may be in parallel.</detail>
        <error>In the array signature, if any element in <p>x</p> does not have a corresponding element in <p>y</p> (or vice-versa), this function raises a "misaligned matrices" error.</error>
      </doc>
    def apply(x: PFAArray[PFAArray[Double]], y: PFAArray[PFAArray[Double]], fcn: (Double, Double) => Double): PFAArray[PFAArray[Double]] = {
      val xv = x.toVector map {_.toVector}
      val yv = y.toVector map {_.toVector}

      val sizes = xv map {_.size}
      if (sizes != (yv map {_.size}))
        throw new PFARuntimeException("misaligned matrices")

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
  provide(ZipMap)

  ////   dot (Dot)
  object Dot extends LibFcn with EJMLInterface {
    val name = prefix + "dot"
    val sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double)), "y" -> P.Array(P.Double)), P.Array(P.Double)),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "y" -> P.Map(P.Double)), P.Map(P.Double)),
                        Sig(List("x" -> P.Array(P.Array(P.Double)), "y" -> P.Array(P.Array(P.Double))), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "y" -> P.Map(P.Map(P.Double))), P.Map(P.Map(P.Double)))))
    val doc =
      <doc>
        <desc>Multiply two matrices or a matrix and a vector, which may be represented as dense arrays or potentially sparse maps.</desc>
        <detail>Like most functions that deal with matrices, this function has an array signature and a map signature.  In the array signature, the number of columns of <p>x</p> must be equal to the number of rows (or the number of elements) of <p>y</p>.  In the map signature, missing values are assumed to be zero.</detail>
        <detail>Matrices supplied as maps may be computed using sparse methods.</detail>
      </doc>

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode = paramTypes(1) match {
      case AvroArray(AvroDouble()) | AvroMap(AvroDouble()) =>
        JavaCode("%s.MODULE$.applyVector(%s, %s)",
          this.getClass.getName,
          wrapArg(0, args, paramTypes, true),
          wrapArg(1, args, paramTypes, true))
      case AvroArray(AvroArray(AvroDouble())) | AvroMap(AvroMap(AvroDouble())) =>
        JavaCode("%s.MODULE$.apply(%s, %s)",
          this.getClass.getName,
          wrapArg(0, args, paramTypes, true),
          wrapArg(1, args, paramTypes, true))
    }
    def applyVector(x: PFAArray[PFAArray[Double]], y: PFAArray[Double]): PFAArray[Double] = {
      val xm = toDense(x)
      val ym = toDenseVector(y)
      val result =
        try {
          xm.mult(ym)
        }
        catch {
          case _: MatrixDimensionException => throw new PFARuntimeException("misaligned matrices")
        }
      toArray(result)
    }
    def applyVector(x: PFAMap[PFAMap[java.lang.Double]], y: PFAMap[java.lang.Double]): PFAMap[java.lang.Double] = {
      val rows = rowKeys(x).toSeq
      val inter = (colKeys(x) union vecKeys(y)).toSeq
      val xm = toDense(x, rows, inter)
      val ym = toDenseVector(y, inter)
      val result = xm.mult(ym)
      toMap(result, rows)
    }
    def apply(x: PFAArray[PFAArray[Double]], y: PFAArray[PFAArray[Double]]): PFAArray[PFAArray[Double]] = {
      val xm = toDense(x)
      val ym = toDense(y)
      val result =
        try {
          xm.mult(ym)
        }
        catch {
          case _: MatrixDimensionException => throw new PFARuntimeException("misaligned matrices")
        }
      toArrayArray(result)
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]], y: PFAMap[PFAMap[java.lang.Double]]): PFAMap[PFAMap[java.lang.Double]] = {
      val rows = rowKeys(x).toSeq
      val inter = (colKeys(x) union rowKeys(y)).toSeq
      val cols = colKeys(y).toSeq
      val xm = toDense(x, rows, inter)
      val ym = toDense(y, inter, cols)
      val result = xm.mult(ym)
      toMapMap(result, rows, cols)
    }
  }
  provide(Dot)

  ////   transpose (Transpose)
  object Transpose extends LibFcn with EJMLInterface {
    val name = prefix + "transpose"
    val sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double))), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double))), P.Map(P.Map(P.Double)))))
    val doc =
      <doc>
        <desc>Transpose a rectangular matrix.</desc>
        <error>If the matrix has fewer than 1 row or fewer than 1 column, this function raises a "too few rows/cols" error.</error>
        <error>If the columns are ragged (arrays of different lengths or maps with different sets of keys), this function raises a "ragged columns" error.</error>
      </doc>
    def apply(x: PFAArray[PFAArray[Double]]): PFAArray[PFAArray[Double]] = {
      val xv = x.toVector
      val rows = xv.size
      if (rows < 1)
        throw new PFARuntimeException("too few rows/cols")
      val cols = xv.head.toVector.size
      if (cols < 1)
        throw new PFARuntimeException("too few rows/cols")
      if (ragged(x))
        throw new PFARuntimeException("ragged columns")
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
        throw new PFARuntimeException("too few rows/cols")
      if (ragged(x))
        throw new PFARuntimeException("ragged columns")
      PFAMap.fromMap(
        (for (c <- cols) yield
          (c, PFAMap.fromMap(
            (for (r <- rows) yield
              (r, x.toMap.apply(r).toMap.apply(c))).toMap))).toMap)
    }
  }
  provide(Transpose)

  ////   inverse (Inverse)
  object Inverse extends LibFcn with EJMLInterface {
    val name = prefix + "inverse"
    val sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double))), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double))), P.Map(P.Map(P.Double)))))
    val doc =
      <doc>
        <desc>Compute the inverse (or Moore-Penrose pseudoinverse, if not square) of <p>x</p>.</desc>
        <error>If the matrix has fewer than 1 row or fewer than 1 column, this function raises a "too few rows/cols" error.</error>
        <error>If <p>x</p> is an array with ragged columns (arrays of different lengths), this function raises a "ragged columns" error.</error>
      </doc>
    def apply(x: PFAArray[PFAArray[Double]]): PFAArray[PFAArray[Double]] = {
      if (x.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      if (ragged(x))
        throw new PFARuntimeException("ragged columns")
      if (x.toVector.head.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      val xm = toDense(x)
      val inv = xm.pseudoInverse
      toArrayArray(inv)
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]]): PFAMap[PFAMap[java.lang.Double]] = {
      val rows = rowKeys(x).toSeq
      val cols = colKeys(x).toSeq
      if (rows.size < 1  |  cols.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      val xm = toDense(x, rows, cols)
      val inv = xm.pseudoInverse
      toMapMap(inv, cols, rows)
    }
  }
  provide(Inverse)

  ////   trace (Trace)
  object Trace extends LibFcn with EJMLInterface {
    val name = prefix + "trace"
    val sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double))), P.Double),
                        Sig(List("x" -> P.Map(P.Map(P.Double))), P.Double)))
    val doc =
      <doc>
        <desc>Compute the trace of a matrix (sum of diagonal elements).</desc>
        <error>If <p>x</p> is an array with ragged columns (arrays of different lengths), this function raises a "ragged columns" error.</error>
      </doc>
    def apply(x: PFAArray[PFAArray[Double]]): Double = {
      if (ragged(x))
        throw new PFARuntimeException("ragged columns")
      (for (i <- 0 until x.toVector.size) yield
        x.toVector.apply(i).toVector.apply(i)).sum
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]]): Double = {
      val keys = rowKeys(x) intersect colKeys(x)
      (for (k <- keys) yield
        x.toMap.apply(k).toMap.apply(k).doubleValue).sum
    }
  }
  provide(Trace)

  ////   det (Det)
  object Det extends LibFcn with EJMLInterface {
    val name = prefix + "det"
    val sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double))), P.Double),
                        Sig(List("x" -> P.Map(P.Map(P.Double))), P.Double)))
    val doc =
      <doc>
        <desc>Compute the determinant of a matrix.</desc>
        <error>If the matrix has fewer than 1 row or fewer than 1 column, this function raises a "too few rows/cols" error.</error>
        <error>If <p>x</p> is an array with ragged columns (arrays of different lengths), this function raises a "ragged columns" error.</error>
        <error>If <p>x</p> is not a square matrix, this function raises a "non-square matrix" error.</error>
      </doc>
    def apply(x: PFAArray[PFAArray[Double]]): Double = {
      if (x.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      if (ragged(x))
        throw new PFARuntimeException("ragged columns")
      if (x.toVector.head.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      if (x.toVector.size != x.toVector.head.toVector.size)
        throw new PFARuntimeException("non-square matrix")
      val xm = toDense(x)
      xm.determinant
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]]): Double = {
      val keys = (rowKeys(x) union colKeys(x)).toSeq
      if (keys.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      val xm = toDense(x, keys, keys)
      xm.determinant
    }
  }
  provide(Det)

  ////   symmetric (Symmetric)
  object Symmetric extends LibFcn with EJMLInterface {
    val name = prefix + "symmetric"
    val sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double)), "tolerance" -> P.Double), P.Boolean),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "tolerance" -> P.Double), P.Boolean)))
    val doc =
      <doc>
        <desc>Determine if a matrix is symmetric withing tolerance.</desc>
        <ret>Returns <c>true</c> if the absolute value of element <m>i</m>, <m>j</m> minus element <m>j</m>, <m>i</m> is less than <p>tolerance</p>.</ret>
        <error>If the matrix has fewer than 1 row or fewer than 1 column, this function raises a "too few rows/cols" error.</error>
        <error>If <p>x</p> is an array with ragged columns (arrays of different lengths), this function raises a "ragged columns" error.</error>
        <error>If <p>x</p> is not a square matrix, this function raises a "non-square matrix" error.</error>
      </doc>
    def apply(x: PFAArray[PFAArray[Double]], tolerance: Double): Boolean = {
      if (x.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      if (ragged(x))
        throw new PFARuntimeException("ragged columns")
      if (x.toVector.head.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      if (x.toVector.size != x.toVector.head.toVector.size)
        throw new PFARuntimeException("non-square matrix")
      val size = x.toVector.size
      (0 until size) forall {i =>
        ((i+1) until size) forall {j =>
          Math.abs(x.toVector.apply(i).toVector.apply(j) - x.toVector.apply(j).toVector.apply(i)) < tolerance}}
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]], tolerance: Double): Boolean = {
      val keys = (rowKeys(x) union colKeys(x)).toSeq
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
          Math.abs(ij - ji) < tolerance}}
    }
  }
  provide(Symmetric)

  ////   eigenBasis (EigenBasis)
  object EigenBasis extends LibFcn with EJMLInterface {
    val name = prefix + "eigenBasis"
    val sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double))), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double))), P.Map(P.Map(P.Double)))))
    val doc =
      <doc>
        <desc>Compute the eigenvalues and eigenvectors of a real, symmetric matrix <p>x</p> (which are all real).</desc>
        <ret>A matrix in which each row (first level of array or map hierarchy) is a normalized eigenvector of <p>x</p> divided by the square root of the corresponding eigenvalue.  If provided as an array, the rows are in decreasing order of eigenvalue (increasing order of inverse square root eigenvalue).  If provided as a map, the rows are keyed by string representations of integers starting with <c>"0"</c>, and increasing row keys are in decreasing order of eigenvalue.</ret>
        <detail>If <p>x</p> is the covariance matrix of a zero-mean dataset, the matrix that this function returns would transform the dataset to one with unit variances and zero covariances.</detail>
        <detail>If <p>x</p> is not symmetric or not exactly symmetric, it will first be symmetrized (<m>{"(x + x^T)/2"}</m>).  For example, a matrix represented by only the upper triangle (other elements are zero or missing from the map) becomes a symmetric matrix with the upper triangle unchanged.</detail>
        <error>If the matrix has fewer than 1 row or fewer than 1 column, this function raises a "too few rows/cols" error.</error>
        <error>If <p>x</p> is an array with ragged columns (arrays of different lengths), this function raises a "ragged columns" error.</error>
        <error>If <p>x</p> is not a square matrix, this function raises a "non-square matrix" error.</error>
      </doc>
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
      val eigvec = (0 until size) map {evd.getEigenVector(_)}
      val order = argsort(eigvalm2)

      var out = new SimpleMatrix(eigvec(order(0))).scale(eigvalm2(order(0))).transpose
      for (j <- 1 until size)
        out = out.combine(j, 0, new SimpleMatrix(eigvec(order(j))).scale(eigvalm2(order(j))).transpose)
      out
    }

    def apply(x: PFAArray[PFAArray[Double]]): PFAArray[PFAArray[Double]] = {
      if (x.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      if (ragged(x))
        throw new PFARuntimeException("ragged columns")
      if (x.toVector.head.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      if (x.toVector.size != x.toVector.head.toVector.size)
        throw new PFARuntimeException("non-square matrix")
      val out = calculate(toDense(x))
      toArrayArray(out)
    }

    def apply(x: PFAMap[PFAMap[java.lang.Double]]): PFAMap[PFAMap[java.lang.Double]] = {
      val keys = (rowKeys(x) union colKeys(x)).toSeq
      if (keys.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      val out = calculate(toDense(x, keys, keys))
      toMapMap(out, (0 until keys.size) map {_.toString}, keys)
    }
  }
  provide(EigenBasis)

  ////   truncate (Truncate)
  object Truncate extends LibFcn with EJMLInterface {
    val name = prefix + "truncate"
    val sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double)), "keep" -> P.Int), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "keep" -> P.Array(P.String)), P.Map(P.Map(P.Double)))))
    val doc =
      <doc>
        <desc>Remove rows from a matrix so that it becomes a projection operator.</desc>
        <param name="x">The matrix to truncate.</param>
        <param name="keep">If <p>x</p> is an array, this is the number of rows to keep, starting with the first row.  If <p>x</p> is a map, this is the set of keys to keep.  If <p>keep</p> is larger than the number of rows or is not a subset of the keys, the excess is ignored.</param>
        <detail>In Principle Component Analysis (PCA), this would be applied to the eigenbasis transformation (<f>la.eigenBasis</f>) to keep only a specified number (or set) of transformed components.</detail>
        <error>If the matrix has fewer than 1 row or fewer than 1 column, this function raises a "too few rows/cols" error.</error>
        <error>If <p>x</p> is an array with ragged columns (arrays of different lengths), this function raises a "ragged columns" error.</error>
      </doc>
    def apply(x: PFAArray[PFAArray[Double]], keep: Int): PFAArray[PFAArray[Double]] = {
      if (x.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      if (ragged(x))
        throw new PFARuntimeException("ragged columns")
      if (x.toVector.head.toVector.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      PFAArray.fromVector(x.toVector.take(keep))
    }
    def apply(x: PFAMap[PFAMap[java.lang.Double]], keep: PFAArray[String]): PFAMap[PFAMap[java.lang.Double]] = {
      val rows = rowKeys(x)
      val cols = colKeys(x)
      if (rows.size < 1  |  cols.size < 1)
        throw new PFARuntimeException("too few rows/cols")
      val tofilter = rows intersect keep.toVector.toSet
      PFAMap.fromMap(x.toMap filterKeys {tofilter.contains(_)})
    }
  }
  provide(Truncate)

}
