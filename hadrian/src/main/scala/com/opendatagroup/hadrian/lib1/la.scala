package com.opendatagroup.hadrian.lib1

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

  ////////////////////////////////////////////////////////////////////

  ////   map (MapApply)
  object MapApply extends LibFcn {
    val name = prefix + "map"
    val sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double)), "fcn" -> P.Fcn(List(P.Double), P.Double)), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "fcn" -> P.Fcn(List(P.Double), P.Double)), P.Map(P.Map(P.Double)))))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each element from <p>x</p>.</desc>
        <detail>The order in which elements are computed is not specified, and may be in parallel.</detail>
      </doc>
    def apply(x: PFAArray[PFAArray[Double]], fcn: (Double) => Double): PFAArray[PFAArray[Double]] = {
      val xv = x.toVector map { _.toVector }

      val sizes = xv map { _.size }

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
  object ZipMap extends LibFcn {
    val name = prefix + "zipmap"
    val sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double)), "y" -> P.Array(P.Array(P.Double)), "fcn" -> P.Fcn(List(P.Double, P.Double), P.Double)), P.Array(P.Array(P.Double))),
                        Sig(List("x" -> P.Map(P.Map(P.Double)), "y" -> P.Map(P.Map(P.Double)), "fcn" -> P.Fcn(List(P.Double, P.Double), P.Double)), P.Map(P.Map(P.Double)))))
    val doc =
      <doc>
        <desc>Apply <p>fcn</p> to each pair of elements from <p>x</p> and <p>y</p>.</desc>
        <detail>The order in which elements are computed is not specified, and may be in parallel.</detail>
        <error>If any element in <p>x</p> does not have a corresponding element in <p>y</p> (or vice-versa), this function raises a "misaligned matrices" error.</error>
      </doc>
    def apply(x: PFAArray[PFAArray[Double]], y: PFAArray[PFAArray[Double]], fcn: (Double, Double) => Double): PFAArray[PFAArray[Double]] = {
      val xv = x.toVector map { _.toVector }
      val yv = y.toVector map { _.toVector }

      val sizes = xv map { _.size }
      if (sizes != (yv map { _.size }))
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

      val indexes = xv.iterator flatMap {case (i, v) => v.iterator map {case (j, _) => (i, j)}} toList

      if (indexes != (yv.iterator flatMap {case (i, v) => v.iterator map {case (j, _) => (i, j)}} toList))
        throw new PFARuntimeException("misaligned matrices")

      PFAMap.fromMap(xv map { case (i, v) => (i, PFAMap.fromMap(v map {case (j, _) => (j, java.lang.Double.valueOf(fcn(xv(i)(j), yv(i)(j))))})) })
    }
  }
  provide(ZipMap)

  // ////    (ZipMap)
  // object ZipMap extends LibFcn {
  //   val name = prefix + "zipmap"
  //   val sig = Sigs(List(Sig(List("x" -> P.Array(P.Array(P.Double)), "y" -> P.Array(P.Array(P.Double)), "fcn" -> P.Fcn(List(P.Double, P.Double), P.Double)), P.Array(P.Array(P.Double))),
  //                       Sig(List("x" -> P.Map(P.Map(P.Double)), "y" -> P.Map(P.Map(P.Double)), "fcn" -> P.Fcn(List(P.Double, P.Double), P.Double)), P.Map(P.Map(P.Double)))))
  //   val doc =
  //     <doc>
  //     </doc>
  // }
  // provide()
}
