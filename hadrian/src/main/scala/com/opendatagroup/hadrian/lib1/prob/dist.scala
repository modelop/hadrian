package com.opendatagroup.hadrian.lib1.prob

import scala.annotation.tailrec
import scala.collection.immutable.ListMap

import org.apache.avro.Schema

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.errors.PFASemanticException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema
import com.opendatagroup.hadrian.jvmcompiler.javaType
import com.opendatagroup.hadrian.jvmcompiler.JVMNameMangle
import com.opendatagroup.hadrian.jvmcompiler.PFAEngineBase

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFARecord

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

package object dist {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "prob.dist."

  //////////////////////////////////////////////////////////////////// 

  ////   gaussianLL (GaussianLL)
  object GaussianLL extends LibFcn {
    val name = prefix + "gaussianLL"
    val sig = Sigs(List(Sig(List("x" -> P.Double, "mu" -> P.Double, "sigma" -> P.Double), P.Double),
                        Sig(List("x" -> P.Double, "params" -> P.WildRecord("A", ListMap("mean" -> P.Double, "variance" -> P.Double))), P.Double)))
    val doc =
      <doc>
        <desc>Compute the log-likelihood of a Gaussian (normal) distribution parameterized by <p>mu</p> and <p>sigma</p> or a record <p>params</p>.</desc>
        <param name="x">Value at which to compute the log-likelihood.</param>
        <param name="mu">Centroid of the distribution (same as <pf>mean</pf>).</param>
        <param name="sigma">Width of the distribution (same as the square root of <pf>variance</pf>).</param>
        <param name="params">Alternate way of specifying the parameters of the distribution; this record could be created by <f>stat.sample.update</f>.</param>
        <ret>With <m>{"\\mu"}</m> = <p>mu</p> or <pf>mean</pf> and <m>{"\\sigma"}</m> = <p>sigma</p> or the square root of <pf>variance</pf>, this function returns <m>{"-(x - \\mu)^2/(2 \\sigma^2) - \\log(\\sigma \\sqrt{2\\pi})"}</m>.</ret>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode = {
      paramTypes(1) match {
        case _: AvroDouble =>
          JavaCode("%s.MODULE$.apply(%s, %s, %s)",
            this.getClass.getName,
            wrapArg(0, args, paramTypes, true),
            wrapArg(1, args, paramTypes, true),
            wrapArg(2, args, paramTypes, true))
        case record: AvroRecord =>
          JavaCode("%s.MODULE$.apply(%s, ((%s)%s).%s, Math.sqrt(((%s)%s).%s))",
            this.getClass.getName,
            wrapArg(0, args, paramTypes, true),
            javaType(paramTypes(1).asInstanceOf[AvroType], false, true, false),
            wrapArg(1, args, paramTypes, true),
            JVMNameMangle.s("mean"),
            javaType(paramTypes(1).asInstanceOf[AvroType], false, true, false),
            wrapArg(1, args, paramTypes, true),
            JVMNameMangle.s("variance"))
      }
    }

    def apply(x: Double, mu: Double, sigma: Double): Double =
      -(x - mu)*(x - mu)/(2.0 * sigma*sigma) - Math.log(sigma * Math.sqrt(2.0 * Math.PI))
  }
  provide(GaussianLL)

}
