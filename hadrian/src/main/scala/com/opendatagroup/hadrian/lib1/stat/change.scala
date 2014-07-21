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

package com.opendatagroup.hadrian.lib1.stat

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

package object change {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "stat.change."

  //////////////////////////////////////////////////////////////////// 

  ////   updateTrigger (UpdateTrigger)
  object UpdateTrigger extends LibFcn {
    val name = prefix + "updateTrigger"
    val sig = Sig(List("predicate" -> P.Boolean, "history" -> P.Union(List(P.Null, P.WildRecord("A", ListMap("numEvents" -> P.Int, "numRuns" -> P.Int, "currentRun" -> P.Int, "longestRun" -> P.Int))))), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Update the state of a trigger that counts the number of times <p>predicate</p> is satisfied (<c>true</c>), as well as the number and lengths of runs of <c>true</c>.</desc>
        <param name="predicate">Expression that evaluates to <c>true</c> or <c>false</c>.</param>
        <param name="history">Summary of previous results of the <p>predicate</p>, or <c>null</c> to start a new history.
          <paramField name="numEvents">The number of times <p>predicate</p> evaluated to <c>true</c>.</paramField>
          <paramField name="numRuns">The number of contiguous intervals in which <p>predicate</p> was <c>true</c>, including the current one.</paramField>
          <paramField name="currentRun">If <p>predicate</p> is <c>false</c>, <pf>currentRun</pf> is 0.  Otherwise, <pf>currentRun</pf> is incremented (greater than or equal to 1 if <p>predicate</p> evaluated to <c>true</c>).</paramField>
          <paramField name="longestRun">The longest run observed so far; may be equal to <pf>currentRun</pf>.</paramField>
        </param>
        <ret>If <p>history</p> is <c>null</c>, a new record is created with <pf>numEvents</pf> = 1, <pf>numRuns</pf> = 1, <pf>currentRun</pf> = 1, <pf>longestRun</pf> = 1 if <p>predicate</p> is <c>true</c>, <pf>numEvents</pf> = 0, <pf>numRuns</pf> = 0, <pf>currentRun</pf> = 0, <pf>longestRun</pf> = 0 if <p>predicate</p> is <c>false</c>.  If the input <p>history</p> has fields other than <pf>numEvents</pf>, <pf>numRuns</pf>, <pf>currentRun</pf>, and <pf>longestRun</pf>, they are copied unaltered to the output state.</ret>
        <error>If any of <pf>numEvents</pf>, <pf>numRuns</pf>, <pf>currentRun</pf>, and <pf>longestRun</pf> are less than 0, a "counter out of range" error is raised.</error>
        <error>If <p>history</p> is <c>null</c> and the record type has fields other than <pf>numEvents</pf>, <pf>numRuns</pf>, <pf>currentRun</pf>, and <pf>longestRun</pf>, then a "cannot initialize unrecognized fields" error is raised.  Unrecognized fields are only allowed if an initial record is provided.</error>
      </doc>

    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode = {
      val record = retType.asInstanceOf[AvroRecord]

      val hasOthers = !(record.fields.map(_.name).toSet subsetOf Set("numEvents", "numRuns", "currentRun", "longestRun"))

      JavaCode("%s.MODULE$.apply(%s, %s, thisEngineBase, %s, %s)",
        this.getClass.getName,
        wrapArg(0, args, paramTypes, true),
        wrapArg(1, args, paramTypes, true),
        javaSchema(retType, false),
        (if (hasOthers) "true" else "false"))
    }

    def apply(predicate: Boolean, history: AnyRef, pfaEngineBase: PFAEngineBase, schema: Schema, hasOthers: Boolean): PFARecord = {
      if (history == null) {
        if (hasOthers)
          throw new PFARuntimeException("cannot initialize unrecognized fields")
        if (predicate)
          pfaEngineBase.fromJson("""{"numEvents": 1, "numRuns": 1, "currentRun": 1, "longestRun": 1}""", schema).asInstanceOf[PFARecord]
        else
          pfaEngineBase.fromJson("""{"numEvents": 0, "numRuns": 0, "currentRun": 0, "longestRun": 0}""", schema).asInstanceOf[PFARecord]
      }

      else {
        val record = history.asInstanceOf[PFARecord]
        var numEvents = record.get("numEvents").asInstanceOf[java.lang.Number].intValue
        var numRuns = record.get("numRuns").asInstanceOf[java.lang.Number].intValue
        var currentRun = record.get("currentRun").asInstanceOf[java.lang.Number].intValue
        var longestRun = record.get("longestRun").asInstanceOf[java.lang.Number].intValue

        if (numEvents < 0  ||  numRuns < 0  ||  currentRun < 0  ||  longestRun < 0)
          throw new PFARuntimeException("counter out of range")

        if (predicate) {
          numEvents += 1

          if (currentRun == 0)
            numRuns += 1

          currentRun += 1

          if (currentRun > longestRun)
            longestRun = currentRun

        }
        else {
          currentRun = 0
        }

        record.multiUpdate(Array("numEvents", "numRuns", "currentRun", "longestRun"), Array(numEvents, numRuns, currentRun, longestRun))
      }

    }
  }
  provide(UpdateTrigger)

  ////   zValue (ZValue)
  object ZValue extends LibFcn {
    val name = prefix + "zValue"
    val sig = Sig(List("x" -> P.Double, "meanVariance" -> P.WildRecord("A", ListMap("count" -> P.Double, "mean" -> P.Double, "variance" -> P.Double)), "unbiased" -> P.Boolean), P.Double)
    val doc =
      <doc>
        <desc>Calculate the z-value between <p>x</p> and a normal distribution with a given mean and variance.</desc>
        <param name="x">Value to test.</param>
        <param name="meanVariance">A record with <pf>count</pf>, <pf>mean</pf>, and <pf>variance</pf>, such as the output of <f>stat.sample.Update</f>.</param>
        <param name="unbiased">If <c>true</c>, use <pf>count</pf> to correct for the bias due to the fact that a variance centered on the mean has one fewer degrees of freedom than the dataset that it was sampled from (Bessel's correction).</param>
        <ret>If <p>unbiased</p> is <c>false</c>, <m>{"(x - mean)/\\sqrt{variance}"}</m>; otherwise <m>{"(x - mean)(1/\\sqrt{variance})\\sqrt{count/(count - 1)}"}</m>.</ret>
      </doc>
    override def javaCode(args: Seq[JavaCode], argContext: Seq[AstContext], paramTypes: Seq[Type], retType: AvroType): JavaCode =
      JavaCode("%s.MODULE$.apply(%s, ((%s)%s).%s, ((%s)%s).%s, ((%s)%s).%s, %s)",
        this.getClass.getName,
        wrapArg(0, args, paramTypes, true),
        javaType(paramTypes(1).asInstanceOf[AvroType], false, true, false),
        wrapArg(1, args, paramTypes, true),
        JVMNameMangle.s("count"),
        javaType(paramTypes(1).asInstanceOf[AvroType], false, true, false),
        wrapArg(1, args, paramTypes, true),
        JVMNameMangle.s("mean"),
        javaType(paramTypes(1).asInstanceOf[AvroType], false, true, false),
        wrapArg(1, args, paramTypes, true),
        JVMNameMangle.s("variance"),
        wrapArg(2, args, paramTypes, true))
    def apply(x: Double, count: Double, mean: Double, variance: Double, unbiased: Boolean): Double =
      if (unbiased)
        ((x - mean)/Math.sqrt(variance)) * Math.sqrt((count) / (count - 1.0))
      else
        ((x - mean)/Math.sqrt(variance))
  }
  provide(ZValue)

  ////   updateCUSUM (UpdateCUSUM)
  object UpdateCUSUM extends LibFcn {
    val name = prefix + "updateCUSUM"
    val sig = Sig(List("logLikelihoodRatio" -> P.Double, "last" -> P.Double, "reset" -> P.Double), P.Double)
    val doc =
      <doc>
        <desc>Update a cumulative sum (CUSUM) to detect the transition of a dataset from one distribution to another.</desc>
        <param name="logLikelihoodRatio">The logarithm of the ratio of the likelihood of a value for the alterate and baseline distributions: <m>{"\\ln(\\mbox{alt}_{L}/\\mbox{base}_{L})"}</m>, which is <m>{"\\mbox{alt}_{LL} - \\mbox{base}_{LL}"}</m> where <m>L</m> is likelihood and <m>LL</m> is log-likelihood.  Consider using something like <c>{"""{"-": [{"prob.dist.gaussianLL": [...]}, {"prob.dist.gaussianLL": [...]}]}"""}</c>.</param>
        <param name="last">The previous return value from this function.</param>
        <param name="reset">A low value (usually consistent with the baseline hypothesis, such as 0) at which the cumulative sum resets, rather than accumulate very low values and become insensitive to future changes.</param>
        <ret>An incremented cumulative sum.  The output is <m>{"\\max\\{logLikelihoodRatio + last, reset\\}"}</m>.</ret>
      </doc>
    def apply(logLikelihoodRatio: Double, last: Double, reset: Double): Double = {
      val out = logLikelihoodRatio + last
      if (out > reset)
        out
      else
        reset
    }
  }
  provide(UpdateCUSUM)

}
