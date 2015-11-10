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

package test.scala.makedocs

import scala.language.postfixOps
import scala.collection.mutable

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.signature.Sig
import com.opendatagroup.hadrian.signature.Sigs
import com.opendatagroup.hadrian.signature.PFAVersion
import com.opendatagroup.hadrian.signature.Lifespan
import com.opendatagroup.hadrian.signature.Pattern
import com.opendatagroup.hadrian.signature.P
import com.opendatagroup.hadrian.signature.toLaTeX
import com.opendatagroup.hadrian.signature.toHTML
import com.opendatagroup.hadrian.signature.toText
import com.opendatagroup.hadrian.lib
import com.opendatagroup.hadrian.jvmcompiler.defaultPFAVersion
import com.opendatagroup.hadrian.ast.LibFcn
import test.scala._

@RunWith(classOf[JUnitRunner])
class MakeDocsSuite extends FlatSpec with Matchers {
  val pfaVersion = PFAVersion(0, 8, 1)

  val libfcn =
    lib.core.provides ++
    lib.math.provides ++
    lib.spec.provides ++
    lib.link.provides ++
    lib.kernel.provides ++
    lib.la.provides ++
    lib.metric.provides ++
    lib.rand.provides ++
    lib.string.provides ++
    lib.regex.provides ++
    lib.parse.provides ++
    lib.cast.provides ++
    lib.array.provides ++
    lib.map.provides ++
    lib.bytes.provides ++
    lib.fixed.provides ++
    lib.enum.provides ++
    lib.time.provides ++ 
    lib.impute.provides ++
    lib.interp.provides ++
    lib.prob.dist.provides ++
    lib.stat.test.provides ++
    lib.stat.sample.provides ++
    lib.stat.change.provides ++
    lib.model.reg.provides ++
    lib.model.tree.provides ++
    lib.model.cluster.provides ++
    lib.model.neighbor.provides ++
    lib.model.naive.provides ++
    lib.model.neural.provides ++
    lib.model.svm.provides

  def xmlToLaTeX(node: scala.xml.Node): String = node.child.map(x => x.label match {
    case "#PCDATA" => x.text
    case "#ENTITY" => x.text
    case "p" => s"{\\PFAp ${x.text}}"
    case "c" => s"{\\PFAc ${x.text}}"
    case "t" => s"{\\PFAt ${x.text}}"
    case "tp" => s"{\\PFAtp ${x.text}}"
    case "pf" => s"{\\PFApf ${x.text}}"
    case "m" => s"$$${x.text}$$"
    case "f" => s"{\\PFAf \\hyperlink{${x.text}}{${x.text}}}"
    case "paramField" => ""
    case y => throw new Exception(s"Match error node = $node x = $x x.label = ${x.label} x.text = ${x.text}")
  }).mkString("").replace("%", "\\%").replace("\n", " ").replaceAll("\"([^\"]*)\"", "``$1''").replace("\u2264", "$\\leq$").replace("\u2265", "$\\geq$")

  def safeHTML(x: String) = x.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

  def xmlToHTML(node: scala.xml.Node): String = node.child.map(x => x.label match {
    case "#PCDATA" => safeHTML(x.text)
    case "#ENTITY" => safeHTML(x.text)
    case "p" => s"""<span class="PFAp">${safeHTML(x.text)}</span>"""
    case "c" => s"""<span class="PFAc">${safeHTML(x.text)}</span>"""
    case "t" => s"""<span class="PFAt">${safeHTML(x.text)}</span>"""
    case "tp" => s"""<span class="PFAtp">${safeHTML(x.text)}</span>"""
    case "pf" => s"""<span class="PFApf">${safeHTML(x.text)}</span>"""
    case "m" => s"""\\(${x.text}\\)"""
    case "f" => s"""<span class="PFAf">${safeHTML(x.text)}</span>"""
    case "paramField" => ""
    case y => throw new Exception(s"Match error node = $node x = $x x.label = ${x.label} x.text = ${x.text}")
  }).mkString("")

  "LaTeX generator" must "generate LaTeX" taggedAs(MakeDocs) in {
    val outputFile = new java.io.PrintWriter(new java.io.File("target/libfcns.tex"))

    outputFile.println("\\" + """usepackage{xstring}

\newcommand{\libfcn}[1]{%
    \par\noindent%
    \IfEqCase*{#1}{%""")

    for ((n, f) <- libfcn) {
      val sanitized = f.name.replace("%", "\\%").replace("&", "\\&").replace("^", "\\^{}")

      val asname = sanitized.replace("~", "TILDE")
      val quoted = "\"" + sanitized.replace("~", "\\textasciitilde{}") + "\""

      outputFile.print(s"    {$asname}{\\hypertarget{$asname}{\\noindent \\mbox{\\hspace{0.015\\linewidth}} {\\bf Signature:} ")

      var where = Seq[String]()

      f.sig match {
        case Sig(params, ret, lifespan) if (lifespan.current(pfaVersion)  ||  lifespan.deprecated(pfaVersion)) => {
          val names = params map {case (n, p) => n} mkString(", ")
          val depwarning =
            if (lifespan.deprecated(pfaVersion))
              s"""\\\\ \\vspace{-0.8 cm} \\fbox{\\bf Deprecated; exists until PFA ${lifespan.death.get}${if (lifespan.contingency != None) ": " + lifespan.contingency.get else ""}.}"""
            else
              ""
          outputFile.print(s"\\mbox{\\PFAc \\{$quoted:$$\\!$$ [$names]\\} \\vspace{0.2 cm} \\\\")

          val alreadyLabeled = mutable.Set[String]()
          for ((n, p) <- params) {
            val pp = toLaTeX(p, alreadyLabeled)
            where = where :+ s" & \\PFAc $n \\rm & $pp \\\\"
          }
          val rr = toLaTeX(ret, alreadyLabeled)
          where = where :+ s" & {\\it (returns)} & $rr \\\\ $depwarning"
        }
        case Sigs(sigs) => {
          outputFile.print(s"\\mbox{\\PFAc")

          val possibilities =
            for (Sig(params, ret, lifespan) <- sigs) yield {
              val names = params map {case (n, p) => n} mkString(", ")
              s"\\{$quoted:$$\\!$$ [$names]\\}"
            }

          outputFile.print(possibilities.distinct.mkString(" \\rm or \\PFAc "))

          val newwhere =
            (for (Sig(params, ret, lifespan) <- sigs) yield {
              val alreadyLabeled = mutable.Set[String]()
              var out =
                (for ((n, p) <- params) yield {
                  val pp = toLaTeX(p, alreadyLabeled)
                  s" & \\PFAc $n \\rm & $pp \\\\"
                }).mkString(" ")
              val rr = toLaTeX(ret, alreadyLabeled)
              val depwarning =
                if (lifespan.deprecated(pfaVersion))
                  s"""\\\\ \\vspace{-0.8 cm} \\fbox{\\bf Deprecated; exists until PFA ${lifespan.death.get}${if (lifespan.contingency != None) ": " + lifespan.contingency.get else ""}.}"""
                else
                  ""
              out = out + s" & {\\it (returns)} & $rr \\\\ $depwarning"
              out
            }).mkString(" \\end{tabular} \\vspace{0.2 cm} \\\\ \\mbox{\\hspace{1.5 cm}}or \\vspace{0.2 cm} \\\\ \\begin{tabular}{p{0.01\\linewidth} l p{0.8\\linewidth}}")
          where = where :+ newwhere
        }
      }

      outputFile.print("} \\vspace{0.2 cm} \\\\ ")

      if (!where.isEmpty) {
        outputFile.print("\\rm \\begin{tabular}{p{0.01\\linewidth} l p{0.8\\linewidth}}" + where.mkString(" ") + " \\end{tabular} \\vspace{0.3 cm} \\\\ ")
      }

      val desc = xmlToLaTeX(f.doc \ "desc" head)
      outputFile.print(s"\\mbox{\\hspace{0.015\\linewidth}} {\\bf Description:} $desc \\vspace{0.2 cm} \\\\ ")

      val params = f.doc \ "param"
      val rets = f.doc \ "ret"
      if (!params.isEmpty || !rets.isEmpty) {
        outputFile.print(s"\\mbox{\\hspace{0.015\\linewidth}} {\\bf Parameters:} \\vspace{0.2 cm} \\\\ \\begin{tabular}{p{0.01\\linewidth} l p{0.8\\linewidth}} ")

        for (param <- params) {
          val name = param \ "@name"

          var fields = ""
          val paramFields = param \ "paramField"
          if (!paramFields.isEmpty) {
            fields = fields + "\\begin{description*}"
            for (field <- paramFields) {
              val fieldName = field \ "@name"
              fields = fields + s"\\item[\\PFAc $fieldName:] ${xmlToLaTeX(field)} "
            }
            fields = fields + "\\end{description*}"
          }

          outputFile.print(s" & \\PFAc $name \\rm & ${xmlToLaTeX(param)} $fields \\\\ ")
        }

        for (ret <- rets) {
          outputFile.print(s" & {\\it (return value)} \\rm & ${xmlToLaTeX(ret)} \\\\ ")
        }

        outputFile.print("\\end{tabular} \\vspace{0.2 cm} \\\\ ")
      }

      val details = f.doc \ "detail"
      val nondeterministic = f.doc \ "nondeterministic"

      if (!details.isEmpty  ||  !nondeterministic.isEmpty) {
        outputFile.print("\\mbox{\\hspace{0.015\\linewidth}} {\\bf Details:} \\vspace{0.2 cm} \\\\ \\mbox{\\hspace{0.045\\linewidth}} \\begin{minipage}{0.935\\linewidth}")
        outputFile.print(details.map(xmlToLaTeX).mkString(" \\vspace{0.1 cm} \\\\ "))

        if (!details.isEmpty  &&  !nondeterministic.isEmpty)
          outputFile.print(" \\vspace{0.1 cm} \\\\ ")

        outputFile.print(nondeterministic.map(_ \ "@type" toString).map(_ match {
          case "pseudorandom" => "{\\bf Nondeterministic: pseudorandom.} This function intentionally gives different results every time it is executed."
          case "unstable" => "{\\bf Nondeterministic: unstable.} This function gives the same results every time it is executed, but those results may not be exactly the same on all systems."
          case "unordered" => "{\\bf Nondeterministic: unordered.} This function gives the same set of values every time it is executed on all systems, but the values may have a different order."
        }).mkString(" \\vspace{0.1 cm} \\\\ "))

        outputFile.print("\\end{minipage} \\vspace{0.2 cm} \\vspace{0.2 cm} \\\\ ")
      }

      val errors = f.doc \ "error"
      if (!errors.isEmpty) {
        outputFile.print("\\mbox{\\hspace{0.015\\linewidth}} {\\bf Runtime Errors:} \\vspace{0.2 cm} \\\\ \\mbox{\\hspace{0.045\\linewidth}} \\begin{minipage}{0.935\\linewidth}")
        outputFile.print(errors.map(error => "{\\bf \\#" + error.attributes("code").toString + ":} " + xmlToLaTeX(error)).mkString(" \\vspace{0.1 cm} \\\\ "))
        outputFile.print("\\end{minipage} \\vspace{0.2 cm} \\vspace{0.2 cm} \\\\ ")
      }

      outputFile.println("}}%")
    }

    outputFile.println("""    }[{\bf FIXME: LaTeX error: wrong libfcn name!}]%
}%""")
    outputFile.close()

  }

  "HTML generator" must "generate HTML" taggedAs(MakeDocs) in {
    val outputFile = new java.io.PrintWriter(new java.io.File("target/libfcns.html"))

//     outputFile.println(s"""<html><head><script type="text/javascript"
//   src="https://cdn.mathjax.org/mathjax/latest/MathJax.js?config=TeX-AMS-MML_HTMLorMML">
// </script></head><body>""")

    var lastHeading = ""
    outputFile.println(s"""<div id="lib:" class="PFAhead"><h2>core</h2></div>""")

    for ((n, f) <- libfcn) {
      val heading = f.name.split('.').init.mkString(".")
      if (heading != lastHeading) {
        outputFile.println(s"""<div id="lib:" class="PFAhead"><h2 id="lib:$heading">$heading</h2></div>""")
        lastHeading = heading
      }

      outputFile.println(s"""<div id="fcn:${f.name}" class="PFAfcndef">""")

      def handleSig(sig: Sig): String = sig match {
        case Sig(params, ret, lifespan) =>
          val depwarning =
            if (lifespan.deprecated(pfaVersion))
              s"""<div style="margin-top: -20px; margin-bottom: 20px"><span style="border: 1px solid #0e5f80; padding: 3px;"><b>Deprecated; exists until PFA ${lifespan.death.get}${if (lifespan.contingency != None) ": " + lifespan.contingency.get else ""}.</b></span></div>"""
            else
              ""
          var out = ""
          val names = params map {case (n, p) => n} mkString(", ")
          out = out + s"""{<b>"${f.name}":</b> [$names]}<br><table class="PFAwhere">"""
          val alreadyLabeled = mutable.Set[String]()
          for ((n, p) <- params) {
            val pp = toHTML(p, alreadyLabeled)
            out = out + s"""<tr><td><b>$n</b></td><td>$pp</td></tr>"""
          }
          val rr = toHTML(ret, alreadyLabeled)
          out = out + s"""<tr><td><i>(returns)</i></td><td>$rr</td></tr></table>$depwarning"""
          out
      }

      f.sig match {
        case sig: Sig => outputFile.println(handleSig(sig))
        case Sigs(sigs) => outputFile.println(sigs.map(handleSig).mkString("\n"))
      }

      val desc = xmlToHTML(f.doc \ "desc" head)
      outputFile.println(s"""<p><b>Description:</b> $desc</p>""")

      val params = f.doc \ "param"
      val rets = f.doc \ "ret"
      if (!params.isEmpty) {
        outputFile.println(s"""<p><b>Parameters:</b><table class="PFAparams">""")

        for (param <- params) {
          val name = param \ "@name"
          outputFile.println(s"""<tr><td><b>$name</b></td><td>${xmlToHTML(param)}</td></tr>""")

          val paramFields = param \ "paramField"
          if (!paramFields.isEmpty) {
            outputFile.println(s"""<tr><td></td><td><table class="PFAfields">""")
            for (field <- paramFields) {
              val fieldName = field \ "@name"
              outputFile.println(s"""<tr><td><b>$fieldName</b></td><td>${xmlToHTML(field)}</td></tr>""")
            }
            outputFile.println(s"""</table></td></tr>""")
          }
        }
        outputFile.println(s"""</table></p>""")
      }

      if (!rets.isEmpty) {
        outputFile.println(s"""<p><b>Returns:</b><table class="PFAparams">""")
        for (ret <- rets) {
          outputFile.println(s"""<tr><td>${xmlToHTML(ret)}</td></tr>""")
        }
        outputFile.println(s"""</table></p>""")
      }

      val details = f.doc \ "detail"
      val nondeterministic = f.doc \ "nondeterministic"

      if (!details.isEmpty  ||  !nondeterministic.isEmpty) {
        val nondeterministicDetails = (nondeterministic.map(_ \ "@type" toString).map(_ match {
          case "pseudorandom" => "<b>Nondeterministic: pseudorandom.</b> This function intentionally gives different results every time it is executed."
          case "unstable" => "<b>Nondeterministic: unstable.</b> This function gives the same results every time it is executed, but those results may not be exactly the same on all systems."
          case "unordered" => "<b>Nondeterministic: unordered.</b> This function gives the same set of values every time it is executed on all systems, but the values may have a different order."
        }))

        val asHtml = (details.map(xmlToHTML) ++ nondeterministicDetails).mkString("</li><li>")
        outputFile.println(s"""<p><b>Details:</b><ul><li>$asHtml</li></ul></p>""")
      }

      val errors = f.doc \ "error"
      if (!errors.isEmpty) {
        outputFile.println(s"""<p><b>Runtime Errors:</b><ul><li>${errors.map(error => "<b>#" + error.attributes("code") + ":</b> " + xmlToHTML(error)).mkString("</li><li>")}</li></ul></p>""")
      }

      outputFile.println("</div>")
    }

    outputFile.close()
  }

  "Specification generator" must "generate official specification (or one that is equal to it)" taggedAs(MakeDocs) in {
    def handlePattern(p: Pattern, alreadyLabeled: mutable.Set[String]): scala.xml.NodeSeq = p match {
      case P.Null => scala.xml.Text("null")
      case P.Boolean => scala.xml.Text("boolean")
      case P.Int => scala.xml.Text("int")
      case P.Long => scala.xml.Text("long")
      case P.Float => scala.xml.Text("float")
      case P.Double => scala.xml.Text("double")
      case P.Bytes => scala.xml.Text("bytes")
      case P.String => scala.xml.Text("string")
      case P.Array(items) => <array>{handlePattern(items, alreadyLabeled)}</array>
      case P.Map(values) => <map>{handlePattern(values, alreadyLabeled)}</map>
      case P.Union(types) => {types map {t: Pattern => <union>{handlePattern(t, alreadyLabeled)}</union>}}
      // case P.Fixed(size, None) => <fixed size={size} />
      // case P.Fixed(size, Some(fullName)) => <fixed size={size} name={fullName}/>
      // case P.Enum(symbols, None) => <enum>{symbols.mkString(", ")}</enum>
      // case P.Enum(symbols, Some(fullName)) => <enum name={fullName}>{symbols.mkString(", ")}</enum>
      // case P.Record(fields, None) => <record>{fields map {case (n, f) => <field name={n}>{f}</field>}}</record>
      // case P.Record(fields, Some(fullName)) => <record name={fullName}>{fields map {case (n, f) => <field name={n}>{f}</field>}}</record>
      case P.Fcn(params, ret) => scala.xml.Text("\n          ") ++
          <function>{params map {x => scala.xml.Text("\n            ") ++ <par>{handlePattern(x, alreadyLabeled)}</par>}}
            <ret>{handlePattern(ret, alreadyLabeled)}</ret>
          </function>
      case P.Wildcard(label, _) if (alreadyLabeled contains label) => <ref label={label} />
      case P.Wildcard(label, oneOf) if (oneOf.isEmpty) => alreadyLabeled.add(label); <any label={label} />
      case P.Wildcard(label, oneOf) => alreadyLabeled.add(label); <any label={label} of={oneOf.map(toText.applyType).mkString(", ")} />
      case P.WildRecord(label, _) if (alreadyLabeled contains label) => <ref label={label} />
      case P.WildRecord(label, minimalFields) if (minimalFields.isEmpty) => alreadyLabeled.add(label); <record label={label} />
      case P.WildRecord(label, minimalFields) => alreadyLabeled.add(label); <record label={label}>{minimalFields map {case (n, f) => <field name={n}>{handlePattern(f, alreadyLabeled)}</field>}}</record>
      case P.WildEnum(label) if (alreadyLabeled contains label) => <ref label={label} />
      case P.WildEnum(label) => alreadyLabeled.add(label); <enum label={label} />
      case P.WildFixed(label) if (alreadyLabeled contains label) => <ref label={label} />
      case P.WildFixed(label) => alreadyLabeled.add(label); <fixed label={label} />
      case P.EnumFields(label, _) if (alreadyLabeled contains label) => <ref label={label} />
      case P.EnumFields(label, wildRecord) => alreadyLabeled.add(label); <enum label={label} ofRecord={wildRecord} />
    }

    def handleSig(sig: Sig) = {
      val alreadyLabeled = mutable.Set[String]()
      var out =
      <sig>{sig.params map {case (n, p) => scala.xml.Text("\n        ") ++ <par name={n}>{handlePattern(p, alreadyLabeled)}</par>}}
        <ret>{handlePattern(sig.ret, alreadyLabeled)}</ret>
      </sig>

      sig.lifespan match {
        case Lifespan(Some(x), _, _, _) => out = out % scala.xml.Attribute("birth", scala.xml.Text(x.toString), scala.xml.Null)
        case _ =>
      }

      sig.lifespan match {
        case Lifespan(_, Some(x), Some(y), Some(z)) => out = out %
          scala.xml.Attribute("deprecation", scala.xml.Text(x.toString), scala.xml.Null) %
          scala.xml.Attribute("death", scala.xml.Text(y.toString), scala.xml.Null) %
          scala.xml.Attribute("contingency", scala.xml.Text(z.toString), scala.xml.Null)
        case Lifespan(_, Some(x), Some(y), None) => out = out %
          scala.xml.Attribute("deprecation", scala.xml.Text(x.toString), scala.xml.Null) %
          scala.xml.Attribute("death", scala.xml.Text(y.toString), scala.xml.Null) %
          scala.xml.Attribute("contingency", scala.xml.Text("none"), scala.xml.Null)
        case _ =>
      }

      out
    }

    val errorCodes = mutable.Map[Int, String]()

    def handleFcn(f: LibFcn) = {
      val sigs = f.sig match {
        case sig: Sig => handleSig(sig)
        case Sigs(sigs) => sigs.zipWithIndex map {case (x, i) => if (i == sigs.size - 1) handleSig(x) else {handleSig(x)} ++ scala.xml.Text("\n      ")}
      }

      for (err <- f.doc \\ "error") {
        val code = java.lang.Integer.parseInt(err.attributes("code").toString)
        if (errorCodes contains code)
          throw new Exception(s"$f.name defines error code $code, but this is already defined in ${errorCodes(code)}")
        errorCodes(code) = f.name
      }

      val out =
    <fcn name={f.name}>
      {sigs}
      {f.doc}
    </fcn>

      out ++ scala.xml.Text("\n\n    ")
    }

    val wholeDocument = <PFASpecification>
  <version>{defaultPFAVersion}</version>
  <libfcns>
    {libfcn.values.map(handleFcn)}</libfcns>
</PFASpecification>

    val outputFile = new java.io.PrintWriter(new java.io.File("target/libfcns.xml"))
    outputFile.println(wholeDocument)
    outputFile.close()
  }

}
