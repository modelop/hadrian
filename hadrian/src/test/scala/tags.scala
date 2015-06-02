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

import org.scalatest.Tag

package test.scala {
  object AstToJson extends Tag("AstToJson")
  object Data extends Tag("Data")
  object JsonToAst extends Tag("JsonToAst")
  object JVMCompilation extends Tag("JVMCompilation")
  object Memory extends Tag("Memory")
  object RandomJson extends Tag("RandomJson")
  object Shared extends Tag("Shared")
  object SignatureMatch extends Tag("SignatureMatch")
  object DataType extends Tag("DataType")
  object DumpState extends Tag("DumpState")
  object TypeCheck extends Tag("TypeCheck")
  object Yaml extends Tag("Yaml")

  object Speed extends Tag("Speed")
  object MultiThreaded extends Tag("MultiThreaded")
  object ThreadSafety extends Tag("ThreadSafety")

  object MakeDocsLatex extends Tag("MakeDocsLatex")

  object Lib1 extends Tag("Lib1")
  object Lib1Array extends Tag("Lib1Array")
  object Lib1Bytes extends Tag("Lib1Bytes")
  object Lib1Cast extends Tag("Lib1Cast")
  object Lib1Core extends Tag("Lib1Core")
  object Lib1Enum extends Tag("Lib1Enum")
  object Lib1Fixed extends Tag("Lib1Fixed")
  object Lib1Impute extends Tag("Lib1Impute")
  object Lib1Interp extends Tag("Lib1Interp")
  object Lib1LA extends Tag("Lib1LA")
  object Lib1Map extends Tag("Lib1Map")
  object Lib1Math extends Tag("Lib1Math")
  object Lib1Metric extends Tag("Lib1Metric")
  object Lib1Parse extends Tag("Lib1Parse")
  object Lib1Rand extends Tag("Lib1Rand")
  object Lib1Regex extends Tag("Lib1Regex")
  object Lib1Spec extends Tag("Lib1Spec")
  object Lib1String extends Tag("Lib1String")
  object Lib1Time extends Tag("Lib1Time")

  object Lib1ProbDist extends Tag("Lib1ProbDist")

  object Lib1StatChange extends Tag("Lib1StatChange")
  object Lib1StatSample extends Tag("Lib1StatSample")

  object Lib1ModelCluster extends Tag("Lib1ModelCluster")
  object Lib1ModelNeighbor extends Tag("Lib1ModelNeighbor")
  object Lib1ModelReg extends Tag("Lib1ModelReg")
  object Lib1ModelTree extends Tag("Lib1ModelTree")
}
