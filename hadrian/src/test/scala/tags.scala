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

  object MakeDocs extends Tag("MakeDocs")

  object Lib extends Tag("Lib")
  object LibArray extends Tag("LibArray")
  object LibBytes extends Tag("LibBytes")
  object LibCast extends Tag("LibCast")
  object LibCore extends Tag("LibCore")
  object LibEnum extends Tag("LibEnum")
  object LibFixed extends Tag("LibFixed")
  object LibImpute extends Tag("LibImpute")
  object LibInterp extends Tag("LibInterp")
  object LibKernel extends Tag("LibKernel")
  object LibLA extends Tag("LibLA")
  object LibLink extends Tag("LibLink")
  object LibMap extends Tag("LibMap")
  object LibMath extends Tag("LibMath")
  object LibMetric extends Tag("LibMetric")
  object LibParse extends Tag("LibParse")
  object LibRand extends Tag("LibRand")
  object LibRegex extends Tag("LibRegex")
  object LibSpec extends Tag("LibSpec")
  object LibString extends Tag("LibString")
  object LibTime extends Tag("LibTime")

  object LibProbDist extends Tag("LibProbDist")

  object LibStatTest extends Tag("LibStatTest")
  object LibStatChange extends Tag("LibStatChange")
  object LibStatSample extends Tag("LibStatSample")

  object LibModelCluster extends Tag("LibModelCluster")
  object LibModelNaive extends Tag("LibModelNaive")
  object LibModelNeighbor extends Tag("LibModelNeighbor")
  object LibModelNeural extends Tag("LibModelNeural")
  object LibModelReg extends Tag("LibModelReg")
  object LibModelSvm extends Tag("LibModelSvm")
  object LibModelTree extends Tag("LibModelTree")
}
