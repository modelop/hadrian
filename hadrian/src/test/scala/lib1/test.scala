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

package test.scala.lib1.test

import scala.collection.JavaConversions._

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.errors._
import test.scala._

@RunWith(classOf[JUnitRunner])
class Lib1TestSuite extends FlatSpec with Matchers {

  "KSTwoSample" must "compute p-value" taggedAs(Lib1, Lib1Test) in {
    val engine = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
action:
  - let:
      y:
       type: {type: array, items: double}
       new:
          - -0.6852076
          - -0.62961294
          -  1.47603708
          - -1.66223465
          - -0.34015844
          -  1.50852341
          - -0.0348001
          - -0.59529466
          -  0.71956491
          - -0.77441149

  - {test.kolmogorov: [input, y]}
""").head
    val x = """[0.53535232,
                0.66523251,
                0.92733853,
                0.45348014,
               -0.37606127,
                1.22115272,
               -0.36264331,
                2.15954568,
                0.49463302,
               -0.81670101]"""
    engine.action(engine.jsonInput(x)).asInstanceOf[java.lang.Double].doubleValue should be (0.31285267601695582 +- .0000001)
    }
}
