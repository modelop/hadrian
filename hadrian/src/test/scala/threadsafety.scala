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

package test.scala.threadsafety

import scala.collection.JavaConversions._
import scala.collection.mutable

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import com.opendatagroup.hadrian.ast._
import com.opendatagroup.hadrian.data._
import com.opendatagroup.hadrian.errors._
import com.opendatagroup.hadrian.jvmcompiler._
import com.opendatagroup.hadrian.reader._
import com.opendatagroup.hadrian.yaml._
import test.scala._

@RunWith(classOf[JUnitRunner])
class ThreadSafetySuite extends FlatSpec with Matchers {
  "Thread safety" must "not run two actions at the same time" taggedAs(ThreadSafety) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  - log: [{string: start}]
  - a.permutations:
      new: [1, 2, 3, 4, 5, 6, 7, 8]
      type: {type: array, items: int}
  - log: [{string: stop}]
""").head

    val accumulatedLog = mutable.ListBuffer[String]()
    engine.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }

    val thread1 = new Thread(new Runnable {def run(): Unit = engine.action(null)})
    val thread2 = new Thread(new Runnable {def run(): Unit = engine.action(null)})

    thread1.start
    thread2.start
    while (thread1.isAlive  ||  thread2.isAlive)
      Thread.sleep(100)

    accumulatedLog.toSeq should be (Seq(""""start"""", """"stop"""", """"start"""", """"stop""""))
  }

  it must "allow two actions from different engines to run at the same time" taggedAs(ThreadSafety) in {
    val engines = PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  - log: [{string: start}]
  - a.permutations:
      new: [1, 2, 3, 4, 5, 6, 7, 8]
      type: {type: array, items: int}
  - log: [{string: stop}]
""", multiplicity = 2)

    val accumulatedLog = mutable.ListBuffer[String]()
    engines.head.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }
    engines.last.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }

    val thread1 = new Thread(new Runnable {def run(): Unit = engines.head.action(null)})
    val thread2 = new Thread(new Runnable {def run(): Unit = engines.last.action(null)})

    thread1.start
    thread2.start
    while (thread1.isAlive  ||  thread2.isAlive)
      Thread.sleep(100)

    accumulatedLog.toSeq should be (Seq(""""start"""", """"start"""", """"stop"""", """"stop""""))
  }

  it must "not run two emit actions at the same time" taggedAs(ThreadSafety) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: string
method: emit
action:
  - emit: {string: start}
  - a.permutations:
      new: [1, 2, 3, 4, 5, 6, 7, 8]
      type: {type: array, items: int}
  - emit: {string: stop}
""").head

    val accumulatedLog = mutable.ListBuffer[String]()
    engine.asInstanceOf[PFAEmitEngine[java.lang.Void, String]].emit = (x: String) => accumulatedLog.synchronized { accumulatedLog.append(x) }

    val thread1 = new Thread(new Runnable {def run(): Unit = engine.action(null)})
    val thread2 = new Thread(new Runnable {def run(): Unit = engine.action(null)})

    thread1.start
    thread2.start
    while (thread1.isAlive  ||  thread2.isAlive)
      Thread.sleep(100)

    accumulatedLog.toSeq should be (Seq("start", "stop", "start", "stop"))
  }

  it must "allow two emit actions from different engines to run the same time" taggedAs(ThreadSafety) in {
    val engines = PFAEngine.fromYaml("""
input: "null"
output: string
method: emit
action:
  - emit: {string: start}
  - a.permutations:
      new: [1, 2, 3, 4, 5, 6, 7, 8]
      type: {type: array, items: int}
  - emit: {string: stop}
""", multiplicity = 2)

    val accumulatedLog = mutable.ListBuffer[String]()
    engines.head.asInstanceOf[PFAEmitEngine[java.lang.Void, String]].emit = (x: String) => accumulatedLog.synchronized { accumulatedLog.append(x) }
    engines.last.asInstanceOf[PFAEmitEngine[java.lang.Void, String]].emit = (x: String) => accumulatedLog.synchronized { accumulatedLog.append(x) }

    val thread1 = new Thread(new Runnable {def run(): Unit = engines.head.action(null)})
    val thread2 = new Thread(new Runnable {def run(): Unit = engines.last.action(null)})

    thread1.start
    thread2.start
    while (thread1.isAlive  ||  thread2.isAlive)
      Thread.sleep(100)

    accumulatedLog.toSeq should be (Seq("start", "start", "stop", "stop"))
  }

  it must "not run two fold actions at the same time" taggedAs(ThreadSafety) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: "null"
method: fold
zero: null
action:
  - log: [{string: start}]
  - a.permutations:
      new: [1, 2, 3, 4, 5, 6, 7, 8]
      type: {type: array, items: int}
  - log: [{string: stop}]
""").head

    val accumulatedLog = mutable.ListBuffer[String]()
    engine.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }

    val thread1 = new Thread(new Runnable {def run(): Unit = engine.action(null)})
    val thread2 = new Thread(new Runnable {def run(): Unit = engine.action(null)})

    thread1.start
    thread2.start
    while (thread1.isAlive  ||  thread2.isAlive)
      Thread.sleep(100)

    accumulatedLog.toSeq should be (Seq(""""start"""", """"stop"""", """"start"""", """"stop""""))
  }

  it must "allow two fold actions from different engines to run at the same time" taggedAs(ThreadSafety) in {
    val engines = PFAEngine.fromYaml("""
input: "null"
output: "null"
method: fold
zero: null
action:
  - log: [{string: start}]
  - a.permutations:
      new: [1, 2, 3, 4, 5, 6, 7, 8]
      type: {type: array, items: int}
  - log: [{string: stop}]
""", multiplicity = 2)

    val accumulatedLog = mutable.ListBuffer[String]()
    engines.head.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }
    engines.last.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }

    val thread1 = new Thread(new Runnable {def run(): Unit = engines.head.action(null)})
    val thread2 = new Thread(new Runnable {def run(): Unit = engines.last.action(null)})

    thread1.start
    thread2.start
    while (thread1.isAlive  ||  thread2.isAlive)
      Thread.sleep(100)

    accumulatedLog.toSeq should be (Seq(""""start"""", """"start"""", """"stop"""", """"stop""""))
  }

  it must "not run an action and a function at the same time" taggedAs(ThreadSafety) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  - log: [{string: start}]
  - a.permutations:
      new: [1, 2, 3, 4, 5, 6, 7, 8]
      type: {type: array, items: int}
  - log: [{string: stop}]
fcns:
  runme:
    params: []
    ret: "null"
    do:
        - log: [{string: fcnstart}]
        - a.permutations:
            new: [1, 2, 3, 4, 5, 6, 7, 8]
            type: {type: array, items: int}
        - log: [{string: fcnstop}]
""").head

    val accumulatedLog = mutable.ListBuffer[String]()
    engine.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }

    val runme = engine.fcn0("runme")
    val thread1 = new Thread(new Runnable {def run(): Unit = engine.action(null)})
    val thread2 = new Thread(new Runnable {def run(): Unit = runme()})

    thread1.start
    thread2.start
    while (thread1.isAlive  ||  thread2.isAlive)
      Thread.sleep(100)

    accumulatedLog.toSeq should be (Seq(""""start"""", """"stop"""", """"fcnstart"""", """"fcnstop""""))
  }

  it must "allow an action and a function from different engines to run at the same time" taggedAs(ThreadSafety) in {
    val engines = PFAEngine.fromYaml("""
input: "null"
output: "null"
action:
  - log: [{string: start}]
  - a.permutations:
      new: [1, 2, 3, 4, 5, 6, 7, 8, 9]
      type: {type: array, items: int}
  - log: [{string: stop}]
fcns:
  runme:
    params: []
    ret: "null"
    do:
        - log: [{string: fcnstart}]
        - a.permutations:
            new: [1, 2, 3, 4, 5, 6, 7, 8, 9]
            type: {type: array, items: int}
        - log: [{string: fcnstop}]
""", multiplicity = 2)

    val accumulatedLog = mutable.ListBuffer[String]()
    engines.head.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }
    engines.last.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }

    val runme = engines.last.fcn0("runme")
    val thread1 = new Thread(new Runnable {def run(): Unit = engines.head.action(null)})
    val thread2 = new Thread(new Runnable {def run(): Unit = runme()})

    thread1.start
    Thread.sleep(10)
    thread2.start
    while (thread1.isAlive  ||  thread2.isAlive)
      Thread.sleep(100)

    accumulatedLog.toSeq should be (Seq(""""start"""", """"fcnstart"""", """"stop"""", """"fcnstop""""))
  }

  it must "not two functions at the same time" taggedAs(ThreadSafety) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: "null"
action: null
fcns:
  runme:
    params: []
    ret: "null"
    do:
        - log: [{string: fcnstart}]
        - a.permutations:
            new: [1, 2, 3, 4, 5, 6, 7, 8]
            type: {type: array, items: int}
        - log: [{string: fcnstop}]
""").head

    val accumulatedLog = mutable.ListBuffer[String]()
    engine.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }

    val runme = engine.fcn0("runme")
    val thread1 = new Thread(new Runnable {def run(): Unit = runme()})
    val thread2 = new Thread(new Runnable {def run(): Unit = runme()})

    thread1.start
    thread2.start
    while (thread1.isAlive  ||  thread2.isAlive)
      Thread.sleep(100)

    accumulatedLog.toSeq should be (Seq(""""fcnstart"""", """"fcnstop"""", """"fcnstart"""", """"fcnstop""""))
  }

  it must "allow two functions from different engines to run at the same time" taggedAs(ThreadSafety) in {
    val engines = PFAEngine.fromYaml("""
input: "null"
output: "null"
action: null
fcns:
  runme:
    params: []
    ret: "null"
    do:
        - log: [{string: fcnstart}]
        - a.permutations:
            new: [1, 2, 3, 4, 5, 6, 7, 8]
            type: {type: array, items: int}
        - log: [{string: fcnstop}]
""", multiplicity = 2)

    val accumulatedLog = mutable.ListBuffer[String]()
    engines.head.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }
    engines.last.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }

    val runme1 = engines.head.fcn0("runme")
    val runme2 = engines.last.fcn0("runme")
    val thread1 = new Thread(new Runnable {def run(): Unit = runme1()})
    val thread2 = new Thread(new Runnable {def run(): Unit = runme2()})

    thread1.start
    thread2.start
    while (thread1.isAlive  ||  thread2.isAlive)
      Thread.sleep(100)

    accumulatedLog.toSeq should be (Seq(""""fcnstart"""", """"fcnstart"""", """"fcnstop"""", """"fcnstop""""))
  }

  it must "not two different functions at the same time" taggedAs(ThreadSafety) in {
    val engine = PFAEngine.fromYaml("""
input: "null"
output: "null"
action: null
fcns:
  runme1:
    params: []
    ret: "null"
    do:
        - log: [{string: fcnstart1}]
        - a.permutations:
            new: [1, 2, 3, 4, 5, 6, 7, 8]
            type: {type: array, items: int}
        - log: [{string: fcnstop1}]
  runme2:
    params: []
    ret: "null"
    do:
        - log: [{string: fcnstart2}]
        - a.permutations:
            new: [1, 2, 3, 4, 5, 6, 7, 8]
            type: {type: array, items: int}
        - log: [{string: fcnstop2}]
""").head

    val accumulatedLog = mutable.ListBuffer[String]()
    engine.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }

    val runme1 = engine.fcn0("runme1")
    val runme2 = engine.fcn0("runme2")
    val thread1 = new Thread(new Runnable {def run(): Unit = runme1()})
    val thread2 = new Thread(new Runnable {def run(): Unit = runme2()})

    thread1.start
    Thread.sleep(100)
    thread2.start
    while (thread1.isAlive  ||  thread2.isAlive)
      Thread.sleep(100)

    accumulatedLog.toSeq should be (Seq(""""fcnstart1"""", """"fcnstop1"""", """"fcnstart2"""", """"fcnstop2""""))
  }

  it must "allow two different functions from different engines to run at the same time" taggedAs(ThreadSafety) in {
    val engines = PFAEngine.fromYaml("""
input: "null"
output: "null"
action: null
fcns:
  runme1:
    params: []
    ret: "null"
    do:
        - log: [{string: fcnstart1}]
        - a.permutations:
            new: [1, 2, 3, 4, 5, 6, 7, 8, 9]
            type: {type: array, items: int}
        - log: [{string: fcnstop1}]
  runme2:
    params: []
    ret: "null"
    do:
        - log: [{string: fcnstart2}]
        - a.permutations:
            new: [1, 2, 3, 4, 5, 6, 7, 8, 9]
            type: {type: array, items: int}
        - log: [{string: fcnstop2}]
""", multiplicity = 2)

    val accumulatedLog = mutable.ListBuffer[String]()
    engines.head.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }
    engines.last.log = (x: String, ns: Option[String]) => accumulatedLog.synchronized { accumulatedLog.append(x) }

    val runme1 = engines.head.fcn0("runme1")
    val runme2 = engines.last.fcn0("runme2")
    val thread1 = new Thread(new Runnable {def run(): Unit = runme1()})
    val thread2 = new Thread(new Runnable {def run(): Unit = runme2()})

    thread1.start
    Thread.sleep(100)
    thread2.start
    while (thread1.isAlive  ||  thread2.isAlive)
      Thread.sleep(100)

    accumulatedLog.toSeq should be (Seq(""""fcnstart1"""", """"fcnstart2"""", """"fcnstop1"""", """"fcnstop2""""))
  }

}
