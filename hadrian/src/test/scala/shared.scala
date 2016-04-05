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

package test.scala.shared

import org.junit.runner.RunWith

import org.scalatest.FlatSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.Matchers

import org.apache.avro.Schema

import com.opendatagroup.hadrian.shared._
import com.opendatagroup.hadrian.datatype._
import test.scala._

@RunWith(classOf[JUnitRunner])
class SharedSuite extends FlatSpec with Matchers {
  "Shared state" must "read and write values in a single thread" taggedAs(Shared) in {
    val state = new SharedMapInMemory()
    state.initialize(Set[String](), (x: String) => null)

    state.put("one", Array[PathIndex](), 1)
    state.put("two", Array[PathIndex](), 2.2)
    state.put("three", Array[PathIndex](), "THREE")
    state.put("three", Array[PathIndex](), "Three")
    state.update("four", Array[PathIndex](), 0, (x: Int) => x + 1, Schema.create(Schema.Type.INT))
    state.update("four", Array[PathIndex](), 0, (x: Int) => x + 1, Schema.create(Schema.Type.INT))
    state.update("four", Array[PathIndex](), 0, (x: Int) => x + 1, Schema.create(Schema.Type.INT))
    state.update("five", Array[PathIndex](), List[Int](), (x: List[Int]) => 5 :: x, Schema.create(Schema.Type.INT))
    state.update("five", Array[PathIndex](), List[Int](), (x: List[Int]) => 5 :: x, Schema.create(Schema.Type.INT))
    state.update("five", Array[PathIndex](), List[Int](), (x: List[Int]) => 5 :: x, Schema.create(Schema.Type.INT))

    state.get("one", Array[PathIndex]()) should be (Right(1))
    state.get("two", Array[PathIndex]()) should be (Right(2.2))
    state.get("three", Array[PathIndex]()) should be (Right("Three"))
    state.get("four", Array[PathIndex]()) should be (Right(3))
    state.get("five", Array[PathIndex]()) should be (Right(List(5, 5, 5)))
    state.get("six", Array[PathIndex]()).isLeft should be (true)

    state.remove("one")
    state.get("one", Array[PathIndex]()).isLeft should be (true)
    state.remove("one")
    state.get("one", Array[PathIndex]()).isLeft should be (true)
  }

  it must "store null differently from no-entry" taggedAs(Shared) in {
    val state = new SharedMapInMemory()
    state.initialize(Set[String](), (x: String) => null)

    state.get("x", Array[PathIndex]()).isLeft should be (true)
    state.put("x", Array[PathIndex](), null)
    state.get("x", Array[PathIndex]()) should be (Right(null))
  }

  it must "read multiple times during write" taggedAs(Shared, MultiThreaded) in {
    val state = new SharedMapInMemory()
    state.initialize(Set[String](), (x: String) => null)

    state.put("x", Array[PathIndex](), 5)

    val reader = new Thread(new Runnable {
      def run(): Unit = {
        for (i <- 0 until 5) {
          val value = state.get("x", Array[PathIndex]())
          if (i < 2)
            value should be (Right(5))
          else if (i < 4)
            value should be (Right(99))
          else
            value.isLeft should be (true)
          Thread.sleep(100)
        }
      }
    })
    reader.start

    state.update("x", Array[PathIndex](), 5, {(x: Int) => Thread.sleep(150); 99}, Schema.create(Schema.Type.INT))
    Thread.sleep(200)
    state.remove("x")
    Thread.sleep(150)
  }

  it must "write atomically" taggedAs(Shared, MultiThreaded) in {
    val state = new SharedMapInMemory()
    state.initialize(Set[String](), (x: String) => null)

    class Increment extends Runnable {
      def run(): Unit =
        state.update("x", Array[PathIndex](), 0, {(x: Int) => Thread.sleep(100); x + 1}, Schema.create(Schema.Type.INT))
    }

    val thread1 = new Thread(new Increment)
    val thread2 = new Thread(new Increment)
    val thread3 = new Thread(new Increment)
    val thread4 = new Thread(new Increment)
    val thread5 = new Thread(new Increment)

    thread1.start
    Thread.sleep(10)
    thread2.start
    Thread.sleep(10)
    thread3.start
    Thread.sleep(10)
    thread4.start
    Thread.sleep(10)
    thread5.start
    Thread.sleep(550)

    state.get("x", Array[PathIndex]()) should be (Right(5))  // and not, for instance, Right(1) because the Incrementers overwrote each other
  }

}
