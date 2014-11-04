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

package com.opendatagroup.hadrian

import java.util.concurrent.ConcurrentHashMap

import scala.collection.JavaConversions._

import org.apache.avro.Schema

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAMap
import com.opendatagroup.hadrian.data.PFARecord
import com.opendatagroup.hadrian.errors.PFAInitializationException
import com.opendatagroup.hadrian.datatype.AvroType

package shared {
  trait PathIndex
  case class I(i: Int) extends PathIndex
  case class M(k: String) extends PathIndex
  case class R(f: String) extends PathIndex

  private case class LastUpdate(var at: Long)
  private case class Ref(var to: Any, val lastUpdate: LastUpdate)

  class SharedState(val cells: SharedMap, val pools: Map[String, SharedMap])

  abstract class SharedMap {
    private var initializing = false
    private var initialized = false
    private var broken = false

    // initialize() only takes effect the first time it is called.  All other attempts pass through.
    def initialize(names: Set[String], value: String => Any): Unit =
      if (!initializing) {
        this.synchronized {
          initializing = true
          val now = System.currentTimeMillis
          for (name <- names) {
            val v = try {
              value(name)
            }
            catch {
              case err: Exception => broken = true;  throw err
            }
            put(name, Array[PathIndex](), v, initializing = true)
          }
          initialized = true
        }
      }

    def blockUntilInitialized(): Unit = {
      while (!initialized) {
        if (broken) throw new PFAInitializationException("the engine that was initializing the shared state encountered an exception; all other engines must stop, too")
        Thread.sleep(100)
      }
    }

    // get() never blocks and returns a value that was associated to the key at some time or None.
    // If a put or update is in progress, we get the value from before the beginning of the update.
    def get(name: String, path: Array[PathIndex]): Either[Exception, Any]

    // put() is exclusive and atomic: only one put or update can happen at a time.
    // put() is a special case of update in which initialValue = value and updator = (x) => value.
    def put[X](name: String, path: Array[PathIndex], value: X, initializing: Boolean = false): Either[Exception, X]

    // update() is exclusive and atomic: only one put or update can happen at a time.
    def update[X](name: String, path: Array[PathIndex], initialValue: X, updator: (X) => X, schema: Schema): Either[Exception, Any]

    // remove() is atomic and more than one remove has the same effect as one remove.
    def remove(name: String, path: Array[PathIndex]): Either[Exception, Any]

    // toMap blocks writing and returns a snapshot of the names and values
    def toMap: Map[String, Any]
  }

  class SharedMapInMemory(initialCapacity: Int = 16, loadFactor: Float = 0.75f, concurrencyLevel: Int = 16) extends SharedMap {
    private val hashMap = new ConcurrentHashMap[String, Ref](initialCapacity, loadFactor, concurrencyLevel)
    private var enumerating = false  // not a lock because it should be asymmetric: toMap sets it, put/update respond to it

    override def toMap: Map[String, Any] = {
      blockUntilInitialized()
      enumerating = true
      val out =
        (for (entry <- hashMap.entrySet) yield {
          val key = entry.getKey
          val ref = entry.getValue
          val value = ref.lastUpdate.synchronized {ref.to}
          (key, value)
        }).toMap
      enumerating = false
      out
    }

    override def get(name: String, path: Array[PathIndex]): Either[Exception, Any] = try {
      blockUntilInitialized()
      hashMap.get(name) match {
        case null => Left(new java.util.NoSuchElementException("no pool item named \"%s\"".format(name)))
        case ref => {
          var out = ref.to
          for (item <- path) (out, item) match {
            case (x: PFAArray[_], I(y)) => out = x(y)
            case (x: PFAMap[_], M(y)) => out = x(y)
            case (x: PFARecord, R(y)) => out = x(y)
          }
          Right(out)
        }
      }
    }
    catch {
      case err: Exception => Left(err)
    }

    override def put[X](name: String, path: Array[PathIndex], value: X, initializing: Boolean = false): Either[Exception, X] = try {
      if (!initializing)
        blockUntilInitialized()
      while (enumerating)
        Thread.sleep(100)

      val newRef = Ref(value, LastUpdate(System.currentTimeMillis))
      newRef.lastUpdate.synchronized {
        val oldRef = hashMap.putIfAbsent(name, newRef)
        if (oldRef != null) {
          oldRef.lastUpdate.synchronized {
            oldRef.to = value
            oldRef.lastUpdate.at = System.currentTimeMillis
          } // end oldRef.lastUpdate.synchronized
        }
      } // end newRef.lastUpdate.synchronized
      Right(value)
    }
    catch {
      case err: Exception => Left(err)
    }

    override def update[X](name: String, path: Array[PathIndex], initialValue: X, updator: (X) => X, schema: Schema): Either[Exception, Any] = try {
      blockUntilInitialized()
      while (enumerating)
        Thread.sleep(100)

      val newRef = Ref(initialValue, LastUpdate(System.currentTimeMillis))
      var result: Any = initialValue
      newRef.lastUpdate.synchronized {
        var ref = hashMap.putIfAbsent(name, newRef)

        if (ref == null)
          ref = newRef

        ref.lastUpdate.synchronized {
          if (path.isEmpty)
            ref.to = updator(ref.to.asInstanceOf[X])
          else
            ref.to = ref.to match {
              case x: PFAArray[_] => x.updated(path, updator, schema)
              case x: PFAMap[_] => x.updated(path, updator, schema)
              case x: PFARecord => x.updated(path, updator, schema)
              case x => updator(x.asInstanceOf[X])
            }
          result = ref.to
          ref.lastUpdate.at = System.currentTimeMillis
        } // end ref.lastUpdate.synchronized

      } // end newRef.lastUpdate.synchronized
      Right(result)
    }
    catch {
      case err: Exception => Left(err)
    }

    override def remove(name: String, path: Array[PathIndex]): Either[Exception, Any] = try {
      blockUntilInitialized()
      Right(hashMap.remove(name).to)
    }
    catch {
      case err: Exception => Left(err)
    }
  }
}
