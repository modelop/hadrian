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
  /** Trait for path indexes (array indexes, map keys, and record field names).
    * 
    * Deep extraction, such as `object.field[3]["key"].anotherField["anotherKey"][77]` is presented as a single path, like `"field", 3, "key", "anotherField", "anotherKey", 77` so that a request for a deep subitem does not require the whole `object` to be returned. If the `object` is large and in a remote database, significantly less data can be sent across the network by unpacking the subitem remotely and sending only that back.
    */
  trait PathIndex
  /** Represents an array index.
    */
  case class I(i: Int) extends PathIndex
  /** Represents a map key.
    */
  case class M(k: String) extends PathIndex
  /** Represents a record field name.
    */
  case class R(f: String) extends PathIndex

  private case class LastUpdate(var at: Long)
  private case class Ref(var to: Any, val lastUpdate: LastUpdate)

  /** Represents the state of a collection of PFA cells and pools, usually the ones that are shared across multiple instances.
    */
  class SharedState(val cells: SharedMap, val pools: Map[String, SharedMap])

  /** An abstract class for shared memory whose update granularity is one item in the map.
    * 
    * Make subclasses of this class for sharing data in different circumstances, such as data shared among JVM processes or among compute nodes in a network, using databases, etc.
    * 
    * The `SharedState`/`SharedMap` policy is:
    * 
    *  - `initialize` first, and all other methods block until `initialize` is done.
    *  - `get` never blocks (after `initialize` is done), returning whatever version of an item is available when it is called. (A `put`, `update`, or `remove` might be in progress, but `get` does ''not'' wait for the new version.)
    *  - `put` replaces one item in the map with a prefabricated alternative, blocking if ''that item'' is being modified (`put`, `update`, or `remove`) by another thread. `put` does not block if other items in the map are being modified. If a `path` is provided, the `put` only changes a part of the item, but the whole item blocks.
    *  - `update` replaces one item in the map with an `updator` function, which transforms an old value into a new value. The same blocking rules apply as for `put`, including a `path` for modifying substructures. A prefabricated `initialValue` must be provided in case `name` does not yet exist in the map: the `initialValue` is inserted, rather than evaluating the `updator` function.
    *  - `remove` removes one item from the map, following the same blocking rules as `put`.
    */
  abstract class SharedMap {
    private var initializing = false
    private var initialized = false
    private var broken = false

    // initialize() only takes effect the first time it is called.  All other attempts pass through.
    /** The first time a `SharedMap` is built, it may need to set up many names at once and ensure that all of these names are installed before any further access. This function provides a way to do that.
      * 
      * `initialize` only takes effect the first time it is called. After that, it is strictly pass-through.
      */
    def initialize(names: Set[String], value: String => Any): Unit =
      if (!initializing) {
        this.synchronized {
          initializing = true
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

    /** Methods like `get`, `put`, etc. should wait for `initialize` to finish. Concrete implementations should call `blockUntilInitialized` first to ensure that the `SharedMap` is past the initialization stage.
      */
    def blockUntilInitialized(): Unit = {
      while (!initialized) {
        if (broken) throw new PFAInitializationException("the engine that was initializing the shared state encountered an exception; all other engines must stop, too")
        Thread.sleep(10)
      }
    }

    // get() never blocks and returns a value that was associated to the key at some time or None.
    // If a put or update is in progress, we get the value from before the beginning of the update.
    /** Returns the item with a given `name` at substructure `path` if it exists or an exception if there are any errors.
      * 
      * Blocks during initialization, but never afterward. If a `put`, `update`, or `remove` is in progress, `get` returns the old version of the item; it ''does not wait'' for a new version to be provided.
      * 
      * @param name name of the object to extract (could be a cell or an item of a pool)
      * @param path sequence of array indexes, map keys, and record field names to extract
      * @return a tagged union (Scala `Either`) of Left(exception object) or Right(result at the end of the path)
      */
    def get(name: String, path: Array[PathIndex]): Either[Exception, Any]

    // put() is exclusive and atomic: only one put, update, or remove can happen at a time.
    // put() is a special case of update in which initialValue = value and updator = (x) => value.
    /** Replaces or adds an item with a given `name` at substructure `path`, returning `value` if there are no errors or an exception otherwise.
      * 
      * Blocks during initialization and during any other update (`put`, `update`, or `remove`). Blocking is at the granularity of a particular item: if another item (with another `name`) is being updated, this function ''does not wait.''
      * 
      * `put` is a special case of update in which `initialValue` is `value` and `updator` is a function that ignores the input and returns `value`.
      * 
      * @param name name of the object to replace (could be a cell or an item of a pool)
      * @param path sequence of array indexes, map keys, and record field names to dig down to before replacing
      * @return a tagged union (Scala `Either`) of Left(exception object) or Right(new value at the end of the path)
      */
    def put[X](name: String, path: Array[PathIndex], value: X, initializing: Boolean = false): Either[Exception, X]

    // update() is exclusive and atomic: only one put, update, or remove can happen at a time.
    /** Replaces or adds an item with a given `name` at substructure `path` with an `updator` function (if it exists) or an `initialValue` (if it does not), returning the new value if there are no errors or an exception otherwise.
      * 
      * Blocks during initialization and during any other update (`put`, `update`, or `remove`). Blocking is at the granularity of a particular item: if another item (with another `name`) is being updated, this function ''does not wait.''
      * 
      * @param name name of the object to replace (could be a cell or an item of a pool)
      * @param path sequence of array indexes, map keys, and record field names to dig down to before replacing
      * @param initialValue value to use it the object does not yet exist
      * @param updator function to apply to an existing object to get a new value
      * @return a tagged union (Scala `Either`) of Left(exception object) or Right(new value at the end of the path)
      */
    def update[X](name: String, path: Array[PathIndex], initialValue: X, updator: (X) => X, schema: Schema): Either[Exception, Any]

    // remove() is exclusive and atomic: only one put, update, or remove can happen at a time; returns null.
    /** Removes an item with a given `name`.
      * 
      * Blocks during initialization and during any other update (`put`, `update`, or `remove`). Blocking is at the granularity of a particular item: if another item (with another `name`) is being updated, this function ''does not wait.''
      * 
      * @param name name of the object to replace (could be a cell or an item of a pool)
      * @return old value
      */
    def remove(name: String): AnyRef

    // toMap blocks writing and returns a snapshot of the names and values
    /** Block updates and return a snapshot of the `SharedMap` at a point in time.
      */
    def toMap: Map[String, Any]
  }

  /** Concrete subclass of `SharedMap` that implements sharing within one JVM using a `java.util.concurrent.ConcurrentHashMap`.
    * 
    * Follows the `SharedMap` concurrency rules (see [[com.opendatagroup.hadrian.shared.SharedMap SharedMap]] documentation for details).
    * 
    * @param initialCapacity passed to `ConcurrentHashMap`
    * @param loadFactor passed to `ConcurrentHashMap`
    * @param concurrencyLevel passed to `ConcurrentHashMap`
    */
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
        Thread.sleep(10)

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
        Thread.sleep(10)

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

    override def remove(name: String): AnyRef = {
      blockUntilInitialized()
      while (enumerating)
        Thread.sleep(10)

      val ref = hashMap.get(name)
      if (ref != null)
        ref.lastUpdate.synchronized {
          hashMap.remove(name)
        }

      null
    }
  }
}
