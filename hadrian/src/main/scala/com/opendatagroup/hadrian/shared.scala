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
  }

  class SharedMapInMemory(initialCapacity: Int = 16, loadFactor: Float = 0.75f, concurrencyLevel: Int = 16) extends SharedMap {
    private val hashMap = new ConcurrentHashMap[String, Ref](initialCapacity, loadFactor, concurrencyLevel)

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
      val newRef = Ref(initialValue, LastUpdate(System.currentTimeMillis))
      var result: Any = initialValue
      newRef.lastUpdate.synchronized {
        val oldRef = hashMap.putIfAbsent(name, newRef)
        if (oldRef != null) {
          oldRef.lastUpdate.synchronized {
            oldRef.to = oldRef.to match {
              case x: PFAArray[_] => x.updated(path, updator, schema)
              case x: PFAMap[_] => x.updated(path, updator, schema)
              case x: PFARecord => x.updated(path, updator, schema)
              case x => updator(x.asInstanceOf[X])
            }
            result = oldRef.to
            oldRef.lastUpdate.at = System.currentTimeMillis
          } // end oldRef.lastUpdate.synchronized
        }
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
