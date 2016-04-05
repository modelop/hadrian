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

package com.opendatagroup.hadrian

import scala.language.postfixOps
import scala.language.existentials

import com.opendatagroup.hadrian.jvmcompiler.PFAEngine

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAMap
import com.opendatagroup.hadrian.data.AnyPFARecord
import com.opendatagroup.hadrian.data.GenericPFARecord
import com.opendatagroup.hadrian.data.PFARecord
import com.opendatagroup.hadrian.data.AnyPFAFixed
import com.opendatagroup.hadrian.data.AnyPFAEnumSymbol

import com.opendatagroup.hadrian.datatype.AvroConversions._
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
import com.opendatagroup.hadrian.datatype.AvroUnion

package memory {
  /** Represents memory usage of an item as a quantity with units and exactness flag.
    * 
    * May be used as an intermediate calculation or a generator of a final report (through its `toString` method).
    * 
    * @param bytes amount of memory used in bytes
    * @param exact if `true`, this calculation is exact because the usage of the structure is precisely known; if `false`, it is an approximation based on polynomial fits to observed usage
    */
  case class Usage(bytes: Long, exact: Boolean = true) {
    /** Represent this memory as a `String`, using a given unit.
      * 
      * @param unit unit of measurement, such as [[com.opendatagroup.hadrian.memory.B B (bytes)]], [[com.opendatagroup.hadrian.memory.KB KB (kilobytes)]], [[com.opendatagroup.hadrian.memory.MB MB (megabytes)]], or [[com.opendatagroup.hadrian.memory.TB TB (terabytes)]]
      * @param formatter format string specifying the number of digits of precision (and whatever else you want)
      * @return formatted usage string that can be integrated into a larger report
      */
    def toString(unit: MemoryUnit, formatter: String = "%.3f") =
      (if (exact) "" else "~") +
      (if (unit == B) bytes.toString else formatter.format(unit(bytes))) +
      " " + unit.toString
    /** Represent this memory as a `String`, using the most convenient units.
      */
    override def toString() = toString(bestUnit(bytes))
    /** Add an exact number of bytes to this `Usage`, propagating the exactness flag.
      * 
      * @param more exact number of bytes
      * @return a new `Usage` object without modifying this one
      */
    def +(more: Long) = new Usage(bytes + more, exact)
    /** Add another `Usage`, propagating the exactness flag (logical-and of this `exact` and that `exact`).
      * 
      * @param that `Usage` to add
      * @return a new `Usage` object without modifying this one
      */
    def +(that: Usage) = new Usage(this.bytes + that.bytes, this.exact  &&  that.exact)
    /** Multiply by an exact number, propagating the exactness flag.
      * 
      * @param times exact number
      * @return a new `Usage` object without modifying this one
      */
    def *(times: Long) = new Usage(bytes * times, exact)
    /** Apply Java object padding to this estimate.
      * 
      * Java object padding is `Math.ceil(bytes / 8.0).toLong * 8L`.
      * 
      * @return a new `Usage` object without modifying this one
      */
    def pad = new Usage(Math.ceil(bytes / 8.0).toLong * 8L, exact)
  }

  /** Represents the memory usage of a cell.
    * 
    * May be used as an intermediate calculation or a generator of a final report (through its `toString` method).
    * 
    * @param name name of the cell
    * @param shared if `true`, this cell is shared (and must not be double-counted for each instance that shares it); if `false`, this cell is not shared
    * @param usage amount of memory used
    */
  case class CellReport(name: String, shared: Boolean, usage: Usage) {
    override def toString() = s"""cell $name: $usage${if (shared) " (shared)" else ""}"""
  }

  /** Represents the memory usage of a pool.
    * 
    * May be used as an intermediate calculation or a generator of a final report (through its `toString` method).
    * 
    * @param name name of the pool
    * @param shared if `true`, this pool is shared (and must not be double-counted for each instance that shares it); if `false`, this pool is not shared
    * @param usage amount of memory used
    * @param number of items in the pool
    */
  case class PoolReport(name: String, shared: Boolean, usage: Usage, items: Int) {
    override def toString() = s"""pool $name: $usage in $items items${if (shared) " (shared)" else ""}"""
  }

  /** Represents the memory usage of a scoring engine's cells and pools (ignoring any double-counting from sharing with other scoring engine instances).
    * 
    * May be used as an intermediate calculation or a generator of a final report (through its `toString` method).
    * 
    * Only includes cells and pools, ignores memory used by class bytecode and instance overhead.
    * 
    * @param engine the scoring engine to analyze
    */
  case class EngineReport(engine: PFAEngine[_ <: AnyRef, _ <: AnyRef]) {
    /** (name, cellReport) pairs for each cell in the scoring engine instance (ignoring any double-counting from cells shared with other scoring engine instances).
      */
    val cells =
      for ((name, conf) <- engine.config.cells) yield {
        val usage = engine.analyzeCell(name, {x => compute(conf.avroType, x)})
        (name, CellReport(name, engine.config.cells(name).shared, usage))
      }

    /** (name, poolReport) pairs for each cell in the scoring engine instance (ignoring any double-counting from pools shared with other scoring engine instances).
      */
    val pools =
      for ((name, conf) <- engine.config.pools) yield {
        val usages = engine.analyzePool(name, {x => compute(conf.avroType, x)})
        (name, PoolReport(name, engine.config.pools(name).shared, usages.values.foldLeft(Usage(0))(_ + _), usages.size))
      }

    /** Total memory usage for the scoring engine instance (ignoring any double-counting from cells or pools shared with other scoring engine instances).
      */
    val total = cells.values.map(_.usage).reduce(_ + _) +
                pools.values.map(_.usage).reduce(_ + _)

    /** Create a formatted report as a `String`.
      */
    override def toString() = toString("")
    /** Create a formatted report as a `String` with a given `indent`.
      */
    def toString(indent: String) =
      (cells.values.toSeq.sortWith((a, b) => if (a.shared != b.shared) a.shared > b.shared else a.name < b.name).map(indent + _.toString) ++
        pools.values.toSeq.sortWith((a, b) => if (a.shared != b.shared) a.shared > b.shared else a.name < b.name).map(indent + _.toString) ++
        Seq(indent + "total: " + total.toString)
      ).mkString("\n")
  }

  /** Represents the memory usage of a collection of scoring engines, properly accounting for cells and pools that are shared among the instances.
    * 
    * May be used as an intermediate calculation or a generator of a final report (through its `toString` method).
    * 
    * Only includes cells and pools, ignores memory used by class bytecode and instance overhead.
    * 
    * @param engines collection of scoring engines to analyze as a group
    */
  case class EnginesReport(engines: Seq[PFAEngine[_ <: AnyRef, _ <: AnyRef]]) {
    if (engines.isEmpty)
      throw new IllegalArgumentException("needs at least one PFAEngine to analyze")
    private val prototype = engines.head
    private val instances = engines.map(_.instance).sorted
    private val engineByInstance = engines.map(x => (x.instance, x)).toMap[Int, PFAEngine[_ <: AnyRef, _ <: AnyRef]]

    /** (name, cellReport) pairs for each shared cell in the group of scoring engines, counting the shared memory only once.
      */
    val sharedCells =
      for ((name, conf) <- prototype.config.cells if (conf.shared)) yield {
        val usage = prototype.analyzeCell(name, {x => compute(conf.avroType, x)})
        (name, CellReport(name, prototype.config.cells(name).shared, usage))
      }

    /** Sorted list of names of unshared cells.
      */
    val unsharedCellNames = (for ((name, conf) <- prototype.config.cells if (!conf.shared)) yield name).toSeq.sorted
    /** (name, cellReport) pairs for each unshared cell in the group of scoring engines, adding the unshared memory from each.
      */
    val unsharedCells =
      (for (instance <- instances) yield
        (instance,
          for ((name, conf) <- prototype.config.cells if (!conf.shared)) yield {
            val usage = engineByInstance(instance).analyzeCell(name, {x => compute(conf.avroType, x)})
            (name, CellReport(name, prototype.config.cells(name).shared, usage))
          })).toMap

    /** (name, poolReport) pairs for each shared pool in the group of scoring engines, counting the shared memory only once.
      */
    val sharedPools =
      for ((name, conf) <- prototype.config.pools if (conf.shared)) yield {
        val usages = prototype.analyzePool(name, {x => compute(conf.avroType, x)})
        (name, PoolReport(name, prototype.config.pools(name).shared, usages.values.foldLeft(Usage(0))(_ + _), usages.size))
      }

    /** Sorted list of names of unshared pools.
      */
    val unsharedPoolNames = (for ((name, conf) <- prototype.config.pools if (!conf.shared)) yield name).toSeq.sorted
    /** (name, poolReport) pairs for each unshared pool in the group of scoring engines, adding the unshared memory from each.
      */
    val unsharedPools =
      (for (instance <- instances) yield
        (instance,
          for ((name, conf) <- prototype.config.pools if (!conf.shared)) yield {
            val usages = engineByInstance(instance).analyzePool(name, {x => compute(conf.avroType, x)})
            (name, PoolReport(name, prototype.config.pools(name).shared, usages.values.foldLeft(Usage(0))(_ + _), usages.size))
          })).toMap

    /** Total memory usage for the collection of scoring engines, properly accounting for cells and pools that are shared among the instances.
      */
    val total = sharedCells.values.map(_.usage).reduce(_ + _) +
                sharedPools.values.map(_.usage).reduce(_ + _) +
                unsharedCells.values.map(x => x.values.map(_.usage).reduce(_ + _)).reduce(_ + _) +
                unsharedPools.values.map(x => x.values.map(_.usage).reduce(_ + _)).reduce(_ + _)

    /** Create a formatted report as a `String`.
      */
    override def toString() = toString("")
    /** Create a formatted report as a `String` with a given `indent`.
      */
    def toString(indent: String) =
      ((sharedCells.values.toSeq.sortBy(_.name).map(indent + _.toString)) ++
       (sharedPools.values.toSeq.sortBy(_.name).map(indent + _.toString)) ++
       (unsharedCellNames flatMap {name: String =>
         instances.map(instance => indent + "instance " + instance.toString + " " + unsharedCells(instance)(name).toString)}) ++
       (unsharedPoolNames flatMap {name: String =>
         instances.map(instance => indent + "instance " + instance.toString + " " + unsharedPools(instance)(name).toString)}) ++
       Seq(indent + "total: " + total.toString)
      ).mkString("\n")
  }
}

package object memory {
  /** Abstract trait for all memory units.
    */
  trait MemoryUnit {
    /** Converts a number of `bytes` into an amount of memory in this unit.
      */
    def apply(bytes: Long): Double
  }

  /** `MemoryUnit` for bytes.
    */
  object B extends MemoryUnit {
    /** Does nothing (`bytes` is already in bytes).
      */
    def apply(bytes: Long) = bytes.toDouble
    /** Unit symbol `"B"`.
      */
    override def toString() = "B"
  }

  /** `MemoryUnit` for kilobytes.
    * 
    * We take the convention that a kilobyte is 1024 bytes.
    */
  object KB extends MemoryUnit {
    /** Converts a number of `bytes` into a number of kilobytes.
      */
    def apply(bytes: Long) = bytes / 1024.0
    /** Unit symbol `"KB"`.
      */
    override def toString() = "KB"
  }

  /** `MemoryUnit` for megabytes.
    * 
    * We take the convention that a megabyte is 1024 kilobytes.
    */
  object MB extends MemoryUnit {
    /** Converts a number of `bytes` into a number of megabytes.
      */
    def apply(bytes: Long) = bytes / 1024.0 / 1024.0
    /** Unit symbol `"MB"`.
      */
    override def toString() = "MB"
  }

  /** `MemoryUnit` for gigabytes.
    * 
    * We take the convention that a gigabyte is 1024 megabytes.
    */
  object GB extends MemoryUnit {
    /** Converts a number of `bytes` into a number of gigabytes.
      */
    def apply(bytes: Long) = bytes / 1024.0 / 1024.0 / 1024.0
    /** Unit symbol `"GB"`.
      */
    override def toString() = "GB"
  }

  /** `MemoryUnit` for terabytes.
    * 
    * We take the convention that a terabyte is 1024 gigabytes.
    */
  object TB extends MemoryUnit {
    /** Converts a number of `bytes` into a number of terabytes.
      */
    def apply(bytes: Long) = bytes / 1024.0 / 1024.0 / 1024.0 / 1024.0
    /** Unit symbol `"TB"`.
      */
    override def toString() = "TB"
  }

  /** Finds the best unit of measure: the largest unit for which the amount is greater than 1.0, if `bytes` is positive.
    */
  def bestUnit(bytes: Long) =
    if      (TB(bytes) > 1.0) TB
    else if (GB(bytes) > 1.0) GB
    else if (MB(bytes) > 1.0) MB
    else if (KB(bytes) > 1.0) KB
    else                      B

  /** Compute the memory usage for a given `avroType` if that usage can be determined without reference to a particular instance.
    * 
    * This method can be much faster than `compute(avroType, instance)`.
    * 
    * The following types can be computed from an `avroType` alone:
    * 
    *  - "null"
    *  - "boolean"
    *  - "int"
    *  - "long"
    *  - "float"
    *  - "double"
    *  - "fixed"
    *  - "enum"
    *  - "record" if all fields can be computed from an `avroType` alone.
    * 
    * The following cannot:
    * 
    *  - "bytes" (requires length of byte array)
    *  - "string" (requires length of string)
    *  - "array" (requires length of array)
    *  - "map" (requires length of map)
    *  - "record" if some fields cannot be computed from an `avroType` alone
    *  - "union" (if the union consists of alternatives that are all the same size, it could be computed from the `avroType` alone, but this case is not handled)
    * 
    * Primitives are assumed to be boxed unless they are found in records, in which case they are unboxed.
    * 
    * @param avroType the type to estimate
    * @return `Some(usage)` if the usage can be calculated without reference to a particular instance; `None` otherwise.
    */
  def compute(avroType: AvroType): Option[Usage] = avroType match {
    case AvroNull() => Some(Usage(0))     // it's cached
    case AvroBoolean() => Some(Usage(0))  // it's cached
    case AvroInt() => Some(Usage(12 + 4).pad)     // assume boxed
    case AvroLong() => Some(Usage(12 + 8).pad)    // assume boxed
    case AvroFloat() => Some(Usage(12 + 4).pad)   // assume boxed
    case AvroDouble() => Some(Usage(12 + 8).pad)  // assume boxed
    case AvroFixed(size, _, _, _, _) => Some(Usage(40 + size).pad)
    case AvroEnum(_, _, _, _, _) => Some(Usage(16))
    case AvroRecord(fields, _, _, _, _) =>
      val fieldUsages = fields map {field => field.avroType match {
        case AvroBoolean() => Some(Usage(1))   // unboxed in records
        case AvroInt() => Some(Usage(4))       // unboxed in records
        case AvroLong() => Some(Usage(8))      // unboxed in records
        case AvroFloat() => Some(Usage(4))     // unboxed in records
        case AvroDouble() => Some(Usage(8))    // unboxed in records
        case t => compute(t).map(_ + 4)   // the object and a pointer to it
      }}

      if (fieldUsages exists {_ == None})
        None
      else
        Some((fieldUsages.flatten.reduce(_ + _) + 12).pad)

    case AvroBytes() => None
    case AvroString() => None
    case AvroArray(_) => None
    case AvroMap(_) => None
    case AvroUnion(_) => None
  }

  /** Compute the memory usage for an `instance` of a given `avroType`.
    * 
    * This method can be much slower than `compute(avroType)`.
    * 
    * This function will first attempt to `compute(avroType)` (without `instance`) and only revert to a general computation if needed.
    * 
    * Memory usage for the following types are computed exactly:
    * 
    *  - any that can be computed from `avroType` alone
    *  - "bytes"
    *  - "string"
    *  - "record" if all fields can be computed exactly
    *  - "union" if this `instance` can be computed exactly
    * 
    * Memory usage for the following types are estimated with polynomials:
    * 
    *  - "array" (which is a Scala `Vector`, ultimately a tree structure whose balancing depends on a complex algorithm)
    *  - "map" (which is a Scala `Map`, ultimately a tree structure whose balancing depends on a complex algorithm)
    *  - "record" if any field is approximate
    *  - "union" if this `instance` is approximate
    * 
    * Primitives are assumed to be boxed unless they are found in records, in which case they are unboxed.
    * 
    * @param avroType the type of the `instance` to estimate
    * @param instance the particular instance to estimate
    * @return a `Usage` object (no `Option`)
    */
  def compute(avroType: AvroType, instance: Any): Usage = compute(avroType) match {
    case Some(x) => x

    case None => (avroType, instance) match {
      case (AvroBytes(), x: Array[_]) => Usage(16 + x.size).pad
      case (AvroString(), x: String) => Usage(36 + x.size * 2).pad
      case (AvroArray(items), x: PFAArray[_]) =>
        val size = x.toVector.size
        val itemsSize = compute(items).map(_ * size).orElse(Some(x.toVector map {x => compute(items, x)} reduce(_ + _))).get
        if (size == 0)       // 24 for the PFAArray wrapper; the Vector scales with the number of 32-item blocks
          Usage(24)
        else if (size <= 32)
          Usage(24 + 200) + itemsSize
        else if (size <= 64)
          Usage(24 + 488) + itemsSize
        else if (size <= 96)
          Usage(24 + 632) + itemsSize
        else if (size <= 128)
          Usage(24 + 776) + itemsSize
        else if (size <= 160)
          Usage(24 + 920) + itemsSize
        else if (size <= 192)
          Usage(24 + 1064) + itemsSize
        else if (size <= 224)
          Usage(24 + 1208) + itemsSize
        else if (size <= 256)
          Usage(24 + 1352) + itemsSize
        else
          Usage(24 + Math.round(141.1 + 4.68*size), exact = false) + itemsSize

      case (AvroMap(values), x: PFAMap[_]) =>
        val size = x.toMap.size
        val valuesSize = compute(values).map(_ * size).orElse(Some(x.toMap map {case (k, v) => compute(values, v)} reduce(_ + _))).get
        val stringSize = x.toMap map {case (k, v) => Usage(32 + 2*k.size).pad.bytes} sum

        if (size == 0)       // 24 for the PFAMap wrapper; the Map scales with the lengths of strings and number of items
          Usage(24)          // size 1, 2, 3, and 4 are special cases
        else if (size == 1)
          Usage(24 + 28 + stringSize).pad + valuesSize
        else if (size == 2)
          Usage(24 + 48 + stringSize).pad + valuesSize
        else if (size == 3)
          Usage(24 + 58 + stringSize).pad + valuesSize
        else if (size == 4)
          Usage(24 + 74 + stringSize).pad + valuesSize
        else
          Usage(24 + Math.round(610.0 + 76.54*size) + stringSize, exact = false).pad + valuesSize

      case (AvroRecord(fields, _, _, _, _), x: AnyPFARecord) =>
        val fieldUsages = fields map {field => field.avroType match {
          case AvroBoolean() => Usage(1)   // unboxed in records
          case AvroInt() => Usage(4)       // unboxed in records
          case AvroLong() => Usage(8)      // unboxed in records
          case AvroFloat() => Usage(4)     // unboxed in records
          case AvroDouble() => Usage(8)    // unboxed in records
          case AvroBytes() => Usage(20 + x.get(field.name).asInstanceOf[Array[_]].size)
          case AvroString() =>
            val len = x.get(field.name).asInstanceOf[String].size
            if (len == 0)
              Usage(4)
            else
              Usage(50 - 2*((len - 1) % 4) + 2*len)
          case t =>
            val subusage = compute(t) match {
              case Some(y) => y
              case None => compute(t, x.get(field.name))
            }
            subusage + 4   // the object and a pointer to it
        }}

        (fieldUsages.reduce(_ + _) + 12).pad

      case (AvroUnion(_), null) => compute(AvroNull()).get
      case (AvroUnion(_), x: java.lang.Boolean) => compute(AvroBoolean()).get
      case (AvroUnion(_), x: java.lang.Integer) => compute(AvroInt()).get
      case (AvroUnion(_), x: java.lang.Long) => compute(AvroLong()).get
      case (AvroUnion(_), x: java.lang.Float) => compute(AvroFloat()).get
      case (AvroUnion(_), x: java.lang.Double) => compute(AvroDouble()).get
      case (AvroUnion(_), x: AnyPFAFixed) => Usage(40 + x.bytes.size).pad
      case (AvroUnion(_), x: AnyPFAEnumSymbol) => Usage(16)
      case (AvroUnion(_), x: GenericPFARecord) =>
        val avroType = schemaToAvroType(x.getSchema)
        compute(avroType).orElse(Some(compute(avroType, x))).get
      case (AvroUnion(_), x: PFARecord) =>
        val avroType = schemaToAvroType(x.getSchema)
        compute(avroType).orElse(Some(compute(avroType, x))).get
      case (AvroUnion(_), x: Array[_]) => compute(AvroBytes(), x)
      case (AvroUnion(_), x: String) => compute(AvroString(), x)
      case (AvroUnion(types), x: PFAArray[_]) =>
        val avroType = types.find(_.isInstanceOf[AvroArray]).get.asInstanceOf[AvroArray].items
        compute(avroType).orElse(Some(compute(avroType, x))).get
      case (AvroUnion(types), x: PFAMap[_]) =>
        val avroType = types.find(_.isInstanceOf[AvroMap]).get.asInstanceOf[AvroMap].values
        compute(avroType).orElse(Some(compute(avroType, x))).get
    }
  }
}
