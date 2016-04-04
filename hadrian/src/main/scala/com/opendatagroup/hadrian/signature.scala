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

import scala.collection.mutable
import scala.language.postfixOps

import com.opendatagroup.hadrian.datatype.Type
import com.opendatagroup.hadrian.datatype.FcnType
import com.opendatagroup.hadrian.datatype.ExceptionType
import com.opendatagroup.hadrian.datatype.AvroType
import com.opendatagroup.hadrian.datatype.AvroCompiled
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
import com.opendatagroup.hadrian.datatype.AvroField
import com.opendatagroup.hadrian.datatype.AvroUnion

package signature {
  /** An internal `Exception` raised when signature matching needs to give up and return `false`.
    * 
    * This is an internal `Exception` and should always be caught, not propagated to the user.
    */
  class IncompatibleTypes(message: String) extends Exception(message)

  /** Describes a PFA (the language) version number as a triple of major number, minor number, release number (all non-negative integers).
    * 
    * The version has a strict ordering (numerical lexicographic).
    * 
    * Used to specify a version in which to interpret a PFA document and to label the beginning and end of support of function signatures.
    * 
    * It could, in the future, be used to label support ranges of special forms, too, but that would always be managed by hand within the special form's constructors.
    */
  case class PFAVersion(major: Int, minor: Int, release: Int) extends Ordered[PFAVersion] {
    if (major < 0  ||  minor < 0  ||  release < 0)
      throw new IllegalArgumentException("PFA version major, minor, and release numbers must be non-negative")

    override def toString() = s"$major.$minor.$release"
    def compare(that: PFAVersion) = (this, that) match {
      case (PFAVersion(a, b, c), PFAVersion(x, y, z)) if (a == x  &&  b == y) => c - z
      case (PFAVersion(a, b, c), PFAVersion(x, y, z)) if (a == x) => b - y
      case (PFAVersion(a, b, c), PFAVersion(x, y, z)) => a - x
    }
  }
  object PFAVersion {
    def fromString(x: String) = try {
      val Array(major, minor, release) = x.split("\\.").map(_.toInt)
      PFAVersion(major, minor, release)
    }
    catch {
      case _: scala.MatchError => throw new IllegalArgumentException("PFA version numbers must have major.minor.release structure")
      case _: java.lang.NumberFormatException => throw new IllegalArgumentException("PFA version major, minor, and release numbers must be integers")
    }
  }

  /** Describes the range of support of a function signature (or, in the future, special form) in terms of an optional beginning of life (birth), and optional deprecation and end of life (death).
    * 
    * If a deprecation is specified, a death must be as well, and vice-versa. Whether or not a birth is specified is independent.
    * 
    * At a given [[com.opendatagroup.hadrian.signature.PFAVersion PFAVersion PFAVersion]], the Lifespan has three possible states: current (method `current` returns `true`), deprecated (method `deprecated` returns `true`), and non-existent (both `current` and `deprecated` return `false`). Method `current` and `deprecated` are mutually exclusive; for a given [[com.opendatagroup.hadrian.signature.PFAVersion PFAVersion PFAVersion]], they would never both return `true`.
    */
  case class Lifespan(birth: Option[PFAVersion], deprecation: Option[PFAVersion], death: Option[PFAVersion], contingency: Option[String]) {
    (deprecation, death) match {
      case (None, None) =>
      case (Some(dep), Some(dth)) if (dep < dth) =>
      case _ => throw new IllegalArgumentException("deprecation and death must be specified together, and deprecation version must be strictly earlier than death version")
    }

    (birth, deprecation) match {
      case (Some(b), Some(d)) =>
        if (b >= d) throw new IllegalArgumentException("if birth and deprecation are specified together, birth version must be strictly earlier than deprecation version")
      case _ =>
    }

    /** @param now the version number to query
      * @return `true` if the feature exists and is not deprecated in version `now`, `false` otherwise.
     */
    def current(now: PFAVersion) =
      (birth == None  ||  now >= birth.get)  &&  (deprecation == None  ||  now < deprecation.get)

    /** @param now the version number to query
      * @return `true` if the feature exists and is deprecated in version `now`, `false` otherwise.
     */
    def deprecated(now: PFAVersion) =
      (deprecation != None  &&  deprecation.get <= now  &&  now < death.get)
  }

  /** Trait for a type pattern.
    * A type pattern is something that a [[com.opendatagroup.hadrian.datatype.AvroType AvroType AvroType]] is matched against when determining if a PFA function signature can be applied to a given set of arguments.
    * 
    * It could be as simple as the type itself (e.g. `P.Int` matches `AvroInt()`) or it could be a complex wildcard.
    */
  trait Pattern
  object P {
    /** Matches [[com.opendatagroup.hadrian.datatype.AvroNull AvroNull AvroNull]].
      */
    case object Null extends Pattern
    /** Matches [[com.opendatagroup.hadrian.datatype.AvroBoolean AvroBoolean AvroBoolean]].
      */
    case object Boolean extends Pattern
    /** Matches [[com.opendatagroup.hadrian.datatype.AvroInt AvroInt AvroInt]].
      */
    case object Int extends Pattern
    /** Matches [[com.opendatagroup.hadrian.datatype.AvroLong AvroLong AvroLong]].
      */
    case object Long extends Pattern
    /** Matches [[com.opendatagroup.hadrian.datatype.AvroFloat AvroFloat AvroFloat]].
      */
    case object Float extends Pattern
    /** Matches [[com.opendatagroup.hadrian.datatype.AvroDouble AvroDouble AvroDouble]].
      */
    case object Double extends Pattern
    /** Matches [[com.opendatagroup.hadrian.datatype.AvroBytes AvroBytes AvroBytes]].
      */
    case object Bytes extends Pattern
    /** Matches [[com.opendatagroup.hadrian.datatype.AvroString AvroString AvroString]].
      */
    case object String extends Pattern

    /** Matches [[com.opendatagroup.hadrian.datatype.AvroArray AvroArray AvroArray]] with a given `items` pattern.
      * 
      * @param items the item pattern to match
      */
    case class Array(items: Pattern) extends Pattern
    /** Matches [[com.opendatagroup.hadrian.datatype.AvroMap AvroMap AvroMap]] with a given `values` pattern.
      * 
      * @param values the values pattern to match
      */
    case class Map(values: Pattern) extends Pattern
    /** Matches [[com.opendatagroup.hadrian.datatype.AvroUnion AvroUnion AvroUnion]] with a given set of sub-patterns.
      * 
      * @param types patterns for the subtypes
      */
    case class Union(types: List[Pattern]) extends Pattern

    /** Matches [[com.opendatagroup.hadrian.datatype.AvroFixed AvroFixed AvroFixed]] with a given size and an optional name.
      * 
      * @param size width of the fixed-width byte array
      * @param fullName optional name for the fixed pattern (if not provided, fixed types of any name would match)
      */
    case class Fixed(size: Int, fullName: Option[String] = None) extends Pattern
    /** Matches [[com.opendatagroup.hadrian.datatype.AvroEnum AvroEnum AvroEnum]] with given symbols and an optional name.
      * 
      * @param symbols names of the enum symbols
      * @param fullName optional name for the fixed pattern (if not provided, fixed types of any name would match)
      */
    case class Enum(symbols: List[String], fullName: Option[String] = None) extends Pattern
    /** Matches [[com.opendatagroup.hadrian.datatype.AvroRecord AvroRecord AvroRecord]] with given fields and an optional name.
      * 
      * @param fields patterns for the record fields
      * @param fullName optional name for the fixed pattern (if not provided, fixed types of any name would match)
      */
    case class Record(fields: scala.collection.immutable.Map[String, Pattern], fullName: Option[String] = None) extends Pattern

    /** Matches [[com.opendatagroup.hadrian.datatype.FcnType FcnType FcnType]] with a given sequence of parameter patterns and return pattern.
      * 
      * @param params patterns for each of the slots in the function-argument's signature (must have only one signature with no wildcards)
      * @param ret pattern for the return type of the function-argument
      */
    case class Fcn(params: List[Pattern], ret: Pattern) extends Pattern

    /** Matches any [[com.opendatagroup.hadrian.datatype.AvroType AvroType AvroType]] or one of a restricted set.
      * 
      * Label letters are shared across a signature (e.g. if two wildcards are both labeled "A", then they both have to resolve to the same type).
      * 
      * @param label label letter (usually one character long, but in principle an arbitrary string)
      * @param oneOf allowed types or `None` for unrestricted
      */
    case class Wildcard(label: String, oneOf: Set[Type] = Set[Type]()) extends Pattern
    /** Matches a [[com.opendatagroup.hadrian.datatype.AvroRecord AvroRecord AvroRecord]] with '''at least''' the requested set of fields.
      * 
      * Label letters are shared across a signature (e.g. if two wildcards are both labeled "A", then they both have to resolve to the same type).
      * 
      * @param label label letter (usually one character long, but in principle an arbitrary string)
      * @param minimalFields fields that a matching record must have and patterns for their respective types
      */
    case class WildRecord(label: String, minimalFields: scala.collection.immutable.Map[String, Pattern]) extends Pattern
    /** Matches a [[com.opendatagroup.hadrian.datatype.AvroEnum AvroEnum AvroEnum]] without any constraint on the symbol names.
      * 
      * Label letters are shared across a signature (e.g. if two wildcards are both labeled "A", then they both have to resolve to the same type).
      * 
      * @param label label letter (usually one character long, but in principle an arbitrary string)
      */
    case class WildEnum(label: String) extends Pattern
    /** Matches a [[com.opendatagroup.hadrian.datatype.AvroFixed AvroFixed AvroFixed]] without any constraint on the size.
      * 
      * Label letters are shared across a signature (e.g. if two wildcards are both labeled "A", then they both have to resolve to the same type).
      * 
      * @param label label letter (usually one character long, but in principle an arbitrary string)
      */
    case class WildFixed(label: String) extends Pattern
    /** Matches a [[com.opendatagroup.hadrian.datatype.AvroEnum AvroEnum AvroEnum]] whose symbols match the fields of a given record
      * 
      * Label letters asre shared across a signature (e.g. if two wildcards are both labeled "A", then they both have to resolve to the same type).
      * 
      * @param label label letter (usually one character long, but in principle an arbitrary string)
      * @param wildRecord label letter of the record (also a wildcard)
      */
    case class EnumFields(label: String, wildRecord: String) extends Pattern

    /** Convert a patternt to a type, if possible (wildcards can't be converted to types).
      * 
      * @param pat pattern to convert
      * @return corresponding type ([[com.opendatagroup.hadrian.datatype.Type Type]] rather than [[com.opendatagroup.hadrian.datatype.AvroType AvroType]] to allow for [[com.opendatagroup.hadrian.datatype.FcnType FcnType]])
      */
    def toType(pat: Pattern): Type = pat match {
      case Null => AvroNull()
      case Boolean => AvroBoolean()
      case Int => AvroInt()
      case Long => AvroLong()
      case Float => AvroFloat()
      case Double => AvroDouble()
      case Bytes => AvroBytes()
      case String => AvroString()

      case Array(items) => AvroArray(toType(items).asInstanceOf[AvroType])
      case Map(values) => AvroMap(toType(values).asInstanceOf[AvroType])
      case Union(types) => AvroUnion(types.map(toType(_).asInstanceOf[AvroType]))

      case Fixed(size, Some(fullName)) => AvroFixed(size, fullName.split("\\.").last, (fullName.split("\\.").init.mkString(".") match { case "" => None;  case x => Some(x) }))
      case Fixed(size, None) => AvroFixed(size)

      case Enum(symbols, Some(fullName)) => AvroEnum(symbols, fullName.split("\\.").last, (fullName.split("\\.").init.mkString(".") match { case "" => None;  case x => Some(x) }))
      case Enum(symbols, None) => AvroEnum(symbols)

      case Record(fields, Some(fullName)) => AvroRecord(fields map {case (k, t) => AvroField(k, mustBeAvro(toType(t)))} toSeq, fullName.split("\\.").last, (fullName.split("\\.").init.mkString(".") match { case "" => None;  case x => Some(x) }))
      case Record(fields, None) => AvroRecord(fields map {case (k, t) => AvroField(k, mustBeAvro(toType(t)))} toSeq)

      case Fcn(params, ret) => FcnType(params.map(toType), toType(ret).asInstanceOf[AvroType])

      case _ => throw new IncompatibleTypes("cannot convert generic pattern to concrete type")
    }

    /** Convert a type to a pattern.
      * 
      * @param t type to convert
      * @return corresponding pattern
      */
    def fromType(t: Type, memo: Set[String] = Set[String]()): Pattern = t match {
      case AvroNull() => Null
      case AvroBoolean() => Boolean
      case AvroInt() => Int
      case AvroLong() => Long
      case AvroFloat() => Float
      case AvroDouble() => Double
      case AvroBytes() => Bytes
      case AvroString() => String

      case AvroArray(items) => Array(fromType(items, memo))
      case AvroMap(values) => Map(fromType(values, memo))
      case AvroUnion(types) => Union(types map {x => fromType(x, memo)} toList)

      case AvroFixed(size, name, Some(namespace), _, _) => Fixed(size, Some(namespace + "." + name))
      case AvroFixed(size, name, None, _, _) => Fixed(size, Some(name))

      case AvroEnum(symbols, name, Some(namespace), _, _) => Enum(symbols, Some(namespace + "." + name))
      case AvroEnum(symbols, name, None, _, _) => Enum(symbols, Some(name))

      case AvroRecord(fields, name, Some(namespace), _, _) => fillRecord(fields, namespace + "." + name, memo)
      case AvroRecord(fields, name, None, _, _) => fillRecord(fields, name, memo)

      case FcnType(params, ret) => Fcn(params map {x => fromType(x, memo)} toList, fromType(ret, memo))
      case _: ExceptionType => throw new IncompatibleTypes("exception type cannot be used in argument patterns")
    }

    private def fillRecord(fields: Seq[AvroField], name: String, memo: Set[String]): Record =
      if (memo.contains(name))
        Record(scala.collection.immutable.Map[String, Pattern](), Some(name))
      else
        Record(fields map {f: AvroField => (f.name, fromType(f.avroType, memo union Set(name)))} toMap, Some(name))

    /** Raise [[com.opendatagroup.hadrian.signature.IncompatibleTypes IncompatibleTypes]] if a given [[com.opendatagroup.hadrian.datatype.Type Type]] is not a [[com.opendatagroup.hadrian.datatype.AvroType AvroType]]; otherwise, pass through.
      * 
      * @param t type to check
      * @return the input `t` or raise an exception
      */
    def mustBeAvro(t: Type): AvroType = t match {
      case x: AvroType => x
      case x => throw new IncompatibleTypes(x.toString + " is not an Avro type")
    }
  }

  object LabelData {
    private def appendTypes(candidates: List[Type], in: List[Type]): List[Type] = {
      var out = in
      for (candidate <- candidates) candidate match {
        case AvroUnion(types) =>
          out = appendTypes(types.toList, out)
        case x =>
          if (out exists {y => y.accepts(x)}) { }
          else {
            out indexWhere {y => x.accepts(y)} match {
              case -1 => out = x :: out
              case i => out = out.updated(i, x)
            }
          }
      }
      out
    }

    def distinctTypes(candidates: List[Type]): List[Type] = {
      var out: List[Type] = Nil
      out = appendTypes(candidates, out)
      out.reverse
    }

    /** Compute the narrowest possible supertype of a set of types.
      * 
      * @param candidates set of types for which to find the narrowest possible supertype
      * @return narrowest possible supertype, usually a union of the candidates
      */
    def broadestType(candidates: List[Type]): Type = {
      val realCandidates = candidates filter {!_.isInstanceOf[ExceptionType]}

      if (candidates.isEmpty)
        throw new IncompatibleTypes("empty list of types")
      else if (realCandidates.isEmpty)
        throw new IncompatibleTypes("list of types consists only of exception type")

      else if (realCandidates forall {case _: AvroNull => true; case _ => false})
        realCandidates.head
      else if (realCandidates forall {case _: AvroBoolean => true; case _ => false})
        realCandidates.head

      else if (realCandidates forall {case _: AvroInt => true; case _ => false})
        realCandidates.head
      else if (realCandidates forall {case _: AvroInt | _: AvroLong => true; case _ => false})
        realCandidates collectFirst {case x: AvroLong => x} get
      else if (realCandidates forall {case _: AvroInt | _: AvroLong | _: AvroFloat => true; case _ => false})
        realCandidates collectFirst {case x: AvroFloat => x} get
      else if (realCandidates forall {case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => true; case _ => false})
        realCandidates collectFirst {case x: AvroDouble => x} get

      else if (realCandidates forall {case _: AvroBytes => true; case _ => false})
        realCandidates.head
      else if (realCandidates forall {case _: AvroString => true; case _ => false})
        realCandidates.head

      else if (realCandidates forall {case _: AvroArray => true; case _ => false})
        AvroArray(P.mustBeAvro(broadestType(realCandidates map {case AvroArray(items) => items})))

      else if (realCandidates forall {case _: AvroMap => true; case _ => false})
        AvroMap(P.mustBeAvro(broadestType(realCandidates map {case AvroMap(values) => values})))

      else if (realCandidates forall {case _: AvroUnion => true; case _ => false})
        AvroUnion(distinctTypes(realCandidates flatMap {case AvroUnion(types) => types}).map(P.mustBeAvro))

      else if (realCandidates forall {case _: AvroFixed => true; case _ => false}) {
        val fullName = realCandidates.head.asInstanceOf[AvroFixed].fullName
        if (realCandidates.tail forall {case x: AvroFixed => x.fullName == fullName})
          realCandidates.head
        else
          throw new IncompatibleTypes("incompatible fixed types: " + realCandidates.mkString(" "))
      }

      else if (realCandidates forall {case _: AvroEnum => true; case _ => false}) {
        val fullName = realCandidates.head.asInstanceOf[AvroEnum].fullName
        if (realCandidates.tail forall {case x: AvroEnum => x.fullName == fullName})
          realCandidates.head
        else
          throw new IncompatibleTypes("incompatible enum types: " + realCandidates.mkString(" "))
      }

      else if (realCandidates forall {case _: AvroRecord => true; case _ => false}) {
        val fullName = realCandidates.head.asInstanceOf[AvroRecord].fullName
        if (realCandidates.tail forall {case x: AvroRecord => x.fullName == fullName})
          realCandidates.head
        else
          AvroUnion(distinctTypes(realCandidates).map(P.mustBeAvro))
      }

      else if (realCandidates forall {case _: FcnType => true; case _ => false}) {
        val params = realCandidates.head.asInstanceOf[FcnType].params
        val ret = realCandidates.head.asInstanceOf[FcnType].ret

        if (realCandidates.tail forall {case FcnType(p, r) => p == params  &&  r == ret})
          realCandidates.head
        else
          throw new IncompatibleTypes("incompatible function types: " + realCandidates.mkString(" "))
      }

      else if (!(realCandidates exists {case _: FcnType => true; case _ => false})) {
        val types = distinctTypes(realCandidates).map(P.mustBeAvro)
        if ((types collect {case _: AvroFixed => true} size) > 1)
          throw new IncompatibleTypes("incompatible fixed types: " + (realCandidates collect {case x: AvroFixed => x} mkString(" ")))
        if ((types collect {case _: AvroEnum => true} size) > 1)
          throw new IncompatibleTypes("incompatible enum types: " + (realCandidates collect {case x: AvroEnum => x} mkString(" ")))
        AvroUnion(types)
      }

      else
        throw new IncompatibleTypes("incompatible function/non-function types: " + realCandidates.mkString(" "))
    }
  }

  /** Used internally to carry information about a label in a wildcard and how well it matches a prospective argument type.
    */
  class LabelData {
    private var members: List[Type] = Nil
    var expectedFields: Option[Seq[String]] = None

    def add(t: Type): Unit = {members = t :: members}
    def determineAssignment: Type = {
      val out = LabelData.broadestType(members)
      expectedFields match {
        case Some(symbols) => out match {
          case AvroRecord(fields, _, _, _, _) =>
            if ((fields map {x: AvroField => x.name}) != symbols)
              throw new IncompatibleTypes("fields do not agree with EnumFields symbols (same names, same order)")
          case _ => throw new IncompatibleTypes("EnumFields.wildRecord does not point to a record")
        }
        case None =>
      }
      out
    }

    override def toString(): String = "LabelData([%s])".format(members.mkString(", "))
  }

  /** Abstract trait for function signatures.
    */
  trait Signature {
    def accepts(args: Seq[Type], version: PFAVersion): Option[(Sig, Seq[Type], AvroType)]
  }

  /** PFA function signature for ad-hoc polymorphism (list of different signatures supported by the function).
    * 
    * @param cases signatures supported by this function; the order of this list is the order in which Hadrian will attempt to resolve the signatures (the first one that matches is used)
    */
  case class Sigs(cases: Seq[Sig]) extends Signature {
    /** Determine if this list of signatures accpets the given arguments for a given PFA version number.
      * 
      * @param args arguments to match against each of the signature patterns
      * @param version PFA version number in which to interpret the patterns
      * @return Some(matching signature, resolved argument types, resolved return type) if one of the signatures accepts the arguments; `None` otherwise
      */
    def accepts(args: Seq[Type], version: PFAVersion): Option[(Sig, Seq[Type], AvroType)] =
      cases.view flatMap {_.accepts(args, version)} headOption
  }

  /** PFA function signature for a single pattern.
    * 
    * This class can either be used directly as a function's only signature or it may be contained within a [[com.opendatagroup.hadrian.signature.Sigs Sigs]].
    * 
    * @param params patterns for each argument (in order)
    * @param ret pattern for the return type
    * @param lifespan validity range for this signature; default is eternal (existing from the beginning of time to the end of time)
    */
  case class Sig(params: Seq[(String, Pattern)], ret: Pattern, lifespan: Lifespan = Lifespan(None, None, None, None)) extends Signature {
    override def toString() = {
      val alreadyLabeled = mutable.Set[String]()
      "(" + (params map {case (n, p) => n + ": " + toText(p, alreadyLabeled)} mkString(", ")) + " -> " + toText(ret, alreadyLabeled) + ")"
    }

    /** Determine if this signature accepts the given arguments for a given PFA version number.
      * 
      * @param args arguments to match against the signature pattern
      * @param version PFA version number in which to interpret the pattern
      * @return Some(self, resolved argument types, resolved return type) if this signature accepts the arguments; `None` otherwise
      */
    def accepts(args: Seq[Type], version: PFAVersion): Option[(Sig, Seq[Type], AvroType)] =
      if (lifespan.current(version)  ||  lifespan.deprecated(version)) {
        val labelData = mutable.Map[String, LabelData]()
        if (params.corresponds(args)({case ((n, p), a) => check(p, a, labelData, false, false)})) {
          try {
            val assignments = labelData map {case (l, ld) => (l, ld.determineAssignment)} toMap
            val assignedParams = params.zip(args) map {case ((n, p), a) => assign(p, a, assignments)}
            val assignedRet = assignRet(ret, assignments)
            Some((this, assignedParams, assignedRet))
          }
          catch {
            case _: IncompatibleTypes => None
          }
        }
        else
          None
      }
      else
        None

    /** Determine if a single slot in the parameter pattern reaches a single argument's type.
      * 
      * @param pat pattern for a type that can include wildcards, etc.
      * @param arg supplied argument type that may or may not fit the pattern
      * @param labelData label associations made so far (so that repeated label letters are forced to resolve to the same types)
      * @param strict if `true`, don't allow subtypes to match supertypes
      * @param reversed if `true`, match structures contravariantly instead of covariantly (for instance, when matching arguments of functions passed as arguments)
      * @return `true` if `arg` matches `pat`; `false` otherwise
      */
    def check(pat: Pattern, arg: Type, labelData: mutable.Map[String, LabelData], strict: Boolean, reversed: Boolean): Boolean = (pat, arg) match {
      case (_, _: ExceptionType) => false

      case (P.Null, AvroNull()) => true
      case (P.Boolean, AvroBoolean()) => true

      case (P.Int, _: AvroInt) => true
      case (P.Long, _: AvroLong) => true
      case (P.Float, _: AvroFloat) => true
      case (P.Double, _: AvroDouble) => true

      case (P.Long, _: AvroInt | _: AvroLong) if (!strict  &&  !reversed) => true
      case (P.Float, _: AvroInt | _: AvroLong | _: AvroFloat) if (!strict  &&  !reversed) => true
      case (P.Double, _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble) if (!strict  &&  !reversed) => true

      case (P.Int | P.Long, _: AvroLong) if (!strict  &&  reversed) => true
      case (P.Int | P.Long | P.Float, _: AvroFloat) if (!strict  &&  reversed) => true
      case (P.Int | P.Long | P.Float | P.Double, _: AvroDouble) if (!strict  &&  reversed) => true

      case (P.Bytes, AvroBytes()) => true
      case (P.String, AvroString()) => true

      case (P.Array(p), AvroArray(a)) => check(p, a, labelData, strict, reversed)
      case (P.Map(p), AvroMap(a)) => check(p, a, labelData, strict, reversed)
      case (P.Union(ptypes), AvroUnion(atypes)) => {
        var found: Boolean = false
        val originalLabelData = mutable.Map(labelData.toSeq: _*)
        var labelData2 = mutable.Map(labelData.toSeq: _*)
        val atypesPermutations = atypes.permutations

        while (!found  &&  !atypesPermutations.isEmpty) {
          val available = mutable.LinkedHashMap[Int, Pattern]((0 until ptypes.size) map {i => (i, ptypes(i))} : _*)
          found = atypesPermutations.next() forall {a =>
            available find {case (i, p) =>
              check(p, a, labelData2, true, reversed)
            } match {
              case Some((i, p)) => {
                available.remove(i)
                true
              }
              case None =>
                false
            }
          }
          if (!found) {
            labelData2 = mutable.Map(originalLabelData.toSeq: _*)
          }
        }

        for ((k, v) <- labelData2)
          labelData(k) = v

        found
      }
      case (P.Union(ptypes), a: AvroType) => ptypes exists {p => check(p, a, labelData, strict, reversed)}

      case (P.Fixed(_, Some(pFullName)), a: AvroFixed) => pFullName == a.fullName
      case (P.Fixed(psize, None), AvroFixed(asize, _, _, _, _)) => psize == asize

      case (P.Enum(_, Some(pFullName)), a: AvroEnum) => pFullName == a.fullName
      case (P.Enum(psymbols, None), a @ AvroEnum(asymbols, _, _, _, _)) => asymbols.toSet subsetOf psymbols.toSet

      case (P.Record(_, Some(pFullName)), a: AvroRecord) => pFullName == a.fullName
      case (P.Record(pfields, None), AvroRecord(afields, _, _, _, _)) => {
        val amap = afields map {case AvroField(name, avroType, _, _, _, _) => (name, avroType)} toMap

        if (pfields.keys.toSet == amap.keys.toSet)
          pfields forall {case (pn, pt) => check(pt, amap(pn), labelData, true, reversed)}
        else
          false
      }

      case (P.Fcn(pparam, pret), FcnType(aparams, aret)) =>
        pparam.corresponds(aparams)({case (p, a) => check(p, a, labelData, strict, true)})  &&
          check(pret, aret, labelData, strict, false)

      case (P.Wildcard(label, oneOf), a) => {
        if (oneOf.isEmpty  ||  oneOf.contains(a)) {
          if (!labelData.contains(label))
            labelData(label) = new LabelData
          labelData(label).add(a)
          true
        }
        else
          false
      }

      case (P.WildRecord(label, minimalFields), a @ AvroRecord(afields, _, _, _, _)) => {
        if (!labelData.contains(label))
          labelData(label) = new LabelData
        labelData(label).add(a)

        val amap = afields map {case AvroField(name, avroType, _, _, _, _) => (name, avroType)} toMap

        if (minimalFields.keys.toSet subsetOf amap.keys.toSet)
          minimalFields forall {case (pn, pt) => check(pt, amap(pn), labelData, true, reversed)}
        else
          false
      }

      case (P.WildEnum(label), a: AvroEnum) => {
        if (!labelData.contains(label))
          labelData(label) = new LabelData
        labelData(label).add(a)
        true
      }

      case (P.WildFixed(label), a: AvroFixed) => {
        if (!labelData.contains(label))
          labelData(label) = new LabelData
        labelData(label).add(a)
        true
      }

      case (P.EnumFields(label, wildRecord), a @ AvroEnum(symbols, _, _, _, _)) => {
        if (!labelData.contains(label))
          labelData(label) = new LabelData
        labelData(label).add(a)
        labelData(wildRecord).expectedFields = Some(symbols)
        true
      }

      case _ => false
    }

    /** Apply the label assignments (e.g. "A" matched to "int", "B" matched to "string", etc.) to each parameter of the signature.
      * 
      * @param original parameter pattern
      * @param supplied argument type
      * @param assigned types to apply
      * @return resolved type for one parameter of the signature
      */
    def assign(pat: Pattern, arg: Type, assignments: Map[String, Type]): Type = (pat, arg) match {
      case (P.Null, AvroNull()) => arg
      case (P.Boolean, AvroBoolean()) => arg

      case (P.Int, _: AvroInt) => AvroInt()
      case (P.Long, _: AvroInt | _: AvroLong) => AvroLong()
      case (P.Float, _: AvroInt | _: AvroLong | _: AvroFloat) => AvroFloat()
      case (P.Double, _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble) => AvroDouble()

      case (P.Bytes, AvroBytes()) => arg
      case (P.String, AvroString()) => arg

      case (P.Array(p), AvroArray(a)) => AvroArray(P.mustBeAvro(assign(p, a, assignments)))
      case (P.Map(p), AvroMap(a)) => AvroMap(P.mustBeAvro(assign(p, a, assignments)))
      case (P.Union(_), a: AvroUnion) => a
      case (P.Union(_), a: AvroType) => a

      case (_: P.Fixed, a: AvroFixed) => a
      case (_: P.Enum, a: AvroEnum) => a
      case (_: P.Record, a: AvroRecord) => a

      case (_: P.Fcn, a: FcnType) => a

      case (P.Wildcard(label, _), _) => assignments(label)
      case (P.WildRecord(label, _), _) => assignments(label)
      case (P.WildEnum(label), _) => assignments(label)
      case (P.WildFixed(label), _) => assignments(label)
      case (P.EnumFields(label, _), _) => assignments(label)
    }

    /** Apply the label assignments (e.g. "A" matched to "int", "B" matched to "string", etc.) to the return pattern.
      * 
      * @param original return pattern
      * @param assignments assigned types to apply
      * @return resolved type for the return value of the signature
      */
    def assignRet(pat: Pattern, assignments: Map[String, Type]): AvroType = pat match {
      case P.Null => AvroNull()
      case P.Boolean => AvroBoolean()
      case P.Int => AvroInt()
      case P.Long => AvroLong()
      case P.Float => AvroFloat()
      case P.Double => AvroDouble()
      case P.Bytes => AvroBytes()
      case P.String => AvroString()

      case P.Array(p) => AvroArray(assignRet(p, assignments))
      case P.Map(p) => AvroMap(assignRet(p, assignments))
      case P.Union(types) => AvroUnion(types map {t => assignRet(t, assignments)})

      case x: P.Fixed => P.toType(x).asInstanceOf[AvroType]
      case x: P.Enum => P.toType(x).asInstanceOf[AvroType]
      case x: P.Record => P.toType(x).asInstanceOf[AvroType]
      case x: P.Fcn => P.toType(x).asInstanceOf[AvroType]

      case P.Wildcard(label, _) => assignments(label).asInstanceOf[AvroType]
      case P.WildRecord(label, _) => assignments(label).asInstanceOf[AvroType]
      case P.WildEnum(label) => assignments(label).asInstanceOf[AvroType]
      case P.WildFixed(label) => assignments(label).asInstanceOf[AvroType]
      case P.EnumFields(label, _) => assignments(label).asInstanceOf[AvroType]
    }
  }

  /** Render a pattern as LaTeX.
    */
  object toLaTeX extends Function1[Pattern, String] {
    /** @param p the pattern
    * @return text representation of the pattern
    */
    def apply(p: Pattern): String = apply(p, mutable.Set[String]())

    // Note: a recursive record (possibly through a union) would make this infinite-loop.
    // It's not smart, but nothing in the library uses recursive records, so it's not a problem yet.
    /** @param p the pattern
    * @param alreadyLabeled used to avoid recursion in recursive types
    * @return text representation of the pattern
    */
    def apply(p: Pattern, alreadyLabeled: mutable.Set[String]): String = p match {
      case P.Null => "null"
      case P.Boolean => "boolean"
      case P.Int => "int"
      case P.Long => "long"
      case P.Float => "float"
      case P.Double => "double"
      case P.Bytes => "bytes"
      case P.Fixed(size, Some(fullName)) => val name = fullName.replace("_", "\\_");  s"fixed ({\\it size:} $size, {\\it name:} $name)"
      case P.Fixed(size, None) => s"fixed ({\\it size:} $size)"
      case P.String => "string"
      case P.Enum(symbols, Some(fullName)) => val name = fullName.replace("_", "\\_");  val syms = symbols map {_.replace("_", "\\_")};  s"enum ({\\it symbols:} $syms, {\\it name:} $name)"
      case P.Enum(symbols, None) => val syms = symbols map {_.replace("_", "\\_")};  s"enum ({\\it symbols:} $syms)"
      case P.Array(items) => "array of " + apply(items, alreadyLabeled)
      case P.Map(values) => "map of " + apply(values, alreadyLabeled)
      case P.Record(fields, Some(fullName)) => val name = fullName.replace("_", "\\_");  val fs = fields.map({case (n, f) => "{\\PFApf " + n.replace("_", "\\_") + ":}$\\!$ " + apply(f, alreadyLabeled)}).mkString(", ");  s"record ({\\it fields:} \\{$fs\\}, {\\it name:} $name)"
      case P.Record(fields, None) => val fs = fields.map({case (n, f) => "{\\PFApf " + n.replace("_", "\\_") + ":}$\\!$ " + apply(f, alreadyLabeled)}).mkString(", ");  s"record ({\\it fields:} \\{$fs\\})"
      case P.Union(types) => "union of \\{%s\\}".format(types map {apply(_, alreadyLabeled)} mkString(", "))
      case P.Fcn(params, ret) => "function (%s) $\\to$ %s".format(params map {apply(_, alreadyLabeled)} mkString(", "), apply(ret, alreadyLabeled))
      case P.Wildcard(label, _) if (alreadyLabeled.contains(label)) => s"{\\PFAtp $label}"
      case P.WildEnum(label) if (alreadyLabeled.contains(label)) => s"{\\PFAtp $label}"
      case P.WildFixed(label) if (alreadyLabeled.contains(label)) => s"{\\PFAtp $label}"
      case P.WildRecord(label, _) if (alreadyLabeled.contains(label)) => s"{\\PFAtp $label}"
      case P.EnumFields(label, _) if (alreadyLabeled.contains(label)) => s"{\\PFAtp $label}"
      case P.Wildcard(label, oneOf) if (oneOf.isEmpty) => alreadyLabeled.add(label);  s"any {\\PFAtp $label}"
      case P.Wildcard(label, oneOf) => alreadyLabeled.add(label);  val types = oneOf.map({applyType(_)}).mkString(", ");  s"any {\\PFAtp $label} of \\{$types\\}"
      case P.WildEnum(label) => alreadyLabeled.add(label);  s"any enum {\\PFAtp $label}"
      case P.WildFixed(label) => alreadyLabeled.add(label);  s"any fixed {\\PFAtp $label}"
      case P.WildRecord(label, fields) => alreadyLabeled.add(label);  val fs = fields.map({case (n, f) => "{\\PFApf " + n.replace("_", "\\_") + ":}$\\!$ " + apply(f, alreadyLabeled)}).mkString(", ");  s"any record {\\PFAtp $label}" + (if (fields.isEmpty) "" else s" with \\{$fs\\}")
      case P.EnumFields(label, wildRecord) => alreadyLabeled.add(label);  s"enum $label of fields of $wildRecord"
    }

    private def applyType(t: Type): String = t match {
      case AvroNull() => "null"
      case AvroBoolean() => "boolean"
      case AvroInt() => "int"
      case AvroLong() => "long"
      case AvroFloat() => "float"
      case AvroDouble() => "double"
      case AvroBytes() => "bytes"
      case AvroString() => "string"
      case AvroArray(items) => "array of " + applyType(items)
      case AvroMap(values) => "map of " + applyType(values)
      // finish on an as-needed basis
    }
  }

  /** Render a pattern as HTML.
    */
  object toHTML extends Function1[Pattern, String] {
    /** @param p the pattern
    * @return text representation of the pattern
    */
    def apply(p: Pattern): String = apply(p, mutable.Set[String]())

    // Note: a recursive record (possibly through a union) would make this infinite-loop.
    // It's not smart, but nothing in the library uses recursive records, so it's not a problem yet.
    /** @param p the pattern
    * @param alreadyLabeled used to avoid recursion in recursive types
    * @return text representation of the pattern
    */
    def apply(p: Pattern, alreadyLabeled: mutable.Set[String]): String = p match {
      case P.Null => "null"
      case P.Boolean => "boolean"
      case P.Int => "int"
      case P.Long => "long"
      case P.Float => "float"
      case P.Double => "double"
      case P.Bytes => "bytes"
      case P.Fixed(size, Some(fullName)) => s"fixed(<i>size:</i> $size, <i>name:</i> $fullName)"
      case P.Fixed(size, None) => s"fixed(<i>size:</i> $size)"
      case P.String => "string"
      case P.Enum(symbols, Some(fullName)) => s"""enum(<i>symbols:</i> ${symbols.mkString(", ")}, <i>name:</i> $fullName)"""
      case P.Enum(symbols, None) => s"""enum (<i>symbols:</i> ${symbols.mkString(", ")})"""
      case P.Array(items) => "array of " + apply(items, alreadyLabeled)
      case P.Map(values) => "map of " + apply(values, alreadyLabeled)
      case P.Record(fields, Some(fullName)) => s"""record (<i>fields:</i> {${fields.map({case (n, f) => "<i>" + n + "</i>: " + apply(f, alreadyLabeled)}).mkString(", ")}}, <i>name:</i> $fullName)"""
      case P.Record(fields, None) => s"""record (<i>fields:</i> {${fields.map({case (n, f) => "<i>" + n + "</i>: " + apply(f, alreadyLabeled)}).mkString(", ")}})"""
      case P.Union(types) => s"""union of {${types map {apply(_, alreadyLabeled)} mkString(", ")}}"""
      case P.Fcn(params, ret) => s"""function of (${params map {apply(_, alreadyLabeled)} mkString(", ")}) &rarr; ${apply(ret, alreadyLabeled)}"""
      case P.Wildcard(label, _) if (alreadyLabeled.contains(label)) => s"<b>$label</b>"
      case P.WildEnum(label) if (alreadyLabeled.contains(label)) => s"<b>$label</b>"
      case P.WildFixed(label) if (alreadyLabeled.contains(label)) => s"<b>$label</b>"
      case P.WildRecord(label, _) if (alreadyLabeled.contains(label)) => s"<b>$label</b>"
      case P.EnumFields(label, _) if (alreadyLabeled.contains(label)) => s"<b>$label</b>"
      case P.Wildcard(label, oneOf) if (oneOf.isEmpty) => alreadyLabeled.add(label);  s"any <b>$label</b>"
      case P.Wildcard(label, oneOf) => alreadyLabeled.add(label);  s"any <b>$label</b> of {${oneOf.map({applyType(_)}).mkString(", ")}}"
      case P.WildEnum(label) => alreadyLabeled.add(label);  s"any enum <b>$label</b>"
      case P.WildFixed(label) => alreadyLabeled.add(label);  s"any fixed <b>$label</b>"
      case P.WildRecord(label, fields) => alreadyLabeled.add(label);  s"""any record <b>$label</b>""" + (if (fields.isEmpty) "" else s""" with fields {${fields.map({case (n, f) => "<i>" + n + "</i>: " + apply(f, alreadyLabeled)}).mkString(", ")}}""")
      case P.EnumFields(label, wildRecord) => alreadyLabeled.add(label);  s"enum <b>$label</b> of fields of $wildRecord"
    }

    private def applyType(t: Type): String = t match {
      case AvroNull() => "null"
      case AvroBoolean() => "boolean"
      case AvroInt() => "int"
      case AvroLong() => "long"
      case AvroFloat() => "float"
      case AvroDouble() => "double"
      case AvroBytes() => "bytes"
      case AvroString() => "string"
      case AvroArray(items) => "array of " + applyType(items)
      case AvroMap(values) => "map of " + applyType(values)
      // finish on an as-needed basis
    }
  }

  /** Render a pattern as human-readable text.
    */
  object toText extends Function1[Pattern, String] {
    /** @param p the pattern
    * @return text representation of the pattern
    */
    def apply(p: Pattern): String = apply(p, mutable.Set[String]())

    // Note: a recursive record (possibly through a union) would make this infinite-loop.
    // It's not smart, but nothing in the library uses recursive records, so it's not a problem yet.
    /** @param p the pattern
    * @param alreadyLabeled used to avoid recursion in recursive types
    * @return text representation of the pattern
    */
    def apply(p: Pattern, alreadyLabeled: mutable.Set[String]): String = p match {
      case P.Null => "null"
      case P.Boolean => "boolean"
      case P.Int => "int"
      case P.Long => "long"
      case P.Float => "float"
      case P.Double => "double"
      case P.Bytes => "bytes"
      case P.Fixed(size, Some(fullName)) => s"fixed(size: $size, name: $fullName)"
      case P.Fixed(size, None) => s"fixed(size: $size)"
      case P.String => "string"
      case P.Enum(symbols, Some(fullName)) => s"""enum(symbols: ${symbols.mkString(", ")}, name: $fullName)"""
      case P.Enum(symbols, None) => s"""enum (symbols: ${symbols.mkString(", ")})"""
      case P.Array(items) => "array of " + apply(items, alreadyLabeled)
      case P.Map(values) => "map of " + apply(values, alreadyLabeled)
      case P.Record(fields, Some(fullName)) => s"""record (fields: {${fields.map({case (n, f) => "" + n + ": " + apply(f, alreadyLabeled)}).mkString(", ")}}, name: $fullName)"""
      case P.Record(fields, None) => s"""record (fields: {${fields.map({case (n, f) => "" + n + ": " + apply(f, alreadyLabeled)}).mkString(", ")}})"""
      case P.Union(types) => s"""union of {${types map {apply(_, alreadyLabeled)} mkString(", ")}}"""
      case P.Fcn(params, ret) => s"""function of (${params map {apply(_, alreadyLabeled)} mkString(", ")}) -> ${apply(ret, alreadyLabeled)}"""
      case P.Wildcard(label, _) if (alreadyLabeled.contains(label)) => s"$label"
      case P.WildEnum(label) if (alreadyLabeled.contains(label)) => s"$label"
      case P.WildFixed(label) if (alreadyLabeled.contains(label)) => s"$label"
      case P.WildRecord(label, _) if (alreadyLabeled.contains(label)) => s"$label"
      case P.EnumFields(label, _) if (alreadyLabeled.contains(label)) => s"$label"
      case P.Wildcard(label, oneOf) if (oneOf.isEmpty) => alreadyLabeled.add(label);  s"any $label"
      case P.Wildcard(label, oneOf) => alreadyLabeled.add(label);  s"any $label of {${oneOf.map({applyType(_)}).mkString(", ")}}"
      case P.WildEnum(label) => alreadyLabeled.add(label);  s"any enum $label"
      case P.WildFixed(label) => alreadyLabeled.add(label);  s"any fixed $label"
      case P.WildRecord(label, fields) => alreadyLabeled.add(label);  s"""any record $label""" + (if (fields.isEmpty) "" else s""" with fields {${fields.map({case (n, f) => "" + n + ": " + apply(f, alreadyLabeled)}).mkString(", ")}}""")
      case P.EnumFields(label, wildRecord) => alreadyLabeled.add(label);  s"enum $label of fields of $wildRecord"
    }

    def applyType(t: Type): String = t match {
      case AvroNull() => "null"
      case AvroBoolean() => "boolean"
      case AvroInt() => "int"
      case AvroLong() => "long"
      case AvroFloat() => "float"
      case AvroDouble() => "double"
      case AvroBytes() => "bytes"
      case AvroString() => "string"
      case AvroArray(items) => "array of " + applyType(items)
      case AvroMap(values) => "map of " + applyType(values)
      // finish on an as-needed basis
    }
  }

}
