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

import scala.collection.mutable
import scala.language.postfixOps

import com.opendatagroup.hadrian.datatype.Type
import com.opendatagroup.hadrian.datatype.FcnType
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
  class IncompatibleTypes(message: String) extends Exception(message)

  trait Pattern
  object P {
    case object Null extends Pattern
    case object Boolean extends Pattern
    case object Int extends Pattern
    case object Long extends Pattern
    case object Float extends Pattern
    case object Double extends Pattern
    case object Bytes extends Pattern
    case object String extends Pattern

    case class Array(items: Pattern) extends Pattern
    case class Map(values: Pattern) extends Pattern
    case class Union(types: List[Pattern]) extends Pattern

    case class Fixed(size: Int, fullName: Option[String] = None) extends Pattern
    case class Enum(symbols: List[String], fullName: Option[String] = None) extends Pattern
    case class Record(fields: scala.collection.immutable.Map[String, Pattern], fullName: Option[String] = None) extends Pattern

    case class Fcn(params: List[Pattern], ret: Pattern) extends Pattern

    case class Wildcard(label: String, oneOf: Set[Type] = Set[Type]()) extends Pattern
    case class WildRecord(label: String, minimalFields: scala.collection.immutable.Map[String, Pattern]) extends Pattern
    case class EnumFields(label: String, wildRecord: String) extends Pattern

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

    def fromType(t: Type): Pattern = t match {
      case AvroNull() => Null
      case AvroBoolean() => Boolean
      case AvroInt() => Int
      case AvroLong() => Long
      case AvroFloat() => Float
      case AvroDouble() => Double
      case AvroBytes() => Bytes
      case AvroString() => String

      case AvroArray(items) => Array(fromType(items))
      case AvroMap(values) => Map(fromType(values))
      case AvroUnion(types) => Union(types.map(fromType(_)).toList)

      case AvroFixed(size, name, Some(namespace), _, _) => Fixed(size, Some(namespace + "." + name))
      case AvroFixed(size, name, None, _, _) => Fixed(size, Some(name))

      case AvroEnum(symbols, name, Some(namespace), _, _) => Enum(symbols, Some(namespace + "." + name))
      case AvroEnum(symbols, name, None, _, _) => Enum(symbols, Some(name))

      case AvroRecord(fields, name, Some(namespace), _, _) => Record(scala.collection.immutable.Map[String, Pattern](), Some(namespace + "." + name))
      case AvroRecord(fields, name, None, _, _) => Record(scala.collection.immutable.Map[String, Pattern](), Some(name))

      case FcnType(params, ret) => Fcn(params.map(fromType).toList, fromType(ret))
    }

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

    def broadestType(candidates: List[Type]): Type = {
      if (candidates.isEmpty)
        throw new IncompatibleTypes("empty list of types")

      else if (candidates forall {case _: AvroNull => true; case _ => false})
        candidates.head
      else if (candidates forall {case _: AvroBoolean => true; case _ => false})
        candidates.head

      else if (candidates forall {case _: AvroInt => true; case _ => false})
        candidates.head
      else if (candidates forall {case _: AvroInt | _: AvroLong => true; case _ => false})
        candidates collectFirst {case x: AvroLong => x} get
      else if (candidates forall {case _: AvroInt | _: AvroLong | _: AvroFloat => true; case _ => false})
        candidates collectFirst {case x: AvroFloat => x} get
      else if (candidates forall {case _: AvroInt | _: AvroLong | _: AvroFloat | _: AvroDouble => true; case _ => false})
        candidates collectFirst {case x: AvroDouble => x} get

      else if (candidates forall {case _: AvroBytes => true; case _ => false})
        candidates.head
      else if (candidates forall {case _: AvroString => true; case _ => false})
        candidates.head

      else if (candidates forall {case _: AvroArray => true; case _ => false})
        AvroArray(P.mustBeAvro(broadestType(candidates map {case AvroArray(items) => items})))

      else if (candidates forall {case _: AvroMap => true; case _ => false})
        AvroMap(P.mustBeAvro(broadestType(candidates map {case AvroMap(values) => values})))

      else if (candidates forall {case _: AvroUnion => true; case _ => false})
        AvroUnion(distinctTypes(candidates flatMap {case AvroUnion(types) => types}).map(P.mustBeAvro))

      else if (candidates forall {case _: AvroFixed => true; case _ => false}) {
        val fullName = candidates.head.asInstanceOf[AvroFixed].fullName
        if (candidates.tail forall {case x: AvroFixed => x.fullName == fullName})
          candidates.head
        else
          throw new IncompatibleTypes("incompatible fixed types: " + candidates.mkString(" "))
      }

      else if (candidates forall {case _: AvroEnum => true; case _ => false}) {
        val fullName = candidates.head.asInstanceOf[AvroEnum].fullName
        if (candidates.tail forall {case x: AvroEnum => x.fullName == fullName})
          candidates.head
        else
          throw new IncompatibleTypes("incompatible enum types: " + candidates.mkString(" "))
      }

      else if (candidates forall {case _: AvroRecord => true; case _ => false}) {
        val fullName = candidates.head.asInstanceOf[AvroRecord].fullName
        if (candidates.tail forall {case x: AvroRecord => x.fullName == fullName})
          candidates.head
        else
          AvroUnion(distinctTypes(candidates).map(P.mustBeAvro))
      }

      else if (candidates forall {case _: FcnType => true; case _ => false}) {
        val params = candidates.head.asInstanceOf[FcnType].params
        val ret = candidates.head.asInstanceOf[FcnType].ret

        if (candidates.tail forall {case FcnType(p, r) => p == params  &&  r == ret})
          candidates.head
        else
          throw new IncompatibleTypes("incompatible function types: " + candidates.mkString(" "))
      }

      else if (!(candidates exists {case _: FcnType => true; case _ => false})) {
        val types = distinctTypes(candidates).map(P.mustBeAvro)
        if ((types collect {case _: AvroFixed => true} size) > 1)
          throw new IncompatibleTypes("incompatible fixed types: " + (candidates collect {case x: AvroFixed => x} mkString(" ")))
        if ((types collect {case _: AvroEnum => true} size) > 1)
          throw new IncompatibleTypes("incompatible enum types: " + (candidates collect {case x: AvroEnum => x} mkString(" ")))
        AvroUnion(types)
      }

      else
        throw new IncompatibleTypes("incompatible function/non-function types: " + candidates.mkString(" "))
    }
  }

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

  trait Signature {
    def accepts(args: Seq[Type]): Option[(Seq[Type], AvroType)]
  }

  case class Sigs(cases: Seq[Sig]) extends Signature {
    def accepts(args: Seq[Type]): Option[(Seq[Type], AvroType)] =
      cases.view flatMap {_.accepts(args)} headOption
  }

  case class Sig(params: Seq[(String, Pattern)], ret: Pattern) extends Signature {
    def accepts(args: Seq[Type]): Option[(Seq[Type], AvroType)] = {
      val labelData = mutable.Map[String, LabelData]()
      if (params.corresponds(args)({case ((n, p), a) => check(p, a, labelData, false, false)})) {
        try {
          val assignments = labelData map {case (l, ld) => (l, ld.determineAssignment)} toMap
          val assignedParams = params.zip(args) map {case ((n, p), a) => assign(p, a, assignments)}
          val assignedRet = assignRet(ret, assignments)
          Some((assignedParams, assignedRet))
        }
        catch {
          case _: IncompatibleTypes => None
        }
      }
      else
        None
    }

    private def check(pat: Pattern, arg: Type, labelData: mutable.Map[String, LabelData], strict: Boolean, reversed: Boolean): Boolean = (pat, arg) match {
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
          val available = mutable.Map[Int, Pattern]((0 until ptypes.size) map {i => (i, ptypes(i))} : _*)
          found = atypesPermutations.next() forall {a =>
            available find {case (i, p) => check(p, a, labelData2, true, reversed)} match {
              case Some((i, p)) => {
                available.remove(i)
                true
              }
              case None => false
            }
          }

          if (!found)
            labelData2 = mutable.Map(originalLabelData.toSeq: _*)
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

      case (P.EnumFields(label, wildRecord), a @ AvroEnum(symbols, _, _, _, _)) => {
        if (!labelData.contains(label))
          labelData(label) = new LabelData
        labelData(label).add(a)
        labelData(wildRecord).expectedFields = Some(symbols)
        true
      }

      case _ => false
    }

    private def assign(pat: Pattern, arg: Type, assignments: Map[String, Type]): Type = (pat, arg) match {
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
      case (P.EnumFields(label, _), _) => assignments(label)
    }

    private def assignRet(pat: Pattern, assignments: Map[String, Type]): AvroType = pat match {
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
      case P.EnumFields(label, _) => assignments(label).asInstanceOf[AvroType]
    }
  }

  object toLaTeX extends Function1[Pattern, String] {
    def apply(p: Pattern): String = apply(p, mutable.Set[String]())

    // Note: a recursive record (possibly through a union) would make this infinite-loop.
    // It's not smart, but nothing in the library uses recursive records, so it's not a problem yet.
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
      case P.WildRecord(label, _) if (alreadyLabeled.contains(label)) => s"{\\PFAtp $label}"
      case P.Wildcard(label, oneOf) if (oneOf.isEmpty) => alreadyLabeled.add(label);  s"any {\\PFAtp $label}"
      case P.Wildcard(label, oneOf) => alreadyLabeled.add(label);  val types = oneOf.map({applyType(_)}).mkString(", ");  s"any {\\PFAtp $label} of \\{$types\\}"
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
}
