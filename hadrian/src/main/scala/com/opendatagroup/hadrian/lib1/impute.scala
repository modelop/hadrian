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

package com.opendatagroup.hadrian.lib1

import com.opendatagroup.hadrian.ast.LibFcn
import com.opendatagroup.hadrian.errors.PFARuntimeException
import com.opendatagroup.hadrian.jvmcompiler.JavaCode
import com.opendatagroup.hadrian.jvmcompiler.javaSchema

import com.opendatagroup.hadrian.ast.AstContext
import com.opendatagroup.hadrian.ast.ExpressionContext
import com.opendatagroup.hadrian.ast.FcnDef
import com.opendatagroup.hadrian.ast.FcnRef

import com.opendatagroup.hadrian.data.PFAArray
import com.opendatagroup.hadrian.data.PFAEnumSymbol
import com.opendatagroup.hadrian.data.PFAFixed
import com.opendatagroup.hadrian.data.PFAMap
import com.opendatagroup.hadrian.data.PFARecord

import com.opendatagroup.hadrian.jvmcompiler.javaType

import com.opendatagroup.hadrian.signature.P
import com.opendatagroup.hadrian.signature.Sig
import com.opendatagroup.hadrian.signature.Signature
import com.opendatagroup.hadrian.signature.Sigs

import com.opendatagroup.hadrian.datatype.Type
import com.opendatagroup.hadrian.datatype.FcnType
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
import com.opendatagroup.hadrian.datatype.AvroField
import com.opendatagroup.hadrian.datatype.AvroUnion

package object impute {
  private var fcns = Map[String, LibFcn]()
  def provides = fcns
  def provide(libFcn: LibFcn): Unit =
    fcns = fcns + Tuple2(libFcn.name, libFcn)

  val prefix = "impute."

  // TODO: errorOnNaN, defaultOnNaN, errorOnInf, defaultOnInf
  // functions that keep a running average in a cell...
  // functors that take an handler function...

  ////   errorOnNull (ErrorOnNull)
  object ErrorOnNull extends LibFcn {
    val name = prefix + "errorOnNull"
    val sig = Sig(List("x" -> P.Union(List(P.Wildcard("A"), P.Null))), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Skip an action by raising an "encountered null" runtime error when <p>x</p> is <c>null</c>.</desc>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.ret match {
      case _: AvroBoolean => JavaCode(DoBoolean.getClass.getName + ".MODULE$")
      case _: AvroInt => JavaCode(DoInt.getClass.getName + ".MODULE$")
      case _: AvroLong => JavaCode(DoLong.getClass.getName + ".MODULE$")
      case _: AvroFloat => JavaCode(DoFloat.getClass.getName + ".MODULE$")
      case _: AvroDouble => JavaCode(DoDouble.getClass.getName + ".MODULE$")
      case _: AvroBytes => JavaCode(DoBytes.getClass.getName + ".MODULE$")
      case _: AvroFixed => JavaCode(DoFixed.getClass.getName + ".MODULE$")
      case _: AvroString => JavaCode(DoString.getClass.getName + ".MODULE$")
      case _: AvroEnum => JavaCode(DoEnum.getClass.getName + ".MODULE$")
      case _: AvroArray => JavaCode(DoArray.getClass.getName + ".MODULE$")
      case _: AvroMap => JavaCode(DoMap.getClass.getName + ".MODULE$")
      case _: AvroRecord => JavaCode(DoRecord.getClass.getName + ".MODULE$")
    }
    object DoBoolean {
      def apply(x: java.lang.Boolean): Boolean = x match {
        case null => throw new PFARuntimeException("encountered null")
        case _ => x.asInstanceOf[java.lang.Boolean].booleanValue
      }
    }
    object DoInt {
      def apply(x: AnyRef): Int = x match {
        case null => throw new PFARuntimeException("encountered null")
        case _ => x.asInstanceOf[java.lang.Integer].intValue
      }
    }
    object DoLong {
      def apply(x: AnyRef): Long = x match {
        case null => throw new PFARuntimeException("encountered null")
        case _ => x.asInstanceOf[java.lang.Long].longValue
      }
    }
    object DoFloat {
      def apply(x: AnyRef): Float = x match {
        case null => throw new PFARuntimeException("encountered null")
        case _ => x.asInstanceOf[java.lang.Float].floatValue
      }
    }
    object DoDouble {
      def apply(x: AnyRef): Double = x match {
        case null => throw new PFARuntimeException("encountered null")
        case _ => x.asInstanceOf[java.lang.Double].doubleValue
      }
    }
    object DoBytes {
      def apply(x: AnyRef): Array[Byte] = x match {
        case null => throw new PFARuntimeException("encountered null")
        case _ => x.asInstanceOf[Array[Byte]]
      }
    }
    object DoFixed {
      def apply(x: AnyRef): PFAFixed = x match {
        case null => throw new PFARuntimeException("encountered null")
        case _ => x.asInstanceOf[PFAFixed]
      }
    }
    object DoString {
      def apply(x: AnyRef): String = x match {
        case null => throw new PFARuntimeException("encountered null")
        case _ => x.asInstanceOf[String]
      }
    }
    object DoEnum {
      def apply(x: AnyRef): PFAEnumSymbol = x match {
        case null => throw new PFARuntimeException("encountered null")
        case _ => x.asInstanceOf[PFAEnumSymbol]
      }
    }
    object DoArray {
      def apply(x: AnyRef): PFAArray[_] = x match {
        case null => throw new PFARuntimeException("encountered null")
        case _ => x.asInstanceOf[PFAArray[_]]
      }
    }
    object DoMap {
      def apply(x: AnyRef): PFAMap[_] = x match {
        case null => throw new PFARuntimeException("encountered null")
        case _ => x.asInstanceOf[PFAMap[_]]
      }
    }
    object DoRecord {
      def apply(x: AnyRef): PFARecord = x match {
        case null => throw new PFARuntimeException("encountered null")
        case _ => x.asInstanceOf[PFARecord]
      }
    }
  }
  provide(ErrorOnNull)

  ////   defaultOnNull (DefaultOnNull)
  object DefaultOnNull extends LibFcn {
    val name = prefix + "defaultOnNull"
    val sig = Sig(List("x" -> P.Union(List(P.Wildcard("A"), P.Null)), "default" -> P.Wildcard("A")), P.Wildcard("A"))
    val doc =
      <doc>
        <desc>Replace <c>null</c> values in <p>x</p> with <p>default</p>.</desc>
      </doc>
    override def javaRef(fcnType: FcnType): JavaCode = fcnType.ret match {
      case _: AvroBoolean => JavaCode(DoBoolean.getClass.getName + ".MODULE$")
      case _: AvroInt => JavaCode(DoInt.getClass.getName + ".MODULE$")
      case _: AvroLong => JavaCode(DoLong.getClass.getName + ".MODULE$")
      case _: AvroFloat => JavaCode(DoFloat.getClass.getName + ".MODULE$")
      case _: AvroDouble => JavaCode(DoDouble.getClass.getName + ".MODULE$")
      case _: AvroBytes => JavaCode(DoBytes.getClass.getName + ".MODULE$")
      case _: AvroFixed => JavaCode(DoFixed.getClass.getName + ".MODULE$")
      case _: AvroString => JavaCode(DoString.getClass.getName + ".MODULE$")
      case _: AvroEnum => JavaCode(DoEnum.getClass.getName + ".MODULE$")
      case _: AvroArray => JavaCode(DoArray.getClass.getName + ".MODULE$")
      case _: AvroMap => JavaCode(DoMap.getClass.getName + ".MODULE$")
      case _: AvroRecord => JavaCode(DoRecord.getClass.getName + ".MODULE$")
    }
    object DoBoolean {
      def apply(x: java.lang.Boolean, default: Boolean): Boolean = x match {
        case null => default
        case _ => x.asInstanceOf[java.lang.Boolean].booleanValue
      }
    }
    object DoInt {
      def apply(x: AnyRef, default: Int): Int = x match {
        case null => default
        case _ => x.asInstanceOf[java.lang.Integer].intValue
      }
    }
    object DoLong {
      def apply(x: AnyRef, default: Long): Long = x match {
        case null => default
        case _ => x.asInstanceOf[java.lang.Long].longValue
      }
    }
    object DoFloat {
      def apply(x: AnyRef, default: Float): Float = x match {
        case null => default
        case _ => x.asInstanceOf[java.lang.Float].floatValue
      }
    }
    object DoDouble {
      def apply(x: AnyRef, default: Double): Double = x match {
        case null => default
        case _ => x.asInstanceOf[java.lang.Double].doubleValue
      }
    }
    object DoBytes {
      def apply(x: AnyRef, default: Array[Byte]): Array[Byte] = x match {
        case null => default
        case _ => x.asInstanceOf[Array[Byte]]
      }
    }
    object DoFixed {
      def apply(x: AnyRef, default: PFAFixed): PFAFixed = x match {
        case null => default
        case _ => x.asInstanceOf[PFAFixed]
      }
    }
    object DoString {
      def apply(x: AnyRef, default: String): String = x match {
        case null => default
        case _ => x.asInstanceOf[String]
      }
    }
    object DoEnum {
      def apply(x: AnyRef, default: PFAEnumSymbol): PFAEnumSymbol = x match {
        case null => default
        case _ => x.asInstanceOf[PFAEnumSymbol]
      }
    }
    object DoArray {
      def apply[X](x: AnyRef, default: PFAArray[X]): PFAArray[X] = x match {
        case null => default
        case _ => x.asInstanceOf[PFAArray[X]]
      }
    }
    object DoMap {
      def apply[X <: AnyRef](x: AnyRef, default: PFAMap[X]): PFAMap[X] = x match {
        case null => default
        case _ => x.asInstanceOf[PFAMap[X]]
      }
    }
    object DoRecord {
      def apply(x: AnyRef, default: PFARecord): PFARecord = x match {
        case null => default
        case _ => x.asInstanceOf[PFARecord]
      }
    }
  }
  provide(DefaultOnNull)

}
