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

package com.opendatagroup.antinous

import scala.collection.JavaConversions._
import scala.language.postfixOps
import scala.Function1
import scala.util.Random

import org.python.core.Py
import org.python.core.PyObject
import org.python.core.PyInteger
import org.python.core.PyString
import org.python.core.PyTuple
import org.python.core.PyList
import org.python.core.PyDictionary
import org.python.core.PyFunction

import com.opendatagroup.hadrian.datatype.AvroType

package object producer {
  private[producer] var random = new Random
  def setRandomSeed(x: Long) {
    random = new Random(x)
  }
}

package producer {
  trait Model {
    def pfa: AnyRef
    def pfa(options: java.util.Map[String, AnyRef]): AnyRef
    def avroType: AvroType
    def avroType(options: java.util.Map[String, AnyRef]): AvroType
  }

  trait Dataset {
    def revert(): Unit
  }

  trait ModelRecord extends Model with Product

  trait Producer[D <: Dataset, M <: Model] {
    def dataset: D
    def optimize(): Unit
    def model: M
  }

  trait JsonToString {
    def toString(x: Any): String = x match {
      case null => "null"
      case true => "true"
      case false => "false"
      case java.lang.Boolean.TRUE => "true"
      case java.lang.Boolean.FALSE => "false"
      case y: Byte => y.toString
      case y: Short => y.toString
      case y: Char => y.toString
      case y: Int => y.toString
      case y: Long => y.toString
      case y: Float => y.toString
      case y: Double => y.toString
      case y: java.lang.Number => y.toString
      case y: String =>
        "\"" + y.
          replace("""\u0022""", """\"""").    // step 1: turn \ (0x5c) into \\
          replace("""\u0008""", """\b""").    // step 2: turn horizontal tab (0x09) into \t
          replace("""\u000c""", """\f""").    // step 3: turn carriage return (0x0d) into \r
          replace("""\u000a""", """\n""").    // step 4: turn newline (0x0a) into \n
          replace("""\u000d""", """\r""").    // step 5: turn formfeed (0x0c) into \f
          replace("""\u0009""", """\t""").    // step 6: turn backspace (0x08) into \b
          replace("""\u005c""", """\\""") +   // step 7: turn " (0x22) into \"
        "\""
      case y: java.util.List[_] =>
        "[" + (y map {z => toString(z)} mkString(", ")) + "]"
      case y: java.util.Map[_, _] =>
        "{" + (y.entrySet map {z: java.util.Map.Entry[_, _] => toString(z.getKey) + ": " + toString(z.getValue)} mkString(", ")) + "}"
      case y => throw new IllegalArgumentException(s"JsonArray/JsonMap should not contain objects of type ${y.getClass.getName}")
    }
    override def toString() = toString(this)
  }

  case class JsonArray[X](elems: X*) extends java.util.List[X] with JsonToString {
    def meta(otherKeys: Int => (String, _)*): JsonArray[JsonObject[_]] = JsonArray(((0 until size) map {i => elems(i) match {
      case x: JsonObject[_] => x.meta((otherKeys map {ok => ok(i)}): _*)
      case x => throw new IllegalArgumentException("JsonArray.meta can only be used if the array contains JsonObjects")
    }}): _*)
    def meta(otherKeys: PyFunction): JsonArray[JsonObject[_]] = JsonArray(((0 until size) map {i =>
      (elems(i), otherKeys.__call__(new PyInteger(i))) match {
        case (old: JsonObject[_], dict: PyDictionary) => JsonObject((old.elems ++ (dict.toSeq map {
          case (k: String, v: PyTuple) => (k, seqAsJavaList(v map {x => x}))
          case (k: String, v: PyList) => (k, seqAsJavaList(v map {x => x}))
          case (k: String, v) => (k, v)
          case _ => throw Py.TypeError(s"JsonArray.meta requires functions to produce dictionaries with string keys")
        })): _*)
        case _ => throw Py.TypeError(s"JsonArray.meta requires functions to produce dictionaries")
      }}): _*)

    // mutable java.util.List methods
    override def add(obj: X): Boolean = throw new UnsupportedOperationException
    override def add(index: Int, obj: X): Unit = throw new UnsupportedOperationException
    override def addAll(that: java.util.Collection[_ <: X]): Boolean = throw new UnsupportedOperationException
    override def addAll(index: Int, that: java.util.Collection[_ <: X]): Boolean = throw new UnsupportedOperationException
    override def clear(): Unit = throw new UnsupportedOperationException
    override def remove(index: Int): X = throw new UnsupportedOperationException
    override def remove(obj: AnyRef): Boolean = throw new UnsupportedOperationException
    override def removeAll(that: java.util.Collection[_]): Boolean = throw new UnsupportedOperationException
    override def retainAll(that: java.util.Collection[_]): Boolean = throw new UnsupportedOperationException
    override def set(index: Int, element: X): X = throw new UnsupportedOperationException

    // immutable java.util.List methods
    override def contains(obj: AnyRef): Boolean = elems contains obj
    override def containsAll(that: java.util.Collection[_]): Boolean = that forall {contains(_)}
    override def get(index: Int): X = elems(index)
    override def indexOf(obj: AnyRef): Int = elems indexOf obj
    override def isEmpty(): Boolean = elems.isEmpty
    override def iterator(): java.util.Iterator[X] = elems.iterator
    override def lastIndexOf(obj: AnyRef): Int = elems lastIndexOf obj
    override def listIterator(): java.util.ListIterator[X] = throw new UnsupportedOperationException
    override def listIterator(index: Int): java.util.ListIterator[X] = throw new UnsupportedOperationException
    override def size(): Int = elems.size
    override def subList(fromIndex: Int, toIndex: Int): java.util.List[X] = elems.slice(fromIndex, toIndex)
    override def toArray(): Array[AnyRef] = elems map {_.asInstanceOf[AnyRef]} toArray
    override def toArray[Y](y: Array[Y with AnyRef]): Array[Y with AnyRef] = toArray.asInstanceOf[Array[Y with AnyRef]]
  }

  case class JsonObject[X](elems: (String, X)*) extends java.util.Map[String, X] with JsonToString {
    def meta(otherKeys: (String, Any)*): JsonObject[_] = JsonObject((elems ++ otherKeys): _*)
    def meta(otherKeys: PyDictionary): JsonObject[_] = meta((otherKeys.toSeq map {
      case (k: String, v: PyTuple) => (k, seqAsJavaList(v map {x => x}))
      case (k: String, v: PyList) => (k, seqAsJavaList(v map {x => x}))
      case (k: String, v) => (k, v)
      case _ => throw Py.TypeError(s"JsonObject.meta requires a dictionary with string keys")}): _*)

    // mutable java.util.Map methods
    override def clear(): Unit = throw new UnsupportedOperationException
    override def put(key: String, value: X): X = throw new UnsupportedOperationException
    override def putAll(that: java.util.Map[_ <: String, _ <: X]): Unit = throw new UnsupportedOperationException
    override def remove(key: AnyRef): X = throw new UnsupportedOperationException

    //////////// immutable java.util.Map contract
    override def containsKey(key: AnyRef): Boolean = elems exists {_._1 == key}
    override def containsValue(value: AnyRef): Boolean = elems exists {_._2 == value}
    override def entrySet(): java.util.Set[java.util.Map.Entry[String, X]] = {
      val out = new java.util.HashSet[java.util.Map.Entry[String, X]](elems.size)
      for ((k, v) <- elems)
        out.add(new java.util.AbstractMap.SimpleEntry[String, X](k, v))
      out
    }
    override def get(key: AnyRef): X = key match {
      case i: String => elems find {_._1 == key} map {_._2} getOrElse {throw new NoSuchElementException("key not found: " + key.toString)}
      case _ => throw new NoSuchElementException("key not found: " + key.toString)
    }
    override def isEmpty(): Boolean = elems.isEmpty
    override def keySet(): java.util.Set[String] = setAsJavaSet(elems map {_._1} toSet)
    override def size(): Int = elems.size
    override def values(): java.util.Collection[X] = elems map {_._2}
  }

}
