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

import java.io.ByteArrayOutputStream

import org.codehaus.jackson.JsonNode
import org.codehaus.jackson.io.JsonStringEncoder
import org.codehaus.jackson.map.ObjectMapper

package object util {
  private val objectMapper = new ObjectMapper

  /** Generate an engine name like `"Engine_12"` by incrementing a counter (starting with 1).
    * 
    * This name is not ''guaranteed'' to be unique in some space, but this function will never generate the same name twice.
    */
  object uniqueEngineName extends Function0[String] {
    private var counter = 0
    /** Get a unique name.
      */
    def apply(): String = {
      counter = counter + 1
      "Engine_%d".format(counter)
    }
  }

  /** Generate a record type name like `"Record_12"` by incrementing a counter (starting with 1).
    * 
    * This name is not ''guaranteed'' to be unique in some space, but this function will never generate the same name twice.
    */
  object uniqueRecordName extends Function0[String] {
    private var counter = 0
    /** Get a unique name.
      */
    def apply(): String = {
      counter = counter + 1
      "Record_%d".format(counter)
    }
  }

  /** Generate an enum type name like `"Enum_12"` by incrementing a counter (starting with 1).
    * 
    * This name is not ''guaranteed'' to be unique in some space, but this function will never generate the same name twice.
    */
  object uniqueEnumName extends Function0[String] {
    private var counter = 0
    /** Get a unique name.
      */
    def apply(): String = {
      counter = counter + 1
      "Enum_%d".format(counter)
    }
  }

  /** Generate a fixed type name like `"Fixed_12"` by incrementing a counter (starting with 1).
    * 
    * This name is not ''guaranteed'' to be unique in some space, but this function will never generate the same name twice.
    */
  object uniqueFixedName extends Function0[String] {
    private var counter = 0
    /** Get a unique name.
      */
    def apply(): String = {
      counter = counter + 1
      "Fixed_%d".format(counter)
    }
  }

  /** Generate a position string from locator mark data and PFA path index.
    * 
    * Replaces `"%%"` with `"PERCENT-PERCENT"` and  `"%"` with `"PERCENT"` so that they can be used in format strings.
    */
  def pos(dot: String, at: String): String =
    "at " + at + (if (dot == "") "" else " (PFA field \"" + dot.replace("%%", "PERCENT-PERCENT").replace("%", "PERCENT") + "\")")

  /** Escapes string `x` so that it can be used in JSON.
    * 
    * Turns special characters such as `"` into escape sequences like `\"`, but ''does not'' add quotes around the whole string.
    */
  def escapeJson(x: String): String = new String(JsonStringEncoder.getInstance.quoteAsString(x))

  /** Unescapes string `json`, turning escape sequences like `\"` into `"` and removes quotes around the whole string.
    */
  object unescapeJson extends Function1[String, Option[String]] {
    /** @return `Some(result)` if successful; `None` otherwise
      */
    def apply(json: String): Option[String] =
      try {
        Some(objectMapper.readValue(json, classOf[String]))
      }
      catch {
        case exception: org.codehaus.jackson.map.JsonMappingException => None
      }
    def unapply(json: String): Option[String] = apply(json)
  }

  /** Convert `intermediate` object into a JSON string.
    */
  def convertToJson(intermediate: AnyRef): String = objectMapper.writeValueAsString(intermediate)
  /** Convert a JSON string into a Jackson node.
    */
  def convertFromJson(json: String): JsonNode = objectMapper.readTree(json)
}
