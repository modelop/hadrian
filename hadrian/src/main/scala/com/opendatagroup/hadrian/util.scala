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

  object uniqueEngineName extends Function0[String] {
    private var counter = 0
    def apply(): String = {
      counter = counter + 1
      "Engine_%d".format(counter)
    }
  }

  object uniqueRecordName extends Function0[String] {
    private var counter = 0
    def apply(): String = {
      counter = counter + 1
      "Record_%d".format(counter)
    }
  }

  object uniqueEnumName extends Function0[String] {
    private var counter = 0
    def apply(): String = {
      counter = counter + 1
      "Enum_%d".format(counter)
    }
  }

  object uniqueFixedName extends Function0[String] {
    private var counter = 0
    def apply(): String = {
      counter = counter + 1
      "Fixed_%d".format(counter)
    }
  }

  def pos(dot: String, at: String): String =
    "in%s object from %s".format(if (dot == "") "" else " field " + dot + " of", at)

  def escapeJson(x: String): String = new String(JsonStringEncoder.getInstance.quoteAsString(x))

  object unescapeJson extends Function1[String, Option[String]] {
    def apply(json: String): Option[String] =
      try {
        Some(objectMapper.readValue(json, classOf[String]))
      }
      catch {
        case exception: org.codehaus.jackson.map.JsonMappingException => None
      }
    def unapply(json: String): Option[String] = apply(json)
  }

  def convertToJson(intermediate: AnyRef): String = objectMapper.writeValueAsString(intermediate)
  def convertFromJson(json: String): JsonNode = objectMapper.readTree(json)
}
