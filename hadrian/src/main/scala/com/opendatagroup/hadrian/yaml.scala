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

import java.util.Scanner

import scala.collection.JavaConversions._

import org.yaml.snakeyaml.constructor.AbstractConstruct
import org.yaml.snakeyaml.constructor.Construct
import org.yaml.snakeyaml.constructor.SafeConstructor
import org.yaml.snakeyaml.error.YAMLException
import org.yaml.snakeyaml.nodes.MappingNode
import org.yaml.snakeyaml.nodes.Node
import org.yaml.snakeyaml.nodes.ScalarNode
import org.yaml.snakeyaml.nodes.SequenceNode
import org.yaml.snakeyaml.nodes.Tag
import org.yaml.snakeyaml.Yaml

import com.opendatagroup.hadrian.ast.EngineConfig
import com.opendatagroup.hadrian.reader.jsonToAst
import com.opendatagroup.hadrian.util.convertToJson

package yaml {
  object yamlToJson extends Function1[String, String] {
    private val safeConstructor = new SafeConstructor

    private def yaml: Yaml = new Yaml(new ConstructorForJsonConversion)

    def apply(in: java.io.InputStream): String = convertToJson(yaml.load(in))
    def apply(in: java.io.Reader): String = convertToJson(yaml.load(in))
    def apply(in: String): String = convertToJson(yaml.load(in))

    private class ConstructorForJsonConversion extends SafeConstructor {
      this.yamlConstructors.put(Tag.BINARY, new MyConstructYamlBinary)
      this.yamlConstructors.put(Tag.TIMESTAMP, new safeConstructor.ConstructYamlStr)
      this.yamlConstructors.put(Tag.OMAP, SafeConstructor.undefinedConstructor)
      this.yamlConstructors.put(Tag.PAIRS, SafeConstructor.undefinedConstructor)
      this.yamlConstructors.put(Tag.SET, SafeConstructor.undefinedConstructor)
      this.yamlConstructors.put(Tag.SEQ, new MyConstructYamlSeq)
      this.yamlConstructors.put(Tag.MAP, new MyConstructYamlMap)

      class NoPosConstructYaml extends AbstractConstruct {
        val constructYamlNull = new safeConstructor.ConstructYamlNull
        val constructYamlBool = new safeConstructor.ConstructYamlBool
        val constructYamlInt = new safeConstructor.ConstructYamlInt
        val constructYamlFloat = new safeConstructor.ConstructYamlFloat
        val constructYamlStr = new safeConstructor.ConstructYamlStr
        val constructYamlSeq = new safeConstructor.ConstructYamlSeq
        val constructYamlMap = new safeConstructor.ConstructYamlMap

        override def construct(node: Node): AnyRef = node match {
          case _: ScalarNode => node.getTag match {
            case Tag.NULL => constructYamlNull.construct(node)
            case Tag.BOOL => println("bool", node); constructYamlBool.construct(node)
            case Tag.INT => constructYamlInt.construct(node)
            case Tag.FLOAT => constructYamlFloat.construct(node)
            case Tag.STR => println("str", node); constructYamlStr.construct(node)
            case _ => throw new YAMLException("Node uses features not allowed in JSON: " + node)
          }
          case _: MappingNode => constructYamlMap.construct(node)
          case _: SequenceNode => constructYamlSeq.construct(node)
          case _ => throw new YAMLException("Node uses features not allowed in JSON: " + node)
        }
      }

      class MyConstructYamlBinary extends safeConstructor.ConstructYamlBinary {
        override def construct(node: Node): AnyRef = new String(super.construct(node).asInstanceOf[Array[Byte]])
      }

      class MyConstructYamlMap extends Construct {
        override def construct(node: Node): AnyRef = {
          var out =
            if (node.isTwoStepsConstruction)
              createDefaultMap
            else
              constructMapping(node.asInstanceOf[MappingNode])

          if (!out.containsKey("@")) {
            val at =
              if (node.getStartMark.getLine == node.getEndMark.getLine)
                "YAML line %d".format(node.getStartMark.getLine + 1)
              else
                "YAML lines %d to %d".format(node.getStartMark.getLine + 1, node.getEndMark.getLine + 1)

            val out2 = new java.util.LinkedHashMap[AnyRef, AnyRef](out.size + 1)
            out2.put("@", at)
            for ((k, v) <- out)
              out2.put(k, v)
            out = out2
          }
          out
        }
        override def construct2ndStep(node: Node, obj: AnyRef): Unit = {
          if (node.isTwoStepsConstruction)
            constructMapping2ndStep(node.asInstanceOf[MappingNode], obj.asInstanceOf[java.util.Map[AnyRef, AnyRef]])
          else
            throw new YAMLException("Unexpected recursive mapping structure. Node: " + node)
        }
      }

      class MyConstructYamlSeq extends Construct {
        override def construct(node: Node): AnyRef = {
          val seqNode = node.asInstanceOf[SequenceNode]
          if (node.isTwoStepsConstruction)
            createDefaultList(seqNode.getValue.size)
          else
            constructSequence(seqNode)
        }
        override def construct2ndStep(node: Node, data: AnyRef): Unit = {
          if (node.isTwoStepsConstruction)
            constructSequenceStep2(node.asInstanceOf[SequenceNode], data.asInstanceOf[java.util.List[AnyRef]])
          else
            throw new YAMLException("Unexpected recursive sequence structure. Node: " + node)
        }
      }
    }
  }

  object yamlToAst extends Function1[String, EngineConfig] {
    def apply(src: java.io.File): EngineConfig =
      jsonToAst(yamlToJson(new Scanner(src).useDelimiter("\\A").next()))

    def apply(src: java.io.InputStream): EngineConfig =
      jsonToAst(yamlToJson(new Scanner(src).useDelimiter("\\A").next()))

    def apply(src: String): EngineConfig = jsonToAst(yamlToJson(src))
  }
}
