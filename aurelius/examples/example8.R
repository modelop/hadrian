# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# 
# Licensed under the Hadrian Personal Use and Evaluation License (PUEL);
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://raw.githubusercontent.com/opendatagroup/hadrian/master/LICENSE
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

source("pfa-generator.R")

pfaDocument <- pfa.config(
    input = avro.record(list(one = avro.int, two = avro.double, three = avro.string), "Datum"),
    output = avro.string,
    cells = list(
        tree = pfa.cell(
            avro.record(list(
                field = avro.enum(c("one", "two", "three"), "TreeFields"),
                operator = avro.string,
                value = avro.union(avro.double, avro.string),
                pass = avro.union(avro.string, "TreeNode"),
                fail = avro.union(avro.string, "TreeNode")), "TreeNode"),
            unjson('{"field": "one",
                     "operator": "<",
                     "value": {"double": 12},
                     "pass": {"TreeNode": {
                              "field": "two",
                              "operator": ">",
                              "value": {"double": 3.5},
                              "pass": {"string": "yes-yes"},
                              "fail": {"string": "yes-no"}}},
                     "fail": {"TreeNode": {
                              "field": "three",
                              "operator": "==",
                              "value": {"string": "TEST"},
                              "pass": {"string": "no-yes"},
                              "fail": {"string": "no-no"}}}}'))),
    action = expression(
        model.tree.simpleWalk(input,
                              tree,
                              function(d = "Datum", t = "TreeNode" -> avro.boolean)
                                  model.tree.simpleTest(d, t))
        ))

engine <- pfa.engine(pfaDocument)
## > engine$action(unjson('{"one": 11, "two": 3.6, "three": "TEST"}'))
## [1] "yes-yes"
## > engine$action(unjson('{"one": 11, "two": 3.4, "three": "TEST"}'))
## [1] "yes-no"
## > engine$action(unjson('{"one": 13, "two": 3.6, "three": "TEST"}'))
## [1] "no-yes"
## > engine$action(unjson('{"one": 13, "two": 3.6, "three": "NOT-TEST"}'))
## [1] "no-no"
