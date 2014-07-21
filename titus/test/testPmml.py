#!/usr/bin/env python

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

import json
import unittest

from titus.pmml.reader import pmmlToAst
from titus.reader import yamlToAst

class TestPMML(unittest.TestCase):
    def testEmpty(self):
        for version in "3.2", "4.0", "4.1", "4.2":
            self.assertEqual(
                yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: double}
output: "null"
action:
  - null
'''),
                pmmlToAst('''<PMML version="{version}">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="double" />
    </DataDictionary>
</PMML>
'''.format(version=version), {"engine.name": "test"}))

    def testBaselineZScore(self):
        self.assertEqual(
            yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: double}
output: double
action:
  - {"/": [{"-": [{attr: input, path: [[x]]}, 0.0]}, {m.sqrt: 1.0}]}
'''),
            pmmlToAst('''<PMML version="4.2">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="double" />
    </DataDictionary>
    <BaselineModel>
        <TestDistributions field="x" testStatistic="zValue">
            <Baseline>
                <GaussianDistribution mean="0" variance="1"/>
            </Baseline>
        </TestDistributions>
    </BaselineModel>
</PMML>
''', {"engine.name": "test"}))

    def testLocalTransformations(self):
        self.assertEqual(
            yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: double}
output: double
action:
  - {let: {y: {attr: input, path: [[x]]}}}
  - {"/": [{"-": [y, 0.0]}, {m.sqrt: 1.0}]}
'''),
            pmmlToAst('''<PMML version="4.2">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="double" />
    </DataDictionary>
    <BaselineModel>
        <LocalTransformations>
            <DerivedField name="y" optype="continuous" dataType="double">
                <FieldRef field="x"/>
            </DerivedField>
        </LocalTransformations>
        <TestDistributions field="y" testStatistic="zValue">
            <Baseline>
                <GaussianDistribution mean="0" variance="1"/>
            </Baseline>
        </TestDistributions>
    </BaselineModel>
</PMML>
''', {"engine.name": "test"}))

        self.assertEqual(
            yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: int}
output: double
action:
  - {let: {y: {upcast: {attr: input, path: [[x]]}, as: double}}}
  - {"/": [{"-": [y, 0.0]}, {m.sqrt: 1.0}]}
'''),
            pmmlToAst('''<PMML version="4.2">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="integer" />
    </DataDictionary>
    <BaselineModel>
        <LocalTransformations>
            <DerivedField name="y" optype="continuous" dataType="double">
                <FieldRef field="x"/>
            </DerivedField>
        </LocalTransformations>
        <TestDistributions field="y" testStatistic="zValue">
            <Baseline>
                <GaussianDistribution mean="0" variance="1"/>
            </Baseline>
        </TestDistributions>
    </BaselineModel>
</PMML>
''', {"engine.name": "test"}))

        self.assertEqual(
            yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: double}
output: double
action:
  - {let: {y: {attr: input, path: [[x]]}}}
  - {let: {z: y}}
  - {"/": [{"-": [z, 0.0]}, {m.sqrt: 1.0}]}
'''),
            pmmlToAst('''<PMML version="4.2">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="double" />
    </DataDictionary>
    <BaselineModel>
        <LocalTransformations>
            <DerivedField name="y" optype="continuous" dataType="double">
                <FieldRef field="x"/>
            </DerivedField>
            <DerivedField name="z" optype="continuous" dataType="double">
                <FieldRef field="y"/>
            </DerivedField>
        </LocalTransformations>
        <TestDistributions field="z" testStatistic="zValue">
            <Baseline>
                <GaussianDistribution mean="0" variance="1"/>
            </Baseline>
        </TestDistributions>
    </BaselineModel>
</PMML>
''', {"engine.name": "test"}))

    def testTransformationDictionary(self):
        self.assertEqual(
            yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: double}
output: double
action:
  - {let: {y: {attr: input, path: [[x]]}}}
  - {"/": [{"-": [y, 0.0]}, {m.sqrt: 1.0}]}
'''),
            pmmlToAst('''<PMML version="4.2">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="double" />
    </DataDictionary>
    <TransformationDictionary>
        <DerivedField name="y" optype="continuous" dataType="double">
            <FieldRef field="x"/>
        </DerivedField>
    </TransformationDictionary>
    <BaselineModel>
        <TestDistributions field="y" testStatistic="zValue">
            <Baseline>
                <GaussianDistribution mean="0" variance="1"/>
            </Baseline>
        </TestDistributions>
    </BaselineModel>
</PMML>
''', {"engine.name": "test"}))

        self.assertEqual(
            yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: int}
output: double
action:
  - {let: {y: {upcast: {attr: input, path: [[x]]}, as: double}}}
  - {"/": [{"-": [y, 0.0]}, {m.sqrt: 1.0}]}
'''),
            pmmlToAst('''<PMML version="4.2">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="integer" />
    </DataDictionary>
    <TransformationDictionary>
        <DerivedField name="y" optype="continuous" dataType="double">
            <FieldRef field="x"/>
        </DerivedField>
    </TransformationDictionary>
    <BaselineModel>
        <TestDistributions field="y" testStatistic="zValue">
            <Baseline>
                <GaussianDistribution mean="0" variance="1"/>
            </Baseline>
        </TestDistributions>
    </BaselineModel>
</PMML>
''', {"engine.name": "test"}))

        self.assertEqual(
            yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: double}
output: double
action:
  - {let: {y: {attr: input, path: [[x]]}}}
  - {let: {z: y}}
  - {"/": [{"-": [z, 0.0]}, {m.sqrt: 1.0}]}
'''),
            pmmlToAst('''<PMML version="4.2">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="double" />
    </DataDictionary>
    <TransformationDictionary>
        <DerivedField name="y" optype="continuous" dataType="double">
            <FieldRef field="x"/>
        </DerivedField>
        <DerivedField name="z" optype="continuous" dataType="double">
            <FieldRef field="y"/>
        </DerivedField>
    </TransformationDictionary>
    <BaselineModel>
        <TestDistributions field="z" testStatistic="zValue">
            <Baseline>
                <GaussianDistribution mean="0" variance="1"/>
            </Baseline>
        </TestDistributions>
    </BaselineModel>
</PMML>
''', {"engine.name": "test"}))

    def testConstant(self):
        self.assertEqual(
            yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: double}
output: double
action:
  - {let: {y: 12}}
  - {"/": [{"-": [y, 0.0]}, {m.sqrt: 1.0}]}
'''),
            pmmlToAst('''<PMML version="4.2">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="double" />
    </DataDictionary>
    <BaselineModel>
        <LocalTransformations>
            <DerivedField name="y" optype="continuous" dataType="integer">
                <Constant dataType="integer">12</Constant>
            </DerivedField>
        </LocalTransformations>
        <TestDistributions field="y" testStatistic="zValue">
            <Baseline>
                <GaussianDistribution mean="0" variance="1"/>
            </Baseline>
        </TestDistributions>
    </BaselineModel>
</PMML>
''', {"engine.name": "test"}))

    def testDefineFunction(self):
        self.assertEqual(
            yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: double}
output: double
action:
  - {"/": [{"-": [{attr: input, path: [[x]]}, 0.0]}, {m.sqrt: 1.0}]}
fcns:
  identity:
    params: [{x: double}]
    ret: double
    do: x
'''),
            pmmlToAst('''<PMML version="4.2">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="double" />
    </DataDictionary>
    <TransformationDictionary>
        <DefineFunction name="identity">
            <ParameterField name="x" dataType="double"/>
            <FieldRef field="x"/>
        </DefineFunction>
    </TransformationDictionary>
    <BaselineModel>
        <TestDistributions field="x" testStatistic="zValue">
            <Baseline>
                <GaussianDistribution mean="0" variance="1"/>
            </Baseline>
        </TestDistributions>
    </BaselineModel>
</PMML>
''', {"engine.name": "test"}))

    def testApply(self):
        self.assertEqual(
            yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: double}
output: double
action:
  - {let: {y: {u.identity: {attr: input, path: [[x]]}}}}
  - {"/": [{"-": [y, 0.0]}, {m.sqrt: 1.0}]}
fcns:
  identity:
    params: [{x: double}]
    ret: double
    do: x
'''),
            pmmlToAst('''<PMML version="4.2">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="double" />
    </DataDictionary>
    <TransformationDictionary>
        <DefineFunction name="identity">
            <ParameterField name="x" dataType="double"/>
            <FieldRef field="x"/>
        </DefineFunction>
    </TransformationDictionary>
    <BaselineModel>
        <LocalTransformations>
            <DerivedField name="y" optype="continuous" dataType="double">
                <Apply function="identity">
                    <FieldRef field="x"/>
                </Apply>
            </DerivedField>
        </LocalTransformations>
        <TestDistributions field="y" testStatistic="zValue">
            <Baseline>
                <GaussianDistribution mean="0" variance="1"/>
            </Baseline>
        </TestDistributions>
    </BaselineModel>
</PMML>
''', {"engine.name": "test"}))

    def testApplyMin2(self):
        self.assertEqual(
            yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: double}
output: double
action:
  - {let: {y: {min: [{attr: input, path: [[x]]}, 0.0]}}}
  - {"/": [{"-": [y, 0.0]}, {m.sqrt: 1.0}]}
'''),
            pmmlToAst('''<PMML version="4.2">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="double" />
    </DataDictionary>
    <BaselineModel>
        <LocalTransformations>
            <DerivedField name="y" optype="continuous" dataType="double">
                <Apply function="min">
                    <FieldRef field="x"/>
                    <Constant dataType="double">0</Constant>
                </Apply>
            </DerivedField>
        </LocalTransformations>
        <TestDistributions field="y" testStatistic="zValue">
            <Baseline>
                <GaussianDistribution mean="0" variance="1"/>
            </Baseline>
        </TestDistributions>
    </BaselineModel>
</PMML>
''', {"engine.name": "test"}))

    def testApplyMin3(self):
        self.assertEqual(
            yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: double}
output: double
action:
  - {let: {y: {a.min: {new: [{attr: input, path: [[x]]}, 0.0, 1], type: {type: array, items: double}}}}}
  - {"/": [{"-": [y, 0.0]}, {m.sqrt: 1.0}]}
'''),
            pmmlToAst('''<PMML version="4.2">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="double" />
    </DataDictionary>
    <BaselineModel>
        <LocalTransformations>
            <DerivedField name="y" optype="continuous" dataType="double">
                <Apply function="min">
                    <FieldRef field="x"/>
                    <Constant dataType="double">0</Constant>
                    <Constant dataType="integer">1</Constant>
                </Apply>
            </DerivedField>
        </LocalTransformations>
        <TestDistributions field="y" testStatistic="zValue">
            <Baseline>
                <GaussianDistribution mean="0" variance="1"/>
            </Baseline>
        </TestDistributions>
    </BaselineModel>
</PMML>
''', {"engine.name": "test"}))

    def testTreeModel(self):
        self.assertEqual(
            yamlToAst('''
name: test
input:
  type: record
  name: DataDictionary
  fields:
    - {name: x, type: double}
    - {name: y, type: int}
    - {name: z, type: string}
output: string
action:
  - model.tree.simpleWalk:
      - input
      - cell: modelData
      - params: [{d: DataDictionary}, {t: TreeNode}]
        ret: boolean
        do: {model.tree.simpleTest: [d, t]}
cells:
  modelData:
    type:
      type: record
      name: TreeNode
      fields:
        - name: field
          type:
            type: enum
            name: TreeFields
            symbols: [x, y, z]
        - {name: operator, type: string}
        - {name: value, type: [double, int, string]}
        - {name: pass, type: [TreeNode, string]}
        - {name: fail, type: [TreeNode, string]}
    init:
      TreeNode:
        field: x
        operator: "<"
        value: {double: 1}
        pass:
          TreeNode:
            field: z
            operator: "=="
            value: {string: hello}
            pass: {string: leaf-1}
            fail: {string: leaf-2}
        fail:
          TreeNode:
            field: z
            operator: "=="
            value: {string: hello}
            pass: {string: leaf-3}
            fail: {string: leaf-4}
'''),
            pmmlToAst('''<PMML version="4.2">
    <Header copyright=""/>
    <DataDictionary>
        <DataField name="x" optype="continuous" dataType="double" />
        <DataField name="y" optype="continuous" dataType="integer" />
        <DataField name="z" optype="categorical" dataType="string" />
    </DataDictionary>
    <TreeModel functionName="categorical" splitCharacteristic="binarySplit">
        <Node>
            <True/>
            <Node>
                <SimplePredicate field="x" operator="lessThan" value="1"/>
                <Node score="leaf-1">
                    <SimplePredicate field="z" operator="equal" value="hello"/>
                </Node>
                <Node score="leaf-2">
                    <SimplePredicate field="z" operator="notEqual" value="hello"/>
                </Node>
            </Node>
            <Node>
                <SimplePredicate field="x" operator="greaterOrEqual" value="1"/>
                <Node score="leaf-3">
                    <SimplePredicate field="z" operator="equal" value="hello"/>
                </Node>
                <Node score="leaf-4">
                    <SimplePredicate field="z" operator="notEqual" value="hello"/>
                </Node>
            </Node>
        </Node>
    </TreeModel>
</PMML>
''', {"engine.name": "test"}))



if __name__ == "__main__":
    unittest.main()
