#!/usr/bin/env python

# Copyright (C) 2014  Open Data ("Open Data" refers to
# one or more of the following companies: Open Data Partners LLC,
# Open Data Research LLC, or Open Data Capital LLC.)
# 
# This file is part of Hadrian.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import unittest

from titus.genpy import PFAEngine

class TestTree(unittest.TestCase):
    def testSimpleTree(self):
        engine, = PFAEngine.fromPmml('''
<PMML version="4.2">
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
''')
        self.assertEqual(engine.action({"x": 0.9, "y": 0, "z": "hello"}), "leaf-1")
        self.assertEqual(engine.action({"x": 0.9, "y": 0, "z": "goodbye"}), "leaf-2")
        self.assertEqual(engine.action({"x": 1.1, "y": 0, "z": "hello"}), "leaf-3")
        self.assertEqual(engine.action({"x": 1.1, "y": 0, "z": "goodbye"}), "leaf-4")

#     def testDMGExample1(self):
#         engine, = PFAEngine.fromPmml('''
# <PMML xmlns="http://www.dmg.org/PMML-4_2" version="4.2">
#     <Header copyright="www.dmg.org" description="A very small binary tree model to show structure."/>
#     <DataDictionary numberOfFields="5">
#         <DataField name="temperature" optype="continuous" dataType="double"/>
#         <DataField name="humidity" optype="continuous" dataType="double"/>
#         <DataField name="windy" optype="categorical" dataType="string">
#             <Value value="true"/>
#             <Value value="false"/>
#         </DataField>
#         <DataField name="outlook" optype="categorical" dataType="string">
#             <Value value="sunny"/>
#             <Value value="overcast"/>
#             <Value value="rain"/>
#         </DataField>
#         <DataField name="whatIdo" optype="categorical" dataType="string">
#             <Value value="will play"/>
#             <Value value="may play"/>
#             <Value value="no play"/>
#         </DataField>
#     </DataDictionary>
#     <TreeModel modelName="golfing" functionName="classification">
#         <MiningSchema>
#             <MiningField name="temperature"/>
#             <MiningField name="humidity"/>
#             <MiningField name="windy"/>
#             <MiningField name="outlook"/>
#             <MiningField name="whatIdo" usageType="target"/>
#         </MiningSchema>
#         <Node score="will play">
#             <True/>
#             <Node score="will play">
#                 <SimplePredicate field="outlook" operator="equal" value="sunny"/>
#                 <Node score="will play">
#                     <CompoundPredicate booleanOperator="and">
#                         <SimplePredicate field="temperature" operator="lessThan" value="90"/>
#                         <SimplePredicate field="temperature" operator="greaterThan" value="50"/>
#                     </CompoundPredicate>
#                     <Node score="will play">
#                         <SimplePredicate field="humidity" operator="lessThan" value="80"/>
#                     </Node>
#                     <Node score="no play">
#                         <SimplePredicate field="humidity" operator="greaterOrEqual" value="80"/>
#                     </Node>
#                 </Node>
#                 <Node score="no play">
#                     <CompoundPredicate booleanOperator="or">
#                         <SimplePredicate field="temperature" operator="greaterOrEqual" value="90"/>
#                         <SimplePredicate field="temperature" operator="lessOrEqual" value="50"/>
#                     </CompoundPredicate>
#                 </Node>
#             </Node>
#             <Node score="may play">
#                 <CompoundPredicate booleanOperator="or">
#                     <SimplePredicate field="outlook" operator="equal" value="overcast"/>
#                     <SimplePredicate field="outlook" operator="equal" value="rain"/>
#                 </CompoundPredicate>
#                 <Node score="may play">
#                     <CompoundPredicate booleanOperator="and">
#                         <SimplePredicate field="temperature" operator="greaterThan" value="60"/>
#                         <SimplePredicate field="temperature" operator="lessThan" value="100"/>
#                         <SimplePredicate field="outlook" operator="equal" value="overcast"/>
#                         <SimplePredicate field="humidity" operator="lessThan" value="70"/>
#                         <SimplePredicate field="windy" operator="equal" value="false"/>
#                     </CompoundPredicate>
#                 </Node>
#                 <Node score="no play">
#                     <CompoundPredicate booleanOperator="and">
#                         <SimplePredicate field="outlook" operator="equal" value="rain"/>
#                         <SimplePredicate field="humidity" operator="lessThan" value="70"/>
#                     </CompoundPredicate>
#                 </Node>
#             </Node>
#         </Node>
#     </TreeModel>
# </PMML>
# ''')

#         print(engine)
