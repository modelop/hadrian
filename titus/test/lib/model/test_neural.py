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

import unittest

from titus.genpy import PFAEngine
from titus.errors import *

class TestLib1ModelNeural(unittest.TestCase):
    def testRegNeural(self):
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  model:
    type:
      type: array
      items:
        type: record
        name: layer
        fields:
          - {name: weights, type: {type: array, items: {type: array, items: double}}}
          - {name: bias,    type: {type: array, items: double}}
    init:
      - {weights: [[ -6.0,  -8.0],
                   [-25.0, -30.0]],
         bias:     [  4.0,  50.0]}
      - {weights: [[-12.0,  30.0]],
         bias:     [-25.0]}
action:
   m.link.logit:
     model.neural.simpleLayers:
       - input
       - cell: model
       - params: [{x: double}]
         ret: double
         do: {m.link.logit: [x]}
""")
        self.assertAlmostEqual(engine.action([0.0, 0.0])[0], 0.0, places=1)
        self.assertAlmostEqual(engine.action([1.0, 0.0])[0], 1.0, places=1)
        self.assertAlmostEqual(engine.action([0.0, 1.0])[0], 1.0, places=1)
        self.assertAlmostEqual(engine.action([1.0, 1.0])[0], 0.0, places=1)







#     def testRegNeural(self):
#         engine, = PFAEngine.fromYaml("""
# input: {type: array, items: double}
# output: double
# cells:
#   model:
#     type:
#       type: record
#       name: layers
#       fields:
#         - {name: weights,    type: {type: array, items: {type: array, items: {type: array, items: double}}}}
#         - {name: bias,       type: {type: array, items: {type: array, items: double}}}
#     init:
#       weights: [ [[ -6.0,  -8.0],
#                   [-25.0, -30.0]],
#                  [[-12.0,  30.0]] ]
#       bias:    [  [  4.0, 50.0],
#                   [-25.0] ]
# action:
#   attr:
#      model.neural.simpleLayers:
#        - input
#        - cell: model
#        - params: [{x: double}]
#          ret: double
#          do: {model.reg.norm.logit: [x]}
# 
#   path: [[id]]
# """)
#         self.assertAlmostEqual(engine.action([0.1, 0.2, 0.3, 0.4, 0.5]), 103.9, places=1)




