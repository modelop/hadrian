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

import unittest

from titus.genpy import PFAEngine
from titus.errors import *

class TestLib1ModelReg(unittest.TestCase):
    def testRegLinear(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: array, items: double}}
        - {name: const, type: double}
    init:
      coeff: [1, 2, 3, 0, 5]
      const: 100.0
action:
  model.reg.linear:
    - input
    - cell: model
''')
        self.assertAlmostEqual(engine.action([0.1, 0.2, 0.3, 0.4, 0.5]), 103.9, places=1)

        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: array, items: {type: array, items: double}}}
        - {name: const, type: {type: array, items: double}}
    init:
      coeff: [[1, 2, 3, 0, 5],
              [1, 1, 1, 1, 1],
              [0, 0, 0, 0, 1]]
      const: [0.0, 0.0, 100.0]
action:
  model.reg.linear:
    - input
    - cell: model
''')
        out = engine.action([0.1, 0.2, 0.3, 0.4, 0.5])
        self.assertAlmostEqual(out[0], 3.9,   places=1)
        self.assertAlmostEqual(out[1], 1.5,   places=1)
        self.assertAlmostEqual(out[2], 100.5, places=1)

        engine, = PFAEngine.fromYaml('''
input: {type: map, values: double}
output: double
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: map, values: double}}
        - {name: const, type: double}
    init:
      coeff: {one: 1, two: 2, three: 3, four: 0, five: 5}
      const: 100.0
action:
  model.reg.linear:
    - input
    - cell: model
''')
        self.assertAlmostEqual(engine.action({"one": 0.1, "two": 0.2, "three": 0.3, "four": 0.4, "five": 0.5}), 103.9, places = 5)

        engine, = PFAEngine.fromYaml('''
input: {type: map, values: double}
output: {type: map, values: double}
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: coeff, type: {type: map, values: {type: map, values: double}}}
        - {name: const, type: {type: map, values: double}}
    init:
      coeff:
        uno: {one: 1, two: 2, three: 3, four: 0, five: 5}
        dos: {one: 1, two: 1, three: 1, four: 1, five: 1}
        tres: {one: 0, two: 0, three: 0, four: 0, five: 1}
      const:
        {uno: 0.0, dos: 0.0, tres: 100.0}
action:
  model.reg.linear:
    - input
    - cell: model
''')
        out = engine.action({"one": 0.1, "two": 0.2, "three": 0.3, "four": 0.4, "five": 0.5})
        self.assertAlmostEqual(out["uno"],  3.9,   places=1)
        self.assertAlmostEqual(out["dos"],  1.5,   places=1)
        self.assertAlmostEqual(out["tres"], 100.5, places=1)

    def testRegLinearVariance1(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: covar, type: {type: array, items: {type: array, items: double}}}
    init:
      covar: [[ 1.0, -0.1, 0.0],
              [-0.1,  2.0, 0.0],
              [ 0.0,  0.0, 0.0]]
action:
  model.reg.linearVariance:
    - input
    - cell: model
''')
        self.assertAlmostEqual(engine.action([0.1, 0.2]), 0.086, places=3)

    def testRegLinearVariance2(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: covar, type: {type: array, items: {type: array, items: {type: array, items: double}}}}
    init:
      covar:
        - [[ 1.0, -0.1, 0.0],
           [-0.1,  2.0, 0.0],
           [ 0.0,  0.0, 0.0]]
        - [[ 1.0, -0.1, 0.0],
           [-0.1,  2.0, 0.0],
           [ 0.0,  0.0, 0.0]]
        - [[ 1.0, -0.1, 0.0],
           [-0.1,  2.0, 0.0],
           [ 0.0,  0.0, 0.0]]
        - [[ 1.0, -0.1, 0.0],
           [-0.1,  2.0, 0.0],
           [ 0.0,  0.0, 0.0]]
action:
  model.reg.linearVariance:
    - input
    - cell: model
''')
        results = engine.action([0.1, 0.2])
        self.assertAlmostEqual(results[0], 0.086, places=3)
        self.assertAlmostEqual(results[1], 0.086, places=3)
        self.assertAlmostEqual(results[2], 0.086, places=3)

    def testRegLinearVariance3(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: double}
output: double
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: covar, type: {type: map, values: {type: map, values: double}}}
    init:
      covar: {a: {a:  1.0, b: -0.1, "": 0.0},
              b: {a: -0.1, b:  2.0, "": 0.0},
             "": {a:  0.0, b:  0.0, "": 0.0}}
action:
  model.reg.linearVariance:
    - input
    - cell: model
''')
        self.assertAlmostEqual(engine.action({"a": 0.1, "b": 0.2}), 0.086, places=3)

    def testRegLinearVariance4(self):
        engine, = PFAEngine.fromYaml('''
input: {type: map, values: double}
output: {type: map, values: double}
cells:
  model:
    type:
      type: record
      name: Model
      fields:
        - {name: covar, type: {type: map, values: {type: map, values: {type: map, values: double}}}}
    init:
      covar:
        one: {a: {a:  1.0, b: -0.1, "": 0.0},
              b: {a: -0.1, b:  2.0, "": 0.0},
             "": {a:  0.0, b:  0.0, "": 0.0}}
        two: {a: {a:  1.0, b: -0.1, "": 0.0},
              b: {a: -0.1, b:  2.0, "": 0.0},
             "": {a:  0.0, b:  0.0, "": 0.0}}
        three: {a: {a:  1.0, b: -0.1, "": 0.0},
                b: {a: -0.1, b:  2.0, "": 0.0},
               "": {a:  0.0, b:  0.0, "": 0.0}}
        four: {a: {a:  1.0, b: -0.1, "": 0.0},
               b: {a: -0.1, b:  2.0, "": 0.0},
              "": {a:  0.0, b:  0.0, "": 0.0}}
action:
  model.reg.linearVariance:
    - input
    - cell: model
''')
        results = engine.action({"a": 0.1, "b": 0.2})
        self.assertAlmostEqual(results["one"], 0.086, places=3)
        self.assertAlmostEqual(results["two"], 0.086, places=3)
        self.assertAlmostEqual(results["three"], 0.086, places=3)

    def testGaussianProcess1(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: GP
        fields:
          - {name: x, type: double}
          - {name: to, type: double}
          - {name: sigma, type: double}
    init:
      - {x:   0, to: -0.3346332030, sigma: 0.2}
      - {x:  10, to: -0.0343383864, sigma: 0.2}
      - {x:  20, to: -0.0276927905, sigma: 0.2}
      - {x:  30, to: 0.05708694575, sigma: 0.2}
      - {x:  40, to: 0.66909595875, sigma: 0.2}
      - {x:  50, to: 0.57458517677, sigma: 0.2}
      - {x:  60, to: 0.63100196978, sigma: 0.2}
      - {x:  70, to: 0.91841243688, sigma: 0.2}
      - {x:  80, to: 0.65081764341, sigma: 0.2}
      - {x:  90, to: 0.71978591756, sigma: 0.2}
      - {x: 100, to: 0.93481331323, sigma: 0.2}
      - {x: 110, to: 0.84831977376, sigma: 0.2}
      - {x: 120, to: 0.73970609648, sigma: 0.2}
      - {x: 130, to: 0.78029917594, sigma: 0.2}
      - {x: 140, to: 0.65909346778, sigma: 0.2}
      - {x: 150, to: 0.47746829475, sigma: 0.2}
      - {x: 160, to: 0.15788020690, sigma: 0.2}
      - {x: 170, to: -0.0417263190, sigma: 0.2}
      - {x: 180, to: 0.03949032925, sigma: 0.2}
      - {x: 190, to: -0.3433432642, sigma: 0.2}
      - {x: 200, to: -0.0254098681, sigma: 0.2}
      - {x: 210, to: -0.6289059981, sigma: 0.2}
      - {x: 220, to: -0.7431731071, sigma: 0.2}
      - {x: 230, to: -0.4354207032, sigma: 0.2}
      - {x: 240, to: -1.0959618089, sigma: 0.2}
      - {x: 250, to: -0.6671072982, sigma: 0.2}
      - {x: 260, to: -0.9050596147, sigma: 0.2}
      - {x: 270, to: -1.2019606762, sigma: 0.2}
      - {x: 280, to: -1.1191287449, sigma: 0.2}
      - {x: 290, to: -1.1299689439, sigma: 0.2}
      - {x: 300, to: -0.5776687178, sigma: 0.2}
      - {x: 310, to: -1.0480428012, sigma: 0.2}
      - {x: 320, to: -0.6461742204, sigma: 0.2}
      - {x: 330, to: -0.5866474699, sigma: 0.2}
      - {x: 340, to: -0.3117119198, sigma: 0.2}
      - {x: 350, to: -0.2478194617, sigma: 0.2}
action:
  model.reg.gaussianProcess:
    - input          # find the best fit to the input x

    - {cell: table}  # use the provided table of training data

    - null           # no explicit krigingWeight: universal Kriging

    - fcn: m.kernel.rbf    # radial basis function (squared exponential)
      fill: {gamma: 2.0}   # with a given gamma (by partial application)
                           # can be replaced with any function,
                           # from the m.kernel.* library or user-defined
''')
        self.assertAlmostEqual(engine.action(  5.0), 0.03087429165, places=3)
        self.assertAlmostEqual(engine.action( 15.0), 0.17652676226, places=3)
        self.assertAlmostEqual(engine.action( 25.0), 0.32346820507, places=3)
        self.assertAlmostEqual(engine.action( 35.0), 0.46100243449, places=3)
        self.assertAlmostEqual(engine.action( 45.0), 0.58099507734, places=3)
        self.assertAlmostEqual(engine.action( 55.0), 0.67895250259, places=3)
        self.assertAlmostEqual(engine.action( 65.0), 0.75393498012, places=3)
        self.assertAlmostEqual(engine.action( 75.0), 0.80736647814, places=3)
        self.assertAlmostEqual(engine.action( 85.0), 0.84119814623, places=3)
        self.assertAlmostEqual(engine.action( 95.0), 0.85612291938, places=3)
        self.assertAlmostEqual(engine.action(105.0), 0.85052616975, places=3)
        self.assertAlmostEqual(engine.action(115.0), 0.82058975268, places=3)
        self.assertAlmostEqual(engine.action(125.0), 0.76154114462, places=3)
        self.assertAlmostEqual(engine.action(135.0), 0.66961346549, places=3)
        self.assertAlmostEqual(engine.action(145.0), 0.54401256459, places=3)
        self.assertAlmostEqual(engine.action(155.0), 0.38817025604, places=3)
        self.assertAlmostEqual(engine.action(165.0), 0.20980133234, places=3)
        self.assertAlmostEqual(engine.action(175.0), 0.01968958089, places=3)
        self.assertAlmostEqual(engine.action(185.0), -0.1704373229, places=3)
        self.assertAlmostEqual(engine.action(195.0), -0.3502702450, places=3)
        self.assertAlmostEqual(engine.action(205.0), -0.5127572323, places=3)
        self.assertAlmostEqual(engine.action(215.0), -0.6548281470, places=3)
        self.assertAlmostEqual(engine.action(225.0), -0.7767913542, places=3)
        self.assertAlmostEqual(engine.action(235.0), -0.8806473549, places=3)
        self.assertAlmostEqual(engine.action(245.0), -0.9679493266, places=3)
        self.assertAlmostEqual(engine.action(255.0), -1.0380130690, places=3)
        self.assertAlmostEqual(engine.action(265.0), -1.0871683887, places=3)
        self.assertAlmostEqual(engine.action(275.0), -1.1093800549, places=3)
        self.assertAlmostEqual(engine.action(285.0), -1.0980749198, places=3)
        self.assertAlmostEqual(engine.action(295.0), -1.0485714839, places=3)
        self.assertAlmostEqual(engine.action(305.0), -0.9602825140, places=3)
        self.assertAlmostEqual(engine.action(315.0), -0.8379324438, places=3)
        self.assertAlmostEqual(engine.action(325.0), -0.6913668676, places=3)
        self.assertAlmostEqual(engine.action(335.0), -0.5339997309, places=3)
        self.assertAlmostEqual(engine.action(345.0), -0.3803701293, places=3)
        self.assertAlmostEqual(engine.action(355.0), -0.2435189466, places=3)

    def testGaussianProcess2(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: {type: array, items: double}
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: GP
        fields:
          - {name: x, type: double}
          - {name: to, type: {type: array, items: double}}
          - {name: sigma, type: {type: array, items: double}}
    init:
      - {x:   0, to: [-0.0275638306327, 1.6436104074682], sigma: [0.2, 0.2]}
      - {x:  10, to: [-0.0550590156488, 1.1279026778761], sigma: [0.2, 0.2]}
      - {x:  20, to: [0.27665811014276, 1.2884952019673], sigma: [0.2, 0.2]}
      - {x:  30, to: [0.32564933012538, 0.6975167314472], sigma: [0.2, 0.2]}
      - {x:  40, to: [0.50951585410170, 0.5366404828626], sigma: [0.2, 0.2]}
      - {x:  50, to: [0.78970794409845, 0.5753573687864], sigma: [0.2, 0.2]}
      - {x:  60, to: [0.79560759832648, 0.8669490726924], sigma: [0.2, 0.2]}
      - {x:  70, to: [1.11012632091040, 0.2893283390564], sigma: [0.2, 0.2]}
      - {x:  80, to: [1.01101991793607, 0.1168159075340], sigma: [0.2, 0.2]}
      - {x:  90, to: [0.89167196367050, 0.2336483742367], sigma: [0.2, 0.2]}
      - {x: 100, to: [0.79669701754334, -0.262415331320], sigma: [0.2, 0.2]}
      - {x: 110, to: [0.73478042254427, -0.269257044570], sigma: [0.2, 0.2]}
      - {x: 120, to: [0.54225961573755, -0.528524392539], sigma: [0.2, 0.2]}
      - {x: 130, to: [0.63387009124588, -0.550031870271], sigma: [0.2, 0.2]}
      - {x: 140, to: [0.53868855884699, -0.756608403729], sigma: [0.2, 0.2]}
      - {x: 150, to: [0.52440311808591, -0.764908616789], sigma: [0.2, 0.2]}
      - {x: 160, to: [0.38234791058889, -0.755332319548], sigma: [0.2, 0.2]}
      - {x: 170, to: [0.06408032993876, -1.208343893027], sigma: [0.2, 0.2]}
      - {x: 180, to: [-0.1251140497492, -1.008797566375], sigma: [0.2, 0.2]}
      - {x: 190, to: [-0.6622773320724, -0.735977078508], sigma: [0.2, 0.2]}
      - {x: 200, to: [-0.5060071246967, -1.131959607514], sigma: [0.2, 0.2]}
      - {x: 210, to: [-0.7506697169187, -0.933266228609], sigma: [0.2, 0.2]}
      - {x: 220, to: [-0.6114675918420, -1.115429627986], sigma: [0.2, 0.2]}
      - {x: 230, to: [-0.7393428452701, -0.644829102596], sigma: [0.2, 0.2]}
      - {x: 240, to: [-1.1005820484414, -0.602487247649], sigma: [0.2, 0.2]}
      - {x: 250, to: [-0.9199172336156, -0.445415709796], sigma: [0.2, 0.2]}
      - {x: 260, to: [-0.5548384390502, -0.130872144887], sigma: [0.2, 0.2]}
      - {x: 270, to: [-1.1663758959153, 0.0403022656204], sigma: [0.2, 0.2]}
      - {x: 280, to: [-1.3683792108867, -0.055259795527], sigma: [0.2, 0.2]}
      - {x: 290, to: [-1.0373014259785, 0.1923335805121], sigma: [0.2, 0.2]}
      - {x: 300, to: [-0.8539507289822, 0.6473186579626], sigma: [0.2, 0.2]}
      - {x: 310, to: [-1.1658738130819, 0.7019580213786], sigma: [0.2, 0.2]}
      - {x: 320, to: [-0.3248586082577, 0.5924413605916], sigma: [0.2, 0.2]}
      - {x: 330, to: [-0.4246629811006, 0.7436475098601], sigma: [0.2, 0.2]}
      - {x: 340, to: [-0.2888893157821, 0.9129729112785], sigma: [0.2, 0.2]}
      - {x: 350, to: [0.16414946814559, 1.1171102512988], sigma: [0.2, 0.2]}
action:
  model.reg.gaussianProcess:
    - input          # find the best fit to the input x

    - {cell: table}  # use the provided table of training data

    - null           # no explicit krigingWeight: universal Kriging

    - fcn: m.kernel.rbf    # radial basis function (squared exponential)
      fill: {gamma: 2.0}   # with a given gamma (by partial application)
                           # can be replaced with any function,
                           # from the m.kernel.* library or user-defined
''')
        for xi, yi in zip(engine.action(  5.0), [0.050270763255514, 1.4492626857937625]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action( 15.0), [0.202128488472172, 1.2713110020675373]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action( 25.0), [0.373879790860577, 1.0903607984674346]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action( 35.0), [0.549471728139445, 0.9222828279518422]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action( 45.0), [0.710705016737154, 0.7735795830495835]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action( 55.0), [0.840880718229975, 0.6411697711359448]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action( 65.0), [0.928231751538775, 0.5151500526277536]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action( 75.0), [0.968010181595931, 0.3834313544270441]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action( 85.0), [0.962498946612432, 0.2365575688235299]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action( 95.0), [0.918962083182861, 0.0710799251660952]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(105.0), [0.846350754433821, -0.109480744439727]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(115.0), [0.752088082104471, -0.296096341267311]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(125.0), [0.640211399707369, -0.477269780678076]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(135.0), [0.511542857843088, -0.642348504023967]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(145.0), [0.365641994383437, -0.783891904586578]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(155.0), [0.203481284390595, -0.898389502372114]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(165.0), [0.029458609235600, -0.985341026538423]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(175.0), [-0.14832016607275, -1.045337536201359]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(185.0), [-0.31973194704182, -1.078106424907824]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(195.0), [-0.47559728238048, -1.081414236647962]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(205.0), [-0.61057739245502, -1.051315002564407]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(215.0), [-0.72464951816128, -0.983656722049111]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(225.0), [-0.82239617282399, -0.876234639178472]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(235.0), [-0.91028667581172, -0.730703295106655]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(245.0), [-0.99300490189735, -0.553433729608649]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(255.0), [-1.07031044036758, -0.354899392727916]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(265.0), [-1.13573876935179, -0.147740859185571]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(275.0), [-1.17774177813471, 0.0558274223675871]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(285.0), [-1.18294425460333, 0.2463503554390451]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(295.0), [-1.14041609925062, 0.4183942587682155]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(305.0), [-1.04551997667269, 0.5703518831234655]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(315.0), [-0.90208517428129, 0.7029745828341570]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(325.0), [-0.72226768613349, 0.8172988304816548]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(335.0), [-0.52422932693523, 0.9128376082472604]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(345.0), [-0.32842152814092, 0.9867583939515017]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action(355.0), [-0.15358827566240, 1.0343436515482327]): self.assertAlmostEqual(xi, yi, places=3)

    def testGaussianProcess3(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output: double
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: GP
        fields:
          - {name: x, type: {type: array, items: double}}
          - {name: to, type: double}
          - {name: sigma, type: double}
    init:
      - {x: [  0,   0], to: 0.82118528, sigma: 0.2}
      - {x: [  0,  36], to: 0.63603407, sigma: 0.2}
      - {x: [  0,  72], to: 0.43135014, sigma: 0.2}
      - {x: [  0, 108], to: -0.5271264, sigma: 0.2}
      - {x: [  0, 144], to: -0.7426378, sigma: 0.2}
      - {x: [  0, 180], to: -1.1869050, sigma: 0.2}
      - {x: [  0, 216], to: -0.7996154, sigma: 0.2}
      - {x: [  0, 252], to: -0.4564504, sigma: 0.2}
      - {x: [  0, 288], to: 0.08426291, sigma: 0.2}
      - {x: [  0, 324], to: 0.80768845, sigma: 0.2}
      - {x: [ 36,   0], to: 1.35803374, sigma: 0.2}
      - {x: [ 36,  36], to: 1.52769845, sigma: 0.2}
      - {x: [ 36,  72], to: 1.08079765, sigma: 0.2}
      - {x: [ 36, 108], to: 0.31241499, sigma: 0.2}
      - {x: [ 36, 144], to: -0.2676979, sigma: 0.2}
      - {x: [ 36, 180], to: -0.7164726, sigma: 0.2}
      - {x: [ 36, 216], to: -0.3338313, sigma: 0.2}
      - {x: [ 36, 252], to: 0.08139820, sigma: 0.2}
      - {x: [ 36, 288], to: 0.71689790, sigma: 0.2}
      - {x: [ 36, 324], to: 1.13835037, sigma: 0.2}
      - {x: [ 72,   0], to: 1.83512995, sigma: 0.2}
      - {x: [ 72,  36], to: 1.61494407, sigma: 0.2}
      - {x: [ 72,  72], to: 1.50290190, sigma: 0.2}
      - {x: [ 72, 108], to: 0.75406155, sigma: 0.2}
      - {x: [ 72, 144], to: 0.03405990, sigma: 0.2}
      - {x: [ 72, 180], to: 0.14337997, sigma: 0.2}
      - {x: [ 72, 216], to: 0.38604138, sigma: 0.2}
      - {x: [ 72, 252], to: 0.36514719, sigma: 0.2}
      - {x: [ 72, 288], to: 1.31043893, sigma: 0.2}
      - {x: [ 72, 324], to: 1.63925281, sigma: 0.2}
      - {x: [108,   0], to: 2.18498629, sigma: 0.2}
      - {x: [108,  36], to: 1.36922627, sigma: 0.2}
      - {x: [108,  72], to: 1.41108233, sigma: 0.2}
      - {x: [108, 108], to: 0.80950036, sigma: 0.2}
      - {x: [108, 144], to: 0.07678710, sigma: 0.2}
      - {x: [108, 180], to: 0.03666408, sigma: 0.2}
      - {x: [108, 216], to: -0.2375061, sigma: 0.2}
      - {x: [108, 252], to: 0.57171030, sigma: 0.2}
      - {x: [108, 288], to: 1.35875134, sigma: 0.2}
      - {x: [108, 324], to: 1.64114251, sigma: 0.2}
      - {x: [144,   0], to: 1.81406684, sigma: 0.2}
      - {x: [144,  36], to: 1.36598027, sigma: 0.2}
      - {x: [144,  72], to: 0.87335695, sigma: 0.2}
      - {x: [144, 108], to: 0.28625228, sigma: 0.2}
      - {x: [144, 144], to: -0.1884535, sigma: 0.2}
      - {x: [144, 180], to: -0.7475230, sigma: 0.2}
      - {x: [144, 216], to: 0.05916590, sigma: 0.2}
      - {x: [144, 252], to: 0.20589299, sigma: 0.2}
      - {x: [144, 288], to: 1.49434570, sigma: 0.2}
      - {x: [144, 324], to: 1.04382638, sigma: 0.2}
      - {x: [180,   0], to: 0.95695423, sigma: 0.2}
      - {x: [180,  36], to: 0.99368592, sigma: 0.2}
      - {x: [180,  72], to: 0.03288738, sigma: 0.2}
      - {x: [180, 108], to: -0.6079039, sigma: 0.2}
      - {x: [180, 144], to: -0.3848322, sigma: 0.2}
      - {x: [180, 180], to: -1.0155591, sigma: 0.2}
      - {x: [180, 216], to: -0.5555413, sigma: 0.2}
      - {x: [180, 252], to: -0.0581398, sigma: 0.2}
      - {x: [180, 288], to: 0.33743708, sigma: 0.2}
      - {x: [180, 324], to: 0.83556571, sigma: 0.2}
      - {x: [216,   0], to: 0.20588985, sigma: 0.2}
      - {x: [216,  36], to: 0.44298549, sigma: 0.2}
      - {x: [216,  72], to: -0.5446849, sigma: 0.2}
      - {x: [216, 108], to: -1.0020396, sigma: 0.2}
      - {x: [216, 144], to: -1.8021995, sigma: 0.2}
      - {x: [216, 180], to: -1.5844545, sigma: 0.2}
      - {x: [216, 216], to: -1.7084132, sigma: 0.2}
      - {x: [216, 252], to: -0.9891052, sigma: 0.2}
      - {x: [216, 288], to: -0.6297273, sigma: 0.2}
      - {x: [216, 324], to: 0.26628269, sigma: 0.2}
      - {x: [252,   0], to: 0.10807076, sigma: 0.2}
      - {x: [252,  36], to: -0.4890686, sigma: 0.2}
      - {x: [252,  72], to: -0.5842210, sigma: 0.2}
      - {x: [252, 108], to: -1.2321703, sigma: 0.2}
      - {x: [252, 144], to: -1.8977512, sigma: 0.2}
      - {x: [252, 180], to: -2.1240163, sigma: 0.2}
      - {x: [252, 216], to: -1.9555430, sigma: 0.2}
      - {x: [252, 252], to: -1.5510880, sigma: 0.2}
      - {x: [252, 288], to: -0.6289043, sigma: 0.2}
      - {x: [252, 324], to: -0.2906448, sigma: 0.2}
      - {x: [288,   0], to: 0.04032433, sigma: 0.2}
      - {x: [288,  36], to: -0.0974952, sigma: 0.2}
      - {x: [288,  72], to: -0.6059362, sigma: 0.2}
      - {x: [288, 108], to: -1.4171517, sigma: 0.2}
      - {x: [288, 144], to: -1.7699124, sigma: 0.2}
      - {x: [288, 180], to: -2.1935099, sigma: 0.2}
      - {x: [288, 216], to: -1.9860432, sigma: 0.2}
      - {x: [288, 252], to: -1.1616088, sigma: 0.2}
      - {x: [288, 288], to: -0.8162288, sigma: 0.2}
      - {x: [288, 324], to: 0.16975848, sigma: 0.2}
      - {x: [324,   0], to: 0.34328957, sigma: 0.2}
      - {x: [324,  36], to: 0.26405396, sigma: 0.2}
      - {x: [324,  72], to: -0.3641890, sigma: 0.2}
      - {x: [324, 108], to: -0.9854455, sigma: 0.2}
      - {x: [324, 144], to: -1.3019051, sigma: 0.2}
      - {x: [324, 180], to: -1.6919030, sigma: 0.2}
      - {x: [324, 216], to: -1.1601112, sigma: 0.2}
      - {x: [324, 252], to: -0.9362727, sigma: 0.2}
      - {x: [324, 288], to: -0.4371584, sigma: 0.2}
      - {x: [324, 324], to: 0.17624777, sigma: 0.2}
action:
  model.reg.gaussianProcess:
    - input          # find the best fit to the input x

    - {cell: table}  # use the provided table of training data

    - null           # no explicit krigingWeight: universal Kriging

    - fcn: m.kernel.rbf    # radial basis function (squared exponential)
      fill: {gamma: 2.0}   # with a given gamma (by partial application)
                           # can be replaced with any function,
                           # from the m.kernel.* library or user-defined
''')
        self.assertAlmostEqual(engine.action([  0,   0]), 0.789702380, places=3)
        self.assertAlmostEqual(engine.action([  0,  36]), 0.783152417, places=3)
        self.assertAlmostEqual(engine.action([  0,  72]), 0.336554168, places=3)
        self.assertAlmostEqual(engine.action([  0, 108]), -0.31735296, places=3)
        self.assertAlmostEqual(engine.action([  0, 144]), -0.87359504, places=3)
        self.assertAlmostEqual(engine.action([  0, 180]), -1.11058453, places=3)
        self.assertAlmostEqual(engine.action([  0, 216]), -0.85981518, places=3)
        self.assertAlmostEqual(engine.action([  0, 252]), -0.31377406, places=3)
        self.assertAlmostEqual(engine.action([  0, 288]), 0.302067355, places=3)
        self.assertAlmostEqual(engine.action([  0, 324]), 0.732170021, places=3)
        self.assertAlmostEqual(engine.action([ 36,   0]), 1.368724991, places=3)
        self.assertAlmostEqual(engine.action([ 36,  36]), 1.462784826, places=3)
        self.assertAlmostEqual(engine.action([ 36,  72]), 1.109794918, places=3)
        self.assertAlmostEqual(engine.action([ 36, 108]), 0.299486358, places=3)
        self.assertAlmostEqual(engine.action([ 36, 144]), -0.45987381, places=3)
        self.assertAlmostEqual(engine.action([ 36, 180]), -0.67344493, places=3)
        self.assertAlmostEqual(engine.action([ 36, 216]), -0.37367662, places=3)
        self.assertAlmostEqual(engine.action([ 36, 252]), 0.144398029, places=3)
        self.assertAlmostEqual(engine.action([ 36, 288]), 0.769348666, places=3)
        self.assertAlmostEqual(engine.action([ 36, 324]), 1.174563705, places=3)
        self.assertAlmostEqual(engine.action([ 72,   0]), 1.850909636, places=3)
        self.assertAlmostEqual(engine.action([ 72,  36]), 1.637780435, places=3)
        self.assertAlmostEqual(engine.action([ 72,  72]), 1.465527124, places=3)
        self.assertAlmostEqual(engine.action([ 72, 108]), 0.840201434, places=3)
        self.assertAlmostEqual(engine.action([ 72, 144]), -0.00294014, places=3)
        self.assertAlmostEqual(engine.action([ 72, 180]), -0.28097522, places=3)
        self.assertAlmostEqual(engine.action([ 72, 216]), -0.01113069, places=3)
        self.assertAlmostEqual(engine.action([ 72, 252]), 0.530567388, places=3)
        self.assertAlmostEqual(engine.action([ 72, 288]), 1.249584091, places=3)
        self.assertAlmostEqual(engine.action([ 72, 324]), 1.626805927, places=3)
        self.assertAlmostEqual(engine.action([108,   0]), 2.126374782, places=3)
        self.assertAlmostEqual(engine.action([108,  36]), 1.532192371, places=3)
        self.assertAlmostEqual(engine.action([108,  72]), 1.312555308, places=3)
        self.assertAlmostEqual(engine.action([108, 108]), 0.848578656, places=3)
        self.assertAlmostEqual(engine.action([108, 144]), 0.016508458, places=3)
        self.assertAlmostEqual(engine.action([108, 180]), -0.36538357, places=3)
        self.assertAlmostEqual(engine.action([108, 216]), -0.08564144, places=3)
        self.assertAlmostEqual(engine.action([108, 252]), 0.662172897, places=3)
        self.assertAlmostEqual(engine.action([108, 288]), 1.452223836, places=3)
        self.assertAlmostEqual(engine.action([108, 324]), 1.595081961, places=3)
        self.assertAlmostEqual(engine.action([144,   0]), 1.821615875, places=3)
        self.assertAlmostEqual(engine.action([144,  36]), 1.347803533, places=3)
        self.assertAlmostEqual(engine.action([144,  72]), 0.914697032, places=3)
        self.assertAlmostEqual(engine.action([144, 108]), 0.317009091, places=3)
        self.assertAlmostEqual(engine.action([144, 144]), -0.35898810, places=3)
        self.assertAlmostEqual(engine.action([144, 180]), -0.59941579, places=3)
        self.assertAlmostEqual(engine.action([144, 216]), -0.24689828, places=3)
        self.assertAlmostEqual(engine.action([144, 252]), 0.648902743, places=3)
        self.assertAlmostEqual(engine.action([144, 288]), 1.382349357, places=3)
        self.assertAlmostEqual(engine.action([144, 324]), 1.218171697, places=3)
        self.assertAlmostEqual(engine.action([180,   0]), 1.009615522, places=3)
        self.assertAlmostEqual(engine.action([180,  36]), 0.906944445, places=3)
        self.assertAlmostEqual(engine.action([180,  72]), 0.351897200, places=3)
        self.assertAlmostEqual(engine.action([180, 108]), -0.50574559, places=3)
        self.assertAlmostEqual(engine.action([180, 144]), -1.04549932, places=3)
        self.assertAlmostEqual(engine.action([180, 180]), -1.04417136, places=3)
        self.assertAlmostEqual(engine.action([180, 216]), -0.76769335, places=3)
        self.assertAlmostEqual(engine.action([180, 252]), -0.05415796, places=3)
        self.assertAlmostEqual(engine.action([180, 288]), 0.672986650, places=3)
        self.assertAlmostEqual(engine.action([180, 324]), 0.711362567, places=3)
        self.assertAlmostEqual(engine.action([216,   0]), 0.221888719, places=3)
        self.assertAlmostEqual(engine.action([216,  36]), 0.240183946, places=3)
        self.assertAlmostEqual(engine.action([216,  72]), -0.23160998, places=3)
        self.assertAlmostEqual(engine.action([216, 108]), -1.12330176, places=3)
        self.assertAlmostEqual(engine.action([216, 144]), -1.68735535, places=3)
        self.assertAlmostEqual(engine.action([216, 180]), -1.69764134, places=3)
        self.assertAlmostEqual(engine.action([216, 216]), -1.58297377, places=3)
        self.assertAlmostEqual(engine.action([216, 252]), -1.12069223, places=3)
        self.assertAlmostEqual(engine.action([216, 288]), -0.36194660, places=3)
        self.assertAlmostEqual(engine.action([216, 324]), 0.135935887, places=3)
        self.assertAlmostEqual(engine.action([252,   0]), -0.13986011, places=3)
        self.assertAlmostEqual(engine.action([252,  36]), -0.24481529, places=3)
        self.assertAlmostEqual(engine.action([252,  72]), -0.63250177, places=3)
        self.assertAlmostEqual(engine.action([252, 108]), -1.31714040, places=3)
        self.assertAlmostEqual(engine.action([252, 144]), -1.89469570, places=3)
        self.assertAlmostEqual(engine.action([252, 180]), -2.12134236, places=3)
        self.assertAlmostEqual(engine.action([252, 216]), -2.01067201, places=3)
        self.assertAlmostEqual(engine.action([252, 252]), -1.50394827, places=3)
        self.assertAlmostEqual(engine.action([252, 288]), -0.80015485, places=3)
        self.assertAlmostEqual(engine.action([252, 324]), -0.19396956, places=3)
        self.assertAlmostEqual(engine.action([288,   0]), 0.002864571, places=3)
        self.assertAlmostEqual(engine.action([288,  36]), -0.23510960, places=3)
        self.assertAlmostEqual(engine.action([288,  72]), -0.73985078, places=3)
        self.assertAlmostEqual(engine.action([288, 108]), -1.30389377, places=3)
        self.assertAlmostEqual(engine.action([288, 144]), -1.81904616, places=3)
        self.assertAlmostEqual(engine.action([288, 180]), -2.17590823, places=3)
        self.assertAlmostEqual(engine.action([288, 216]), -1.93250697, places=3)
        self.assertAlmostEqual(engine.action([288, 252]), -1.27413740, places=3)
        self.assertAlmostEqual(engine.action([288, 288]), -0.70495225, places=3)
        self.assertAlmostEqual(engine.action([288, 324]), -0.23168405, places=3)
        self.assertAlmostEqual(engine.action([324,   0]), 0.295490316, places=3)
        self.assertAlmostEqual(engine.action([324,  36]), 0.079930170, places=3)
        self.assertAlmostEqual(engine.action([324,  72]), -0.45917604, places=3)
        self.assertAlmostEqual(engine.action([324, 108]), -0.95631842, places=3)
        self.assertAlmostEqual(engine.action([324, 144]), -1.36180103, places=3)
        self.assertAlmostEqual(engine.action([324, 180]), -1.62933375, places=3)
        self.assertAlmostEqual(engine.action([324, 216]), -1.32079787, places=3)
        self.assertAlmostEqual(engine.action([324, 252]), -0.80798892, places=3)
        self.assertAlmostEqual(engine.action([324, 288]), -0.49451255, places=3)
        self.assertAlmostEqual(engine.action([324, 324]), -0.16477560, places=3)

    def testGaussianProcess4(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  table:
    type:
      type: array
      items:
        type: record
        name: GP
        fields:
          - {name: x, type: {type: array, items: double}}
          - {name: to, type: {type: array, items: double}}
          - {name: sigma, type: {type: array, items: double}}
    init:
      - {x: [  0,   0], to: [0.01870587, 0.96812508], sigma: [0.2, 0.2]}
      - {x: [  0,  36], to: [0.00242101, 0.95369720], sigma: [0.2, 0.2]}
      - {x: [  0,  72], to: [0.13131668, 0.53822666], sigma: [0.2, 0.2]}
      - {x: [  0, 108], to: [-0.0984303, -0.3743950], sigma: [0.2, 0.2]}
      - {x: [  0, 144], to: [0.15985766, -0.6027780], sigma: [0.2, 0.2]}
      - {x: [  0, 180], to: [-0.2417438, -1.0968682], sigma: [0.2, 0.2]}
      - {x: [  0, 216], to: [0.05190623, -0.9102348], sigma: [0.2, 0.2]}
      - {x: [  0, 252], to: [0.27249439, -0.4792263], sigma: [0.2, 0.2]}
      - {x: [  0, 288], to: [0.07282733, 0.48063363], sigma: [0.2, 0.2]}
      - {x: [  0, 324], to: [-0.0842266, 0.57112860], sigma: [0.2, 0.2]}
      - {x: [ 36,   0], to: [0.47755174, 1.13094388], sigma: [0.2, 0.2]}
      - {x: [ 36,  36], to: [0.41956515, 0.90267757], sigma: [0.2, 0.2]}
      - {x: [ 36,  72], to: [0.59136153, 0.41456807], sigma: [0.2, 0.2]}
      - {x: [ 36, 108], to: [0.60570628, -0.2181357], sigma: [0.2, 0.2]}
      - {x: [ 36, 144], to: [0.59105899, -0.5619968], sigma: [0.2, 0.2]}
      - {x: [ 36, 180], to: [0.57772703, -0.8929270], sigma: [0.2, 0.2]}
      - {x: [ 36, 216], to: [0.23902551, -0.8220304], sigma: [0.2, 0.2]}
      - {x: [ 36, 252], to: [0.61153563, -0.0519713], sigma: [0.2, 0.2]}
      - {x: [ 36, 288], to: [0.64443777, 0.48040414], sigma: [0.2, 0.2]}
      - {x: [ 36, 324], to: [0.48667517, 0.71326465], sigma: [0.2, 0.2]}
      - {x: [ 72,   0], to: [1.09232448, 0.93827725], sigma: [0.2, 0.2]}
      - {x: [ 72,  36], to: [0.81049592, 1.11762190], sigma: [0.2, 0.2]}
      - {x: [ 72,  72], to: [0.71568727, 0.06369347], sigma: [0.2, 0.2]}
      - {x: [ 72, 108], to: [0.72942906, -0.5640199], sigma: [0.2, 0.2]}
      - {x: [ 72, 144], to: [1.06713767, -0.4772772], sigma: [0.2, 0.2]}
      - {x: [ 72, 180], to: [1.38277511, -0.9363026], sigma: [0.2, 0.2]}
      - {x: [ 72, 216], to: [0.61698083, -0.8860234], sigma: [0.2, 0.2]}
      - {x: [ 72, 252], to: [0.82624676, -0.1171322], sigma: [0.2, 0.2]}
      - {x: [ 72, 288], to: [0.83217277, 0.30132193], sigma: [0.2, 0.2]}
      - {x: [ 72, 324], to: [0.74893667, 0.80824628], sigma: [0.2, 0.2]}
      - {x: [108,   0], to: [0.66284547, 0.85288292], sigma: [0.2, 0.2]}
      - {x: [108,  36], to: [0.59724043, 0.88159718], sigma: [0.2, 0.2]}
      - {x: [108,  72], to: [0.28727426, 0.20407304], sigma: [0.2, 0.2]}
      - {x: [108, 108], to: [0.90503697, -0.5979697], sigma: [0.2, 0.2]}
      - {x: [108, 144], to: [1.05726502, -0.8156704], sigma: [0.2, 0.2]}
      - {x: [108, 180], to: [0.55263541, -1.1994934], sigma: [0.2, 0.2]}
      - {x: [108, 216], to: [0.50777742, -0.7713018], sigma: [0.2, 0.2]}
      - {x: [108, 252], to: [0.60347324, -0.2211189], sigma: [0.2, 0.2]}
      - {x: [108, 288], to: [1.16101443, -0.1406493], sigma: [0.2, 0.2]}
      - {x: [108, 324], to: [0.92295182, 0.51506096], sigma: [0.2, 0.2]}
      - {x: [144,   0], to: [0.80924121, 0.83038461], sigma: [0.2, 0.2]}
      - {x: [144,  36], to: [0.80043759, 0.57306896], sigma: [0.2, 0.2]}
      - {x: [144,  72], to: [0.74865899, 0.12507470], sigma: [0.2, 0.2]}
      - {x: [144, 108], to: [0.54867424, -0.2083665], sigma: [0.2, 0.2]}
      - {x: [144, 144], to: [0.58431995, -0.7811933], sigma: [0.2, 0.2]}
      - {x: [144, 180], to: [0.71950969, -0.9713840], sigma: [0.2, 0.2]}
      - {x: [144, 216], to: [0.52307948, -0.8731280], sigma: [0.2, 0.2]}
      - {x: [144, 252], to: [0.36976490, -0.3895379], sigma: [0.2, 0.2]}
      - {x: [144, 288], to: [0.37565453, 0.21778435], sigma: [0.2, 0.2]}
      - {x: [144, 324], to: [0.45793731, 0.85264234], sigma: [0.2, 0.2]}
      - {x: [180,   0], to: [-0.0441948, 1.09297816], sigma: [0.2, 0.2]}
      - {x: [180,  36], to: [-0.2817155, 0.69222421], sigma: [0.2, 0.2]}
      - {x: [180,  72], to: [0.12103868, 0.25006600], sigma: [0.2, 0.2]}
      - {x: [180, 108], to: [0.11426250, -0.5415858], sigma: [0.2, 0.2]}
      - {x: [180, 144], to: [0.10181024, -0.8848316], sigma: [0.2, 0.2]}
      - {x: [180, 180], to: [-0.1477347, -1.1392833], sigma: [0.2, 0.2]}
      - {x: [180, 216], to: [0.35044408, -0.9500126], sigma: [0.2, 0.2]}
      - {x: [180, 252], to: [0.18675249, -0.4131455], sigma: [0.2, 0.2]}
      - {x: [180, 288], to: [0.24436046, 0.35884024], sigma: [0.2, 0.2]}
      - {x: [180, 324], to: [0.07432997, 1.02698144], sigma: [0.2, 0.2]}
      - {x: [216,   0], to: [-0.6591356, 0.94999291], sigma: [0.2, 0.2]}
      - {x: [216,  36], to: [-0.4494247, 0.69657926], sigma: [0.2, 0.2]}
      - {x: [216,  72], to: [-0.4270339, 0.15420512], sigma: [0.2, 0.2]}
      - {x: [216, 108], to: [-0.5964852, -0.4521517], sigma: [0.2, 0.2]}
      - {x: [216, 144], to: [-0.3799727, -0.9904939], sigma: [0.2, 0.2]}
      - {x: [216, 180], to: [-0.5694217, -1.0015548], sigma: [0.2, 0.2]}
      - {x: [216, 216], to: [-0.6918730, -0.5267317], sigma: [0.2, 0.2]}
      - {x: [216, 252], to: [-0.5838720, -0.4841855], sigma: [0.2, 0.2]}
      - {x: [216, 288], to: [-0.5693374, -0.0133151], sigma: [0.2, 0.2]}
      - {x: [216, 324], to: [-0.4903301, 1.03380154], sigma: [0.2, 0.2]}
      - {x: [252,   0], to: [-1.3293399, 0.71483260], sigma: [0.2, 0.2]}
      - {x: [252,  36], to: [-1.3110310, 0.72705720], sigma: [0.2, 0.2]}
      - {x: [252,  72], to: [-1.0671501, 0.24425863], sigma: [0.2, 0.2]}
      - {x: [252, 108], to: [-0.8844714, -0.2823489], sigma: [0.2, 0.2]}
      - {x: [252, 144], to: [-0.9533401, -1.1736452], sigma: [0.2, 0.2]}
      - {x: [252, 180], to: [-0.5345838, -1.2210451], sigma: [0.2, 0.2]}
      - {x: [252, 216], to: [-1.0862084, -0.7348636], sigma: [0.2, 0.2]}
      - {x: [252, 252], to: [-0.7549718, -0.1849688], sigma: [0.2, 0.2]}
      - {x: [252, 288], to: [-1.2390564, 0.54575855], sigma: [0.2, 0.2]}
      - {x: [252, 324], to: [-1.0288154, 0.84115420], sigma: [0.2, 0.2]}
      - {x: [288,   0], to: [-0.5410771, 1.10696790], sigma: [0.2, 0.2]}
      - {x: [288,  36], to: [-0.8322681, 0.44386847], sigma: [0.2, 0.2]}
      - {x: [288,  72], to: [-0.9040048, 0.00519231], sigma: [0.2, 0.2]}
      - {x: [288, 108], to: [-0.6676514, -0.4833115], sigma: [0.2, 0.2]}
      - {x: [288, 144], to: [-1.0580007, -1.2009009], sigma: [0.2, 0.2]}
      - {x: [288, 180], to: [-0.8102370, -1.2521135], sigma: [0.2, 0.2]}
      - {x: [288, 216], to: [-1.2759558, -0.7864478], sigma: [0.2, 0.2]}
      - {x: [288, 252], to: [-0.5628566, 0.13344358], sigma: [0.2, 0.2]}
      - {x: [288, 288], to: [-0.9149276, 0.22418075], sigma: [0.2, 0.2]}
      - {x: [288, 324], to: [-0.5648838, 0.75833374], sigma: [0.2, 0.2]}
      - {x: [324,   0], to: [-0.6311144, 0.83818280], sigma: [0.2, 0.2]}
      - {x: [324,  36], to: [-0.5527385, 0.84973376], sigma: [0.2, 0.2]}
      - {x: [324,  72], to: [-0.3039325, -0.2189731], sigma: [0.2, 0.2]}
      - {x: [324, 108], to: [-0.4498324, 0.07328764], sigma: [0.2, 0.2]}
      - {x: [324, 144], to: [-0.7415195, -0.6128136], sigma: [0.2, 0.2]}
      - {x: [324, 180], to: [-0.7918942, -1.2435311], sigma: [0.2, 0.2]}
      - {x: [324, 216], to: [-0.6853270, -0.5134147], sigma: [0.2, 0.2]}
      - {x: [324, 252], to: [-0.7581712, -0.7304523], sigma: [0.2, 0.2]}
      - {x: [324, 288], to: [-0.4803783, 0.12660344], sigma: [0.2, 0.2]}
      - {x: [324, 324], to: [-0.6815587, 0.82271760], sigma: [0.2, 0.2]}
action:
  model.reg.gaussianProcess:
    - input          # find the best fit to the input x

    - {cell: table}  # use the provided table of training data

    - null           # no explicit krigingWeight: universal Kriging

    - fcn: m.kernel.rbf    # radial basis function (squared exponential)
      fill: {gamma: 2.0}   # with a given gamma (by partial application)
                           # can be replaced with any function,
                           # from the m.kernel.* library or user-defined
''')
        for xi, yi in zip(engine.action([  0,   0]), [0.064779229813204, 0.96782301456871]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([  0,  36]), [0.121664830264969, 0.91524378145119]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([  0,  72]), [0.216316970981976, 0.43536919421404]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([  0, 108]), [0.158097330508110, -0.1570712583734]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([  0, 144]), [0.005843683253181, -0.6746409371841]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([  0, 180]), [-0.05505229209753, -1.0348201168723]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([  0, 216]), [0.027244856798085, -0.9323695730894]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([  0, 252]), [0.228310304549874, -0.3237075447481]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([  0, 288]), [0.329555998371579, 0.33447339642380]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([  0, 324]), [0.224134433890304, 0.56179760089078]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 36,   0]), [0.642510569955593, 1.08150359261086]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 36,  36]), [0.574414083994342, 1.01547509493490]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 36,  72]), [0.525254987256053, 0.38482080968042]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 36, 108]), [0.521968103358422, -0.2322465354319]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 36, 144]), [0.646137319268672, -0.6026320145505]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 36, 180]), [0.724069639046670, -0.9017679835361]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 36, 216]), [0.573719616038126, -0.8428508869520]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 36, 252]), [0.545852542329543, -0.2506561706813]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 36, 288]), [0.606872636602972, 0.45029925454635]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 36, 324]), [0.441389771978765, 0.74083544766659]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 72,   0]), [0.977748454643780, 1.00104108097745]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 72,  36]), [0.797350444231129, 1.03140212433305]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 72,  72]), [0.673249258827750, 0.32601280917617]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 72, 108]), [0.793138523118285, -0.3819477574562]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 72, 144]), [1.136586425054690, -0.7293445925598]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 72, 180]), [1.227854236064106, -0.9631493525473]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 72, 216]), [0.852897762435422, -0.8365809399416]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 72, 252]), [0.750239280286434, -0.2492263368339]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 72, 288]), [0.940481362701912, 0.39790602429840]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([ 72, 324]), [0.768251823210774, 0.72003180803082]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([108,   0]), [0.859379032952287, 0.83957209536458]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([108,  36]), [0.746591722059892, 0.84563877181533]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([108,  72]), [0.700088795230686, 0.19173494869166]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([108, 108]), [0.825268228136332, -0.4830828897588]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([108, 144]), [1.037957610023134, -0.8665175669264]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([108, 180]), [0.988710260763903, -1.0813726256458]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([108, 216]), [0.642968431918638, -0.8706558175409]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([108, 252]), [0.703236484927596, -0.2891600211628]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([108, 288]), [1.051373809585268, 0.30156199893011]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([108, 324]), [0.905999740390180, 0.65523913744991]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([144,   0]), [0.722094791027407, 0.86978626471051]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([144,  36]), [0.729299311470238, 0.66775521482295]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([144,  72]), [0.678505139706764, 0.06440219730373]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([144, 108]), [0.625609724224827, -0.4743647710358]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([144, 144]), [0.632999597034182, -0.8421184081561]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([144, 180]), [0.580666654244519, -1.0626002319736]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([144, 216]), [0.414434221704458, -0.8919514744551]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([144, 252]), [0.488257767012426, -0.3608586328044]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([144, 288]), [0.699495457977056, 0.31413231222162]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([144, 324]), [0.576720865037063, 0.80266743957387]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([180,   0]), [0.261714306314791, 1.02763696753567]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([180,  36]), [0.311599104375023, 0.70761204493112]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([180,  72]), [0.263577805718530, 0.09714664449536]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([180, 108]), [0.127845554599678, -0.4576463574262]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([180, 144]), [0.090383674750804, -0.8722179387155]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([180, 180]), [0.113864383315045, -1.0580113962883]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([180, 216]), [0.069990322010953, -0.9057571224064]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([180, 252]), [0.066226881493862, -0.4089280668320]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([180, 288]), [0.062772044621099, 0.39131654543846]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([180, 324]), [0.009460119595085, 1.01055465934145]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([216,   0]), [-0.73312221526568, 0.93804750105795]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([216,  36]), [-0.68789807457787, 0.70400861091742]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([216,  72]), [-0.55197886175110, 0.16874457785834]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([216, 108]), [-0.52450042744038, -0.4729170540772]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([216, 144]), [-0.49933749191187, -0.9901355525858]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([216, 180]), [-0.49437643240508, -1.0701171487030]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([216, 216]), [-0.54576440321639, -0.7756610412889]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([216, 252]), [-0.57560565089801, -0.2764287342223]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([216, 288]), [-0.68768932281517, 0.46231642218308]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([216, 324]), [-0.63276363506716, 1.00624161265100]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([252,   0]), [-1.25883018418244, 0.86182070915403]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([252,  36]), [-1.27125247167942, 0.66522056518503]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([252,  72]), [-1.05057208838978, 0.16063055721863]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([252, 108]), [-0.92161147272343, -0.5412979662814]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([252, 144]), [-0.88016893226660, -1.1607246598436]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([252, 180]), [-0.93610096907829, -1.1769350713380]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([252, 216]), [-1.03159661761564, -0.6954753280662]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([252, 252]), [-1.01849469291683, -0.1575283455216]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([252, 288]), [-1.11213871999217, 0.44341038263258]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([252, 324]), [-0.99527599035834, 0.85559322529816]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([288,   0]), [-0.87403287278578, 1.00916210932357]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([288,  36]), [-0.92463976323619, 0.76734917968979]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([288,  72]), [-0.83080416416296, 0.20798340092916]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([288, 108]), [-0.84872084908145, -0.4899647534093]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([288, 144]), [-0.93999991208532, -1.1658773991644]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([288, 180]), [-1.05082792329645, -1.2795153389135]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([288, 216]), [-1.12069043055638, -0.8204558452373]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([288, 252]), [-0.97320402678885, -0.3078952922898]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([288, 288]), [-0.90525349575954, 0.28418179753749]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([288, 324]), [-0.80502946506568, 0.77845811479377]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([324,   0]), [-0.50633607253996, 0.89851286875373]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([324,  36]), [-0.48838873442536, 0.76025946112935]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([324,  72]), [-0.41973030207655, 0.32609340242212]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([324, 108]), [-0.52386015896191, -0.2120845250366]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([324, 144]), [-0.69731418799696, -0.8419776200889]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([324, 180]), [-0.78738043867671, -1.1436369487980]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([324, 216]), [-0.80709074532220, -0.9317562808077]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([324, 252]), [-0.66910568870076, -0.5508149563349]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([324, 288]), [-0.58129897583945, 0.08421193036071]): self.assertAlmostEqual(xi, yi, places=3)
        for xi, yi in zip(engine.action([324, 324]), [-0.56539974304732, 0.73545564385919]): self.assertAlmostEqual(xi, yi, places=3)
