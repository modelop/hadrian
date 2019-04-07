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

class TestLib1ModelNaive(unittest.TestCase):

#GAUSSIAN###############################################################
    def testNaiveGaussian(self):
    # test array signature, then map signature
    # from sklearn.naive_bayes import GaussianNB
    # clf = GaussianNB()
    # X = np.array([[-1,  0,  1],
    #               [-1,  0,  1],
    #               [-3,  2,  3],
    #               [-3,  2,  3],
    #               [ 0, -1, -1],
    #               [-2,  3,  3]])
    # Y = np.array([1, 1, 1,1, 2, 2])
    # clf.fit(X,Y)
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  twoClass:
    type:
      type: map
      values:
        type: array
        items:
          type: record
          name: parameters
          fields:
            - {name: mean,  type: double}
            - {name: variance, type: double}
    init:
        {class1: [{mean: -2.0, variance: 1.0},
                  {mean:  1.0, variance: 1.0},
                  {mean:  2.0, variance: 1.0}],
         class2: [{mean: -1.0, variance: 1.0},
                  {mean:  1.0, variance: 4.0},
                  {mean:  1.0, variance: 4.0}]}
action:

    - let:
        class1params: {cell: twoClass, path: [{string: class1}]}
        class2params: {cell: twoClass, path: [{string: class2}]}

    - let:
        class1LL: {model.naive.gaussian: [input, class1params]}
        class2LL: {model.naive.gaussian: [input, class2params]}

    - let:
        classLL:
          type: {type: array, items: double}
          new:
            - class1LL
            - class2LL
    - let:
        class1Lprior: -0.40546511
        class2Lprior: -1.09861229

    - let:
        classLPost:
          type: {type: array, items: double}
          new:
            - "+": [class1LL, class1Lprior]
            - "+": [class2LL, class2Lprior]

    - let:
        C: {a.logsumexp: [classLPost]}

    - a.map:
        - classLPost
        - params: [{x: double}]
          ret: double
          do: {"-": [x, C]}

""")
        pfa_output = engine.action([-1.0, 0.0, 1.0])
        true_value = [-0.4017144770379799, -1.1061560181578711]
        for p,t in zip(pfa_output, true_value):
            self.assertAlmostEqual(p, t, places=3)

        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: array, items: double}
cells:
  twoClass:
    type:
      type: map
      values:
        type: map
        values:
          type: record
          name: parameters
          fields:
            - {name: mean,  type: double}
            - {name: variance, type: double}
    init:
        {class1: {f1: {mean: -2.0, variance: 1.0},
                  f2: {mean:  1.0, variance: 1.0},
                  f3: {mean:  2.0, variance: 1.0}},
         class2: {f1: {mean: -1.0, variance: 1.0},
                  f2: {mean:  1.0, variance: 4.0},
                  f3: {mean:  1.0, variance: 4.0}}}
action:

    - let:
        class1params: {cell: twoClass, path: [{string: class1}]}
        class2params: {cell: twoClass, path: [{string: class2}]}

    - let:
        class1LL: {model.naive.gaussian: [input, class1params]}
        class2LL: {model.naive.gaussian: [input, class2params]}

    - let:
        classLL:
          type: {type: array, items: double}
          new:
            - class1LL
            - class2LL
    - let:
        class1Lprior: -0.40546511
        class2Lprior: -1.09861229

    - let:
        classLPost:
          type: {type: array, items: double}
          new:
            - "+": [class1LL, class1Lprior]
            - "+": [class2LL, class2Lprior]

    - let:
        C: {a.logsumexp: [classLPost]}

    - a.map:
        - classLPost
        - params: [{x: double}]
          ret: double
          do: {"-": [x, C]}


""")
        pfa_output = engine.action({"f1": -1.0, "f2": 0.0, "f3": 1.0})
        true_value = [-0.4017144770379799, -1.1061560181578711]
        for p,t in zip(pfa_output, true_value):
            self.assertAlmostEqual(p, t, places=3)

#MULTINOMIAL################################################################
    def testNaiveMultinomial(self):
        # First to map signature, then array signature
        # from sklearn.naive_bayes import MultinomialNB
        # X = array([[0, 3, 0, 3],
        #            [0, 1, 2, 1],
        #            [4, 0, 2, 4],
        #            [0, 1, 4, 0],
        #            [4, 4, 4, 1],
        #            [2, 0, 0, 1],
        #            [2, 0, 4, 2]])
        # Y = array([1, 1, 1, 2, 2, 3, 3])
        # clf = MultinomialNB(alpha=.01)
        # clf.fit(X,Y)
        engine, = PFAEngine.fromYaml("""
input: {type: map, values: double}
output: {type: array, items: double}
cells:
  threeClass:
    type:
      type: map
      values:
        type: map
        values: double
    init:
        {class1: {f1: 0.2,
                  f2: 0.2,
                  f3: 0.2,
                  f4: 0.4},
         class2: {f1: 0.22228381448432147,
                  f2: 0.27771618612438459,
                  f3: 0.44401330534751776,
                  f4: 0.055986696442301712},
         class3: {f1: 0.36322463751061512,
                  f2: 0.0009057970985842759,
                  f3: 0.36322463751061512,
                  f4: 0.27264492810555946}}
action:

    - let:
        class1params: {cell: threeClass, path: [{string: class1}]}
        class2params: {cell: threeClass, path: [{string: class2}]}
        class3params: {cell: threeClass, path: [{string: class3}]}

    - let:
        class1LL: {model.naive.multinomial: [input, class1params]}
        class2LL: {model.naive.multinomial: [input, class2params]}
        class3LL: {model.naive.multinomial: [input, class3params]}

    - let:
        classLL:
          type: {type: array, items: double}
          new:
            - class1LL
            - class2LL
            - class3LL

    - let:
        class1Lprior: -0.84729786
        class2Lprior: -1.25276297
        class3Lprior: -1.25276297

    - let:
        classLPost:
          type: {type: array, items: double}
          new:
            - "+": [class1LL, class1Lprior]
            - "+": [class2LL, class2Lprior]
            - "+": [class3LL, class3Lprior]

    - let:
        C: {a.logsumexp: [classLPost]}

    - a.map:
        - classLPost
        - params: [{x: double}]
          ret: double
          do: {"-": [x, C]}
    """)
        pfa_output = engine.action({"f1": 0, "f2": 1, "f3": 2, "f4": 1})
        true_value = [-0.49768992, -0.9468967 , -5.4910462 ]
        for p,t in zip(pfa_output, true_value):
            self.assertAlmostEqual(p, t, places=3)

        # test array signature
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: {type: array, items: double}
cells:
  threeClass:
    type:
      type: map
      values:
        type: array
        items: double
    init:
        {class1: [0.2,
                  0.2,
                  0.2,
                  0.4],
         class2: [0.22228381448432147,
                  0.27771618612438459,
                  0.44401330534751776,
                  0.055986696442301712],
         class3: [0.36322463751061512,
                  0.0009057970985842759,
                  0.36322463751061512,
                  0.27264492810555946]}
action:

    - let:
        class1params: {cell: threeClass, path: [{string: class1}]}
        class2params: {cell: threeClass, path: [{string: class2}]}
        class3params: {cell: threeClass, path: [{string: class3}]}

    - let:
        class1LL: {model.naive.multinomial: [input, class1params]}
        class2LL: {model.naive.multinomial: [input, class2params]}
        class3LL: {model.naive.multinomial: [input, class3params]}

    - let:
        classLL:
          type: {type: array, items: double}
          new:
            - class1LL
            - class2LL
            - class3LL

    - let:
        class1Lprior: -0.84729786
        class2Lprior: -1.25276297
        class3Lprior: -1.25276297

    - let:
        classLPost:
          type: {type: array, items: double}
          new:
            - "+": [class1LL, class1Lprior]
            - "+": [class2LL, class2Lprior]
            - "+": [class3LL, class3Lprior]
    - let:
        C: {a.logsumexp: [classLPost]}

    - a.map:
        - classLPost
        - params: [{x: double}]
          ret: double
          do: {"-": [x, C]}
    """)
        pfa_output = engine.action([0, 1, 2, 1])
        true_value = [-0.49768992, -0.9468967 , -5.4910462 ]
        for p,t in zip(pfa_output, true_value):
            self.assertAlmostEqual(p, t, places=3)

#BERNOULLI################################################################
    def testNaiveBernoulli(self):
        # First to map signature, then array signature
        # from sklearn.naive_bayes import BernoulliNB
        # X =  array([[1, 1, 0],
        #             [1, 1, 1],
        #             [1, 0, 1],
        #             [1, 0, 0],
        #             [1, 0, 0],
        #             [1, 1, 1]])
        # Y = array([ 1.,  1.,  2.,  2.,  3.,  3.])
        # clf = BernoulliNB()
        # clf.fit(X,Y)
        engine, = PFAEngine.fromYaml("""
input: {type: array, items: string}
output: {type: array, items: double}
cells:
  threeClass:
    type:
      type: map
      values:
        type: map
        values: double
    init:
        {class1: {f1: 0.75,
                  f2: 0.75,
                  f3: 0.5},
         class2: {f1: 0.75,
                  f2: 0.25,
                  f3: 0.5},
         class3: {f1: 0.75,
                  f2: 0.5,
                  f3: 0.5}}
action:

    - let:
        class1params: {cell: threeClass, path: [{string: class1}]}
        class2params: {cell: threeClass, path: [{string: class2}]}
        class3params: {cell: threeClass, path: [{string: class3}]}

    - let:
        class1LL: {model.naive.bernoulli: [input, class1params]}
        class2LL: {model.naive.bernoulli: [input, class2params]}
        class3LL: {model.naive.bernoulli: [input, class3params]}

    - let:
        classLL:
          type: {type: array, items: double}
          new:
            - class1LL
            - class2LL
            - class3LL
    - let:
        C: {a.logsumexp: [classLL]}

    - a.map:
        - classLL
        - params: [{x: double}]
          ret: double
          do: {"-": [x, C]}

    """)
        pfa_output = engine.action(["f1", "f2", "somethingelse"])
        true_value = [-0.69314718, -1.79175947, -1.09861229]
        for p,t in zip(pfa_output, true_value):
            self.assertAlmostEqual(p, t, places=3)
