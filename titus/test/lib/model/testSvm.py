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

class TestLib1ModelSvm(unittest.TestCase):
    def testSvmLinearKernel(self):
        # reproduces scikit-learn SVC result
        """
        import numpy as np
        from sklearn.svm import SVC
	np.random.seed(503)
	X = np.r_[np.random.randn(4,2)+5 - [1,1], np.random.randn(4,2) + [1,1]]
	Y = [0]*4 + [1]*4
	clf = SVC(kernel="linear")
	clf.fit(X, Y)

        # using parameters:
	sv     = clf.support_vectors_
	nv     = clf.n_support_
	alpha  = clf.dual_coef_.flatten()
	b  = clf._intercept_ #(reverse sign, weird convention/bug in scikit)
        # reproduces
        print clf.decision_function([0, 1])
        print clf.decision_function([3, 3])
        """

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: TwoClass
      fields:
        - {name: const, type: double}
        - name: posClass 
          type: 
            type: array
            items: 
              type: record
              name: Class1Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}
        - name: negClass 
          type: 
            type: array
            items: 
              type: record
              name: Class2Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}

    init: 
      {const: 1.83613,
       posClass: [{supVec: [3.96989384, 3.60281757], coeff: -0.14933674}],
       negClass: [{supVec: [0.43689046, 2.45981766], coeff:  0.1070752},
                  {supVec: [1.47126216, 0.48686121], coeff:  0.04226154}]}

action:
  - model.svm.score:
      - input
      - cell: model
      - params: [{x: {type: array, items: double}}, {y: {type: array, items: double}}]
        ret: double
        do: {m.kernel.linear: [x, y]}
""")
        self.assertAlmostEqual(engine.action([0.0, 1.0]), 1.58205794, places=3)
        self.assertAlmostEqual(engine.action([3.0, 3.0]), -0.37776544, places=3)


    def testSvmPolyKernel(self):
        # reproduces scikit-learn SVC result
        """
        import numpy as np
        from sklearn.svm import SVC
	np.random.seed(503)
	X = np.r_[np.random.randn(4,2)+5 - [1,1], np.random.randn(4,2) + [1,1]]
	Y = [0]*4 + [1]*4
	clf = SVC(kernel="poly", degree=2, gamma=0.1, coef0=0.3)
	clf.fit(X, Y)

        # using parameters:
	sv     = clf.support_vectors_
	nv     = clf.n_support_
	alpha  = clf.dual_coef_.flatten()
	b  = clf._intercept_ #(reverse sign, weird convention/bug in scikit)
        # reproduces
        print clf.decision_function([0, 1])
        print clf.decision_function([3, 3])
        """

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: TwoClass
      fields:
        - {name: const, type: double}
        - name: posClass 
          type: 
            type: array
            items: 
              type: record
              name: Class1Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}
        - name: negClass 
          type: 
            type: array
            items: 
              type: record
              name: Class2Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}

    init: 
      {const: 1.27509531,
       posClass: [{supVec: [3.96989384,  3.60281757], coeff: -0.27658039}],
       negClass: [{supVec: [0.43689046,  2.45981766], coeff:  0.27658039}]}

action:
  - let: {gamma: 0.1}
  - let: {degree: 2}
  - let: {intercept: 0.3}

  - model.svm.score:
      - input
      - cell: model
      - params: [{x: {type: array, items: double}}, {y: {type: array, items: double}}]
        ret: double
        do: {m.kernel.poly: [x, y, gamma, intercept, degree]}
""")
        self.assertAlmostEqual(engine.action([0.0, 1.0]), 1.23696154, places=3)
        self.assertAlmostEqual(engine.action([3.0, 3.0]), -0.1762974, places=3)


    def testSvmRBFKernel(self):
        # reproduces scikit-learn SVC result
        """
        import numpy as np
        from sklearn.svm import SVC
	np.random.seed(503)
	X = np.r_[np.random.randn(4,2)+4 - [1,1], np.random.randn(4,2) + [1,1]]
	Y = [0]*4 + [1]*4
	clf = SVC(kernel="rbf", gamma=0.1)
	clf.fit(X, Y)

        # using parameters:
	supVec = clf.support_vectors_
	nv     = clf.n_support_
	coeff  = clf.dual_coef_.flatten()
	const = clf._intercept_ #(reverse sign, weird convention/bug in scikit)
        # reproduces
        print clf.decision_function([0, 1])
        print clf.decision_function([3, 3])
        """

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: TwoClass
      fields:
        - {name: const, type: double}
        - name: posClass 
          type: 
            type: array
            items: 
              type: record
              name: Class1Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}
        - name: negClass 
          type: 
            type: array
            items: 
              type: record
              name: Class2Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}

    init: 
      {const: 0.19901071,
       posClass: [{supVec: [2.96989384, 2.60281757], coeff: -1.0},
                  {supVec: [4.26890398, 3.91296131], coeff: -0.52019712},
                  {supVec: [4.70758215, 3.36311393], coeff: -0.27706044}],
       negClass: [{supVec: [0.43689046, 2.45981766], coeff:  0.94571794},
                  {supVec: [1.47126216, 0.48686121], coeff:  0.85153962}]}

action:
  - let: {gamma: 0.1}
  - model.svm.score:
      - input
      - cell: model
      - params: [{x: {type: array, items: double}}, {y: {type: array, items: double}}]
        ret: double
        do: {m.kernel.rbf: [x, y, gamma]}
""")
        self.assertAlmostEqual(engine.action([0.0, 1.0]),  1.24330894, places=3)
        self.assertAlmostEqual(engine.action([3.0, 3.0]), -0.56231871, places=3)


    def testSvmSigmoidKernel(self):
        # reproduces scikit-learn SVC result
        """
        import numpy as np
        from sklearn.svm import SVC
	np.random.seed(503)
	X = np.r_[np.random.randn(4,2)+4 - [1,1], np.random.randn(4,2) + [1,1]]
	Y = [0]*4 + [1]*4
	clf = SVC(kernel="sigmoid", gamma=0.2, coef0=.4)
	clf.fit(X, Y)

        # using parameters:
	supVec = clf.support_vectors_
	nv     = clf.n_support_
	coeff  = clf.dual_coef_.flatten()
	const = clf._intercept_ #(reverse sign, weird convention/bug in scikit)
        # reproduces
        print clf.decision_function([0, 1])
        print clf.decision_function([3, 3])
        """

        engine, = PFAEngine.fromYaml("""
input: {type: array, items: double}
output: double
cells:
  model:
    type:
      type: record
      name: TwoClass
      fields:
        - {name: const, type: double}
        - name: posClass 
          type: 
            type: array
            items: 
              type: record
              name: Class1Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}
        - name: negClass 
          type: 
            type: array
            items: 
              type: record
              name: Class2Vectors
              fields:
                - {name: supVec, type: {type: array, items: double}}
                - {name: coeff, type: double}

    init: 
      {const: -0.05318959,
       posClass: [{supVec: [ 2.96989384, 2.60281757], coeff: -1.0},
                  {supVec: [ 4.31554013, 3.47119604], coeff: -1.0},
                  {supVec: [ 4.26890398, 3.91296131], coeff: -1.0},
                  {supVec: [ 4.70758215, 3.36311393], coeff: -1.0}],
       negClass: [{supVec: [ 0.43689046, 2.45981766], coeff:  1.0},
                  {supVec: [ 1.47126216, 0.48686121], coeff:  1.0},
                  {supVec: [ 0.60310957, 1.60019866], coeff:  1.0},
                  {supVec: [-1.2920146 , 1.55663529], coeff:  1.0}]}

action:
  - let: {gamma: 0.2}
  - let: {intercept: 0.4}

  - model.svm.score:
      - input
      - cell: model
      - params: [{x: {type: array, items: double}}, {y: {type: array, items: double}}]
        ret: double
        do: {m.kernel.sigmoid: [x, y, gamma, intercept]}
""")
        self.assertAlmostEqual(engine.action([0.0, 1.0]), -0.79563574, places=3)
        self.assertAlmostEqual(engine.action([3.0, 3.0]), -0.71633509, places=3)


