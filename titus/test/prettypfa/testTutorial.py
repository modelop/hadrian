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

import titus.prettypfa
from titus.genpy import PFAEngine
from titus.errors import PFAInitializationException, PFAUserException

# All of the tutorial examples as PrettyPFA

class TestTutorial(unittest.TestCase):
    def fallbackCheck(self, result, expectedResult):
        try:
            self.assertEqual(result, expectedResult)
        except AssertionError:
            if isinstance(result, float) and isinstance(expectedResult, float):
                self.assertAlmostEqual(result, expectedResult, places=5)
            elif isinstance(result, (list, tuple)) and isinstance(expectedResult, (list, tuple)) and len(result) == len(expectedResult):
                for x, y in zip(result, expectedResult):
                    self.fallbackCheck(x, y)
            else:
                raise

    def check(self, inputs, pfa, outputs, allowedExceptions=()):
        inputs = map(json.loads, inputs.strip().split("\n"))
        outputs = map(json.loads, outputs.strip().split("\n"))

        engine, = titus.prettypfa.engine(pfa)

        if engine.config.method == "emit":
            outputs.reverse()
            engine.emit = lambda result: self.assertEqual(result, outputs.pop())
                
            for datum in inputs:
                try:
                    engine.action(datum)
                except Exception as err:
                    if isinstance(err, allowedExceptions):
                        pass
                    else:
                        raise
        else:
            index = 0
            for datum in inputs:
                try:
                    result = engine.action(datum)
                except Exception as err:
                    if isinstance(err, allowedExceptions):
                        pass
                    else:
                        raise
                else:
                    self.fallbackCheck(result, outputs[index])
                    index += 1

    def testTutorial1_1(self):
        self.check('''
1
2
3
''', r'''
input: double
output: double
action: input + 100
''', '''
101.0
102.0
103.0''')

    def testTutorial1_2(self):
        self.check('''
1
2
3
''', r'''
input: double
output: double
action: m.round(m.sin(input + 100) * 100)
''', '''
45.0
99.0
62.0''')

    def testTutorial1_3(self):
        self.check('''
1
2
3
''', r'''
input: double
output: double
action:
    m.round(m.sin(input + 100) * 100);
''', '''
45.0
99.0
62.0''')

    def testTutorial1_4(self):
        self.check('''
1
2
3
4
5
''', r'''
input: double
output: double
action: m.sqrt(input)
''', '''
1.0
1.4142135623730951
1.7320508075688772
2.0
2.23606797749979
''')

    def testTutorial1_5(self):
        self.check('''
1
2
3
4
5
''', r'''
input: double
output: double
method: emit
action:
    if (input % 2 == 0)
        emit(input / 2)
''', '''
1.0
2.0
''')

    def testTutorial1_6(self):
        self.check('''
1
2
3
4
5
''', r'''
input: double
output: double
method: fold
zero: 0
action: input + tally
merge: tallyOne + tallyTwo
''', '''
1.0
3.0
6.0
10.0
15.0
''')

    def testTutorial1_7(self):
        self.check('''
1
2
3
4
5
''', r'''
input: int
output: string
method: fold
zero: ""
action:
  s.concat(tally, s.int(input))
merge:
  s.concat(tallyOne, tallyTwo)
''', '''
"1"
"12"
"123"
"1234"
"12345"
''')

    def testTutorial2_2(self):
        self.check('''
{"name": "Sun", "x": 0.0, "y": 0.0, "z": 0.0, "spec": "G2 V", "planets": true, "mag": {"double": -26.72}}
{"name": "Proxima Centauri", "x": 2.94, "y": -3.05, "z": -0.14, "spec": "M5 Ve", "planets": false, "mag": {"double": 11.05}}
{"name": "Alpha Centauri A", "x": 3.13, "y": -3.05, "z": -0.05, "spec": "G2 V", "planets": false, "mag": {"double": 0.01}}
{"name": "Alpha Centauri B", "x": 3.13, "y": -3.05, "z": -0.05, "spec": "K0 V", "planets": false, "mag": {"double": 1.34}}
{"name": "Alpha Centauri Bb", "x": 3.13, "y": -3.05, "z": -0.05, "spec": "", "planets": false, "mag": null}
{"name": "Barnard's Star", "x": 4.97, "y": 2.99, "z": 1.45, "spec": "M3.5 V", "planets": false, "mag": {"double": 9.57}}
{"name": "Luhman 16 A", "x": 1.72, "y": -6.32, "z": 0.61, "spec": "L7.5", "planets": false, "mag": null}
{"name": "Luhman 16 B", "x": 1.72, "y": -6.32, "z": 0.61, "spec": "T0.5", "planets": false, "mag": null}
{"name": "Wolf 359", "x": -1.90, "y": -3.90, "z": 6.46, "spec": "M5.5 V", "planets": false, "mag": {"double": 13.53}}
{"name": "Lalande 21185", "x": -3.44, "y": -0.31, "z": 7.54, "spec": "M2 V", "planets": false, "mag": {"double": 7.47}}
{"name": "Sirius A", "x": -5.76, "y": -6.22, "z": -1.33, "spec": "A1 V", "planets": false, "mag": {"double": -1.43}}
{"name": "Sirius B", "x": -5.76, "y": -6.22, "z": -1.33, "spec": "DA2", "planets": false, "mag": {"double": 8.44}}
{"name": "Luyten 726-8 A", "x": -2.15, "y": 0.17, "z": -8.46, "spec": "M5.5 V", "planets": false, "mag": {"double": 12.61}}
{"name": "Luyten 726-8 B", "x": -2.15, "y": 0.17, "z": -8.46, "spec": "M6 V", "planets": false, "mag": {"double": 13.06}}
{"name": "WISEP J154151.66-225025.2", "x": 8.17, "y": -1.95, "z": 3.96, "spec": "Y0.5", "planets": false, "mag": null}
{"name": "Ross 154", "x": 9.33, "y": 1.87, "z": -1.73, "spec": "M3.5 Ve", "planets": false, "mag": {"double": 10.44}}
{"name": "WISEPC J205628.90+145953.3", "x": 4.34, "y": 8.16, "z": -3.22, "spec": "Y0", "planets": false, "mag": null}
{"name": "Ross 248", "x": -3.37, "y": 9.27, "z": -3.00, "spec": "M5.5 V", "planets": false, "mag": {"double": 12.29}}
{"name": "Epsilon Eridani", "x": -6.74, "y": -1.91, "z": -7.79, "spec": "K2 V", "planets": false, "mag": {"double": 3.73}}
{"name": "Epsilon Eridani b", "x": -6.74, "y": -1.91, "z": -7.79, "spec": "", "planets": true, "mag": null}
{"name": "Epsilon Eridani c", "x": -6.75, "y": -1.91, "z": -7.80, "spec": "", "planets": false, "mag": null}
''', r'''
input: record(name:    string,
              x:       double,
              y:       double,
              z:       double,
              spec:    string,
              planets: boolean,
              mag:     union(double, null))
output: double

action: m.sqrt(a.sum(new(array(double), input.x**2, input.y**2, input.z**2)))

''', '''
0.0
4.23859646581271
4.37057204493874
4.37057204493874
4.37057204493874
5.9785867895347975
6.578214043340336
6.578214043340336
7.781490859726046
8.293449222127064
8.581078020854955
8.581078020854955
8.73057844589922
8.73057844589922
9.286172516166173
9.671540725241249
9.787216151695027
10.30969446685982
10.476631137918334
10.476631137918334
10.490500464706152
''')

    def testTutorial2_3(self):
        self.check('''
{"name": "Sun", "x": 0.0, "y": 0.0, "z": 0.0, "spec": "G2 V", "planets": true, "mag": {"double": -26.72}}
{"name": "Proxima Centauri", "x": 2.94, "y": -3.05, "z": -0.14, "spec": "M5 Ve", "planets": false, "mag": {"double": 11.05}}
{"name": "Alpha Centauri A", "x": 3.13, "y": -3.05, "z": -0.05, "spec": "G2 V", "planets": false, "mag": {"double": 0.01}}
{"name": "Alpha Centauri B", "x": 3.13, "y": -3.05, "z": -0.05, "spec": "K0 V", "planets": false, "mag": {"double": 1.34}}
{"name": "Alpha Centauri Bb", "x": 3.13, "y": -3.05, "z": -0.05, "spec": "", "planets": false, "mag": null}
{"name": "Barnard's Star", "x": 4.97, "y": 2.99, "z": 1.45, "spec": "M3.5 V", "planets": false, "mag": {"double": 9.57}}
{"name": "Luhman 16 A", "x": 1.72, "y": -6.32, "z": 0.61, "spec": "L7.5", "planets": false, "mag": null}
{"name": "Luhman 16 B", "x": 1.72, "y": -6.32, "z": 0.61, "spec": "T0.5", "planets": false, "mag": null}
''', r'''
input: record(name:    string,
              x:       double,
              y:       double,
              z:       double,
              spec:    string,
              planets: boolean,
              mag:     union(double, null))
output: double
method: emit

action:
    cast (input.mag) {
        as(magDouble: double) emit(magDouble)
        as(magNull: null) null
    }
''', '''
-26.72
11.05
0.01
1.34
9.57
''')

    def testTutorial2_4(self):
        self.check('''
1
2
3
4
5
''', r'''
input: double
output: double
action:
    var x = input;
    var y = (input + 1) * input;
    y = y / input;
    y - 1
''', '''
1.0
2.0
3.0
4.0
5.0
''')

    def testTutorial2_5(self):
        self.check('''
1
2
3
4
5
''', r'''
input: double
output: double
action:
    var x = input + 1,
        y = input + 2,
        z = input + 3;
    var a = x + y - z,
        b = x * y / z;
    a / b
''', '''
0.6666666666666666
0.8333333333333334
0.8999999999999999
0.9333333333333333
0.9523809523809523
''')

    def testTutorial2_6(self):
        self.check('''
1
2
3
4
5
''', r'''
input: int
output: int
method: emit
action:
    if (input % 2 == 0)
        emit(input)
''', '''
2
4
''')

    def testTutorial2_7(self):
        self.check('''
1
2
3
4
5
''', r'''
input: int
output: string
action:
    if (input % 2 == 0)
        "even"
    else
        "odd"
''', '''
"odd"
"even"
"odd"
"even"
"odd"
''')

    def testTutorial2_8(self):
        self.check('''
0
1
2
3
4
5
''', r'''
input: int
output: string
action:
    if (input % 3 == 0)
        "off"
    else if (input % 3 == 1)
        "on"
    else
         "high impedance"
''', '''
"off"
"on"
"high impedance"
"off"
"on"
"high impedance"
''')

    def testTutorial2_9(self):
        self.check('''
null
''', r'''
input: null
output: int
method: emit
action:
    var i = 0;
    while (i < 10) {
        i = i + 1;
        emit(i)
    }
''', '''
1
2
3
4
5
6
7
8
9
10
''')

    def testTutorial2_10(self):
        self.check('''
null
''', r'''
input: null
output: int
method: emit
action:
    var i = 0;
    do {
        i = i + 1;
        emit(i)
    } until (i == 10)
''', '''
1
2
3
4
5
6
7
8
9
10
''')

    def testTutorial2_11(self):
        self.check('''
["hello", "my", "ragtime", "gal"]
''', r'''
input: array(string)
output: string
method: emit
action:
    for (i = 0;  i < 4;  i = i + 1)
        emit(input[i]);

    foreach (x : input)
        emit(x);

    foreach (k, v : new(map(int), one: 1, two: 2, three: 3))
        emit(k);

''', '''
"hello"
"my"
"ragtime"
"gal"
"hello"
"my"
"ragtime"
"gal"
"three"
"two"
"one"
''')

    def testTutorial2_12(self):
        self.check('''
1
2
3
4
5
''', r'''
input: int
output: int
action: u.squared(u.cubed(input))
fcns:
  squared = fcn(x: int -> int) {
      x * x
  };

  cubed = fcn(x: int -> int) {
      x * u.squared(x)
  }

''', '''
1
64
729
4096
15625
''')

    def testTutorial2_13(self):
        self.check('''
["hello", "my", "darling", "hello", "my", "honey", "hello", "my", "ragtime", "gal"]
''', r'''
input: array(string)
output: string
action: a.maxLT(input, u.customLessThan)
fcns:
  customLessThan = fcn(x: string, y: string -> boolean)
      s.len(x) < s.len(y)
''', '''
"darling"
''')

    def testTutorial2_14(self):
        self.check('''
true
false
''', r'''
input: boolean
output: array(int)
action:
    var sortme = json(array(int), [23, 55, 18, 62, 4, 99]);
    a.sortLT(sortme,
             fcn(x: int, y: int -> boolean) if (input) x < y else x > y)
''', '''
[4,18,23,55,62,99]
[99,62,55,23,18,4]
''')

    def testTutorial2_15(self):
        self.check('''
5
25
''', r'''
input: int
output: array(double)
action: u.bernoulli(input)
fcns:
  bernoulli = fcn(N: int -> array(double)) {
      var BN = new(array(double), 1, -0.5);
      for (M = 2;  M <= N;  M = M + 1) {
          var S = -(1/(M + 1) - 0.5);
          for (K = 2;  K != M;  K = K + 1) {
              var R = 1.0;
              for (J = 2;  J <= K;  J = J + 1)
                  R = R*(J + M - K)/J;
              S = S - R*BN[K];
          };
          BN = a.append(BN, S);
      };

      for (M = 3;  M <= N;  M = M + 2)
          BN = a.replace(BN, M, 0);
      BN
  }
''', '''
[1.0,-0.5,0.16666666666666669,0.0,-0.03333333333333338,0.0]
[1.0,-0.5,0.16666666666666669,0.0,-0.03333333333333338,0.0,0.023809523809523808,0.0,-0.03333333333333302,0.0,0.07575757575757641,0.0,-0.2531135531135573,0.0,1.166666666666674,0.0,-7.0921568627451705,0.0,54.97117794486118,0.0,-529.12424242423,0.0,6192.123188405604,0.0,-86580.25311355002,0.0]
''')

    def testTutorial3_1(self):
        self.check('''
"hello"
"my"
"darling"
"hello"
"my"
"honey"
"hello"
"my"
"ragtime"
"gal"
''', r'''
input: string
output: string
cells:
  longest(string) = ""
action:
  if (s.len(input) > s.len(longest)) {
      longest = input;
      input
  }
  else
      longest
''', '''
"hello"
"hello"
"darling"
"darling"
"darling"
"darling"
"darling"
"darling"
"darling"
"darling"
''')

    def testTutorial3_2(self):
        self.check('''
"hello"
"my"
"darling"
"hello"
"my"
"honey"
"hello"
"my"
"ragtime"
"gal"
''', r'''
input: string
output: int
pools:
  wordCount(int) = {}
action:
  wordCount[input] to fcn(x: int -> int) x + 1 init 0;
  wordCount["hello"]
''', '''
1
1
1
2
2
2
3
3
3
3
''')

    def testTutorial3_3(self):
        self.assertRaises(PFAInitializationException, lambda: titus.prettypfa.engine(r'''
input: string
output: int
cells:
  one(int) = 1;
  two(int) = 2;
action:
  one to u.changeOne
fcns:
  changeOne = fcn(x: int -> int) u.functionThatCallsChangeTwo();
  functionThatCallsChangeTwo = fcn(-> int) {
      two to u.changeTwo;
      1
  };
  changeTwo = fcn(x: int -> int) 2
'''))

    def testTutorial3_4(self):
        self.check('''
{"one": 11, "two": 3.6, "three": "TEST"}
{"one": 11, "two": 3.4, "three": "TEST"}
{"one": 13, "two": 3.6, "three": "TEST"}
{"one": 13, "two": 3.6, "three": "NOT-TEST"}
''', r'''
input: record(one: int, two: double, three: string, Datum)
output: string
cells:
  tree(record(field:    enum([one, two, three], TreeFields),
              operator: string,
              value:    union(double, string),
              pass:     union(string, TreeNode),
              fail:     union(string, TreeNode), TreeNode)) =
      {field: one,
       operator: "<",
       value: {double: 12},
       pass: {TreeNode: {
                  field: two,
                  operator: ">",
                  value: {double: 3.5},
                  pass: {string: "yes-yes"},
                  fail: {string: "yes-no"}}},
       fail: {TreeNode: {
                  field: three,
                  operator: "==",
                  value: {string: TEST},
                  pass: {string: "no-yes"},
                  fail: {string: "no-no"}}}}

action:
  model.tree.simpleWalk(input, tree, fcn(d: Datum, t: TreeNode -> boolean) model.tree.simpleTest(d, t))
''', '''
"yes-yes"
"yes-no"
"no-yes"
"no-no"
''')

    def testTutorial3_5(self):
        self.check('''
1
2
3
4
5
''', r'''
input: int
output: int
cells:
  counter(int, rollback: true) = 0
action:
  counter to fcn(x: int -> int) x + 1;
  if (input < 4)
      error("This one is too small.");
  counter
''', '''
1
2
''', PFAUserException)

    def testTutorial3_6(self):
        self.check('''
1
2
3
4
5
''', r'''
input: int
output: int
action:
  if (input > 3)
      u.callfunc(input);
  input
fcns:
  callfunc = fcn(x: int -> null) log("enter callfunc", x)
''', '''
1
2
3
4
5
''', PFAUserException)

    def testModels_1(self):
        self.check('''
[1.2, 1.2, 1.2, 1.2, 1.2]
[1.8, 1.8, 1.8, 1.8, 1.8]
[2.2, 2.2, 2.2, 2.2, 2.2]
[5.0, 5.0, 5.0, 5.0, 5.0]
[-1000.0, -1000.0, -1000.0, -1000.0, -1000.0]
''', r'''
input: array(double)
output: string
cells:
  clusters(array(record(center: array(double), id: string, Cluster))) =
      [{id: one, center: [1, 1, 1, 1, 1]},
       {id: two, center: [2, 2, 2, 2, 2]},
       {id: three, center: [3, 3, 3, 3, 3]},
       {id: four, center: [4, 4, 4, 4, 4]},
       {id: five, center: [5, 5, 5, 5, 5]}]
action:
  model.cluster.closest(input,
                        clusters,
                        fcn(x: array(double), y: array(double) -> double)
                            metric.euclidean(metric.absDiff, x, y))["id"]
''', '''
"one"
"two"
"two"
"five"
"one"
''', PFAUserException)

    def testModels_2(self):
        self.check('''
{"one": 1, "two": 7, "three": "whatever"}
{"one": 1, "two": 0, "three": "whatever"}
{"one": 15, "two": 7, "three": "TEST"}
{"one": 15, "two": 7, "three": "ZEST"}
''', r'''
input: record(one: int, two: double, three: string, Datum)
output: string
cells:
  tree(record(field: enum([one, two, three], Fields),
              operator: string,
              value: union(int, double, string),
              pass: union(string, TreeNode),
              fail: union(string, TreeNode), TreeNode)) =
      {field: one,
       operator: "<",
       value: {double: 12},
       pass: {TreeNode: {
                  field: two,
                  operator: ">",
                  value: {double: 3.5},
                  pass: {string: "yes-yes"},
                  fail: {string: "yes-no"}}},
       fail: {TreeNode: {
                  field: three,
                  operator: "==",
                  value: {string: TEST},
                  pass: {string: "no-yes"},
                  fail: {string: "no-no"}}}}

action:
  model.tree.simpleWalk(input, tree,
      fcn(d: Datum, t: TreeNode -> boolean) model.tree.simpleTest(d, t))
''', '''
"yes-yes"
"yes-no"
"no-yes"
"no-no"
''', PFAUserException)

    def testModels_3(self):
        self.check('''
3.35
-1.37
-3.92
6.74
12.06
3.81
3.35
-1.18
-1.39
5.55
5.3
12.8
10.36
12.05
3.8
12.81
11.1
8.37
7.32
15.22
''', r'''
input: double
output: boolean
cells:
  last(double) = 0.0

method: emit
action:
  last to fcn(oldValue: double -> double) {
      var newValue = stat.change.updateCUSUM(
          // alternate minus baseline
          prob.dist.gaussianLL(input, 10.0, 3.0) - prob.dist.gaussianLL(input, 2.0, 5.0),
          oldValue,
          0.0);      // resetValue = 0.0
      emit(newValue > 5.0);
      newValue
  }
''', '''
false
false
false
false
false
false
false
false
false
false
false
false
false
true
true
true
true
true
true
true
''', PFAUserException)

    def testModels_4(self):
        self.check('''
{"key": "one", "value": 100.1}
{"key": "one", "value": 101.3}
{"key": "one", "value": 100.9}
{"key": "one", "value": 101.1}
{"key": "one", "value": 101.0}
{"key": "two", "value": 202.1}
{"key": "two", "value": 202.3}
{"key": "two", "value": 202.9}
{"key": "two", "value": 202.1}
{"key": "two", "value": 202.0}
{"key": "one", "value": 100.1}
{"key": "one", "value": 101.3}
{"key": "one", "value": 100.9}
{"key": "one", "value": 101.1}
{"key": "one", "value": 101.0}
{"key": "two", "value": 202.1}
{"key": "two", "value": 202.3}
{"key": "two", "value": 202.9}
{"key": "two", "value": 202.1}
{"key": "two", "value": 202.0}
''', r'''
input: record(key: string, value: double, Input)
output: record(key: string, zValue: double, Output)
pools:
  counters(record(count: double, mean: double, variance: double, Counter)) = {}

method: emit
action:
  counters[input.key] to fcn(oldCounter: Counter -> Counter) {
      var newCounter = stat.sample.update(input.value, 1.0, oldCounter);
      if (newCounter.count > 3)
          emit(new(Output, key: input.key, zValue: stat.change.zValue(input.value, newCounter, false)));
      newCounter
  } init json(Counter, {count: 0.0, mean: 0.0, variance: 0.0})
''', '''
{"key":"one","zValue":0.54882129994845}
{"key":"one","zValue":0.29138575870718914}
{"key":"two","zValue":-0.7624928516630021}
{"key":"two","zValue":-0.8616404368553157}
{"key":"one","zValue":-1.3677897164722017}
{"key":"one","zValue":0.9816907935594097}
{"key":"one","zValue":0.13894250359421337}
{"key":"one","zValue":0.5400617248673225}
{"key":"one","zValue":0.2913857587071894}
{"key":"two","zValue":-0.49319696191608037}
{"key":"two","zValue":0.15191090506255248}
{"key":"two","zValue":1.659850005517447}
{"key":"two","zValue":-0.6434211884046508}
{"key":"two","zValue":-0.8616404368553247}
''', PFAUserException)

if __name__ == "__main__":
    unittest.main()

