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

from titus.reader import yamlToAst
from titus.genpy import PFAEngine
from titus.errors import *
    
def unsigned(x):
    if x < 0:
        return chr(x + 256)
    else:
        return chr(x)
    
def signed(x):
    if ord(x) >= 128:
        return ord(x) - 256
    else:
        return ord(x)

class TestGeneratePython(unittest.TestCase):
    def testMetadataAccessName(self):
        engine, = PFAEngine.fromYaml('''
name: ThisIsMyName
input: "null"
output: string
method: map
action: {s.concat: [name, {string: "!!!"}]}
''')
        self.assertEqual(engine.action(None), "ThisIsMyName!!!")

        engine, = PFAEngine.fromYaml('''
name: ThisIsMyName
input: "null"
output: string
method: emit
action: {emit: {s.concat: [name, {string: "!!!"}]}}
''')
        engine.emit = lambda x: self.assertEqual(x, "ThisIsMyName!!!")
        engine.action(None)

        engine, = PFAEngine.fromYaml('''
name: ThisIsMyName
input: "null"
output: string
method: fold
zero: "!!!"
action: {s.concat: [name, tally]}
merge: {s.concat: [tallyOne, tallyTwo]}
''')
        self.assertEqual(engine.action(None), "ThisIsMyName!!!")

        engine, = PFAEngine.fromYaml('''
name: ThisIsMyName
input: "null"
output: "null"
action: null
begin: {log: {s.concat: [name, {string: "!!!"}]}}
''')
        engine.log = lambda x, ns: self.assertEqual(x, ["ThisIsMyName!!!"])
        engine.begin()

        engine, = PFAEngine.fromYaml('''
name: ThisIsMyName
input: "null"
output: "null"
action: null
end: {log: {s.concat: [name, {string: "!!!"}]}}
''')
        engine.log = lambda x, ns: self.assertEqual(x, ["ThisIsMyName!!!"])
        engine.end()

        engines = PFAEngine.fromYaml('''
name: WeAllHaveTheSameName
input: "null"
output: {type: array, items: string}
action:
  - let: {out: {type: {type: array, items: string}, value: []}}
  - for: {i: 0}
    while: {"<": [i, instance]}
    step: {i: {+: [i, 1]}}
    do:
      set: {out: {a.append: [out, name]}}
  - out
''', multiplicity=10)
        for index, engine in enumerate(engines):
            output = engine.action(None)
            self.assertEqual(len(output), index)
            if len(output) > 0:
                self.assertEqual(output[0], "WeAllHaveTheSameName")

    def testMetadataRefuseToChangeName(self):
        def bad():
            PFAEngine.fromYaml('''
name: ThisIsMyName
input: "null"
output: "null"
method: map
action:
  - set: {name: {string: SomethingElse}}
  - null
''')
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
name: ThisIsMyName
input: "null"
output: "null"
method: emit
action:
  - set: {name: {string: SomethingElse}}
''')
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
name: ThisIsMyName
input: "null"
output: "null"
method: fold
zero: null
action:
  - set: {name: {string: SomethingElse}}
  - null
merge: null
''')
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
name: ThisIsMyName
input: "null"
output: "null"
action: null
begin:
  - set: {name: {string: SomethingElse}}
''')
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
name: ThisIsMyName
input: "null"
output: "null"
action: null
end:
  - set: {name: {string: SomethingElse}}
''')
        self.assertRaises(PFASemanticException, bad)

    def testMetadataAccessVersion(self):
        engine, = PFAEngine.fromYaml('''
version: 123
input: "null"
output: int
method: map
action: {+: [version, 1000]}
''')
        self.assertEqual(engine.action(None), 1123)

        engine, = PFAEngine.fromYaml('''
version: 123
input: "null"
output: int
method: emit
action: {emit: {+: [version, 1000]}}
''')
        engine.emit = lambda x: self.assertEqual(x, 1123)
        engine.action(None)

        engine, = PFAEngine.fromYaml('''
version: 123
input: "null"
output: int
method: fold
zero: 1000
action: {+: [version, tally]}
merge: {+: [tallyOne, tallyTwo]}
''')
        self.assertEqual(engine.action(None), 1123)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: int
method: map
action: {+: [version, 1000]}
''')
        self.assertRaises(PFASemanticException, bad)

        engine, = PFAEngine.fromYaml('''
version: 123
input: "null"
output: "null"
action: null
begin: {log: {+: [version, 1000]}}
''')
        engine.log = lambda x, ns: self.assertEqual(x, [1123])
        engine.begin()

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: "null"
action: null
begin: {+: [version, 1000]}
''')
        self.assertRaises(PFASemanticException, bad)

        engine, = PFAEngine.fromYaml('''
version: 123
input: "null"
output: "null"
action: null
end: {log: {+: [version, 1000]}}
''')
        engine.log = lambda x, ns: self.assertEqual(x, [1123])
        engine.end()

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: "null"
action: null
end: {+: [version, 1000]}
''')
        self.assertRaises(PFASemanticException, bad)

    def testMetadataAccessMetadata(self):
        engine, = PFAEngine.fromYaml('''
metadata: {hello: there}
input: "null"
output: string
method: map
action: metadata.hello
''')
        self.assertEqual(engine.action(None), "there")

        engine, = PFAEngine.fromYaml('''
metadata: {hello: there}
input: "null"
output: string
method: emit
action: {emit: metadata.hello}
''')
        engine.emit = lambda x: self.assertEqual(x, "there")
        engine.action(None)

        engine, = PFAEngine.fromYaml('''
metadata: {hello: there}
input: "null"
output: string
method: fold
zero: "!!!"
action: {s.concat: [metadata.hello, tally]}
merge: {s.concat: [tallyOne, tallyTwo]}
''')
        self.assertEqual(engine.action(None), "there!!!")

        engine, = PFAEngine.fromYaml('''
metadata: {hello: there}
input: "null"
output: "null"
action: null
begin: {log: metadata.hello}
''')
        engine.log = lambda x, ns: self.assertEqual(x, ["there"])
        engine.begin()

        engine, = PFAEngine.fromYaml('''
metadata: {hello: there}
input: "null"
output: "null"
action: null
end: {log: metadata.hello}
''')
        engine.log = lambda x, ns: self.assertEqual(x, ["there"])
        engine.end()

    def testMetadataCountSuccessfulAndUnsuccessfulActionsInAMapTypeEngine(self):
        engine, = PFAEngine.fromYaml('''
input: boolean
output: {type: array, items: long}
method: map
action:
  if: input
  then:
    new: [actionsStarted, actionsFinished]
    type: {type: array, items: long}
  else:
    - error: "yowzers!"
    - {new: [], type: {type: array, items: long}}
''')
        try:
            self.assertEqual(engine.action(True), [1, 0])
        except:
            pass

        try:
            self.assertEqual(engine.action(True), [2, 1])
        except:
            pass

        try:
            engine.action(False)
        except:
            pass

        try:
            engine.action(False)
        except:
            pass

        try:
            self.assertEqual(engine.action(True), [5, 2])
        except:
            pass
        
    def testMetadataCountSuccessfulAndUnsuccessfulActionsInAnEmitTypeEngine(self):
        engine, = PFAEngine.fromYaml('''
input: boolean
output: {type: array, items: long}
method: emit
action:
  if: input
  then:
    emit:
      new: [actionsStarted, actionsFinished]
      type: {type: array, items: long}
  else:
    - error: "yowzers!"
''')
        try:
            engine.emit = lambda x: self.assertEqual(x, [1, 0])
            engin.action(True)
        except:
            pass

        try:
            engine.emit = lambda x: self.assertEqual(x, [2, 1])
            engin.action(True)
        except:
            pass

        try:
            engin.action(False)
        except:
            pass

        try:
            engin.action(False)
        except:
            pass

        try:
            engine.emit = lambda x: self.assertEqual(x, [5, 2])
            engin.action(True)
        except:
            pass

    def testMetadataCountSuccessfulAndUnsuccessfulActionsInAFoldTypeEngine(self):
        engine, = PFAEngine.fromYaml('''
input: boolean
output: {type: array, items: long}
method: fold
zero: []
action:
  if: input
  then:
    new: [actionsStarted, actionsFinished]
    type: {type: array, items: long}
  else:
    - error: "yowzers!"
    - {new: [], type: {type: array, items: long}}
merge: {a.concat: [tallyOne, tallyTwo]}
''')
        try:
            self.assertEqual(engine.action(True), [1, 0])
        except:
            pass

        try:
            self.assertEqual(engine.action(True), [2, 1])
        except:
            pass

        try:
            engine.action(False)
        except:
            pass

        try:
            engine.action(False)
        except:
            pass

        try:
            self.assertEqual(engine.action(True), [5, 2])
        except:
            pass

    def testCorrectlyUseMerge(self):
        engineOne, engineTwo = PFAEngine.fromYaml('''
input: int
output: string
method: fold
zero: "-"
action: {s.concat: [tally, {s.int: input}]}
merge: {s.concat: [tallyOne, tallyTwo]}
''', multiplicity=2)

        self.assertEqual(engineOne.action(1), "-1")
        self.assertEqual(engineOne.action(2), "-12")
        self.assertEqual(engineOne.action(3), "-123")
        self.assertEqual(engineOne.action(4), "-1234")

        self.assertEqual(engineTwo.action(9), "-9")
        self.assertEqual(engineTwo.action(8), "-98")
        self.assertEqual(engineTwo.action(7), "-987")
        self.assertEqual(engineTwo.action(6), "-9876")

        self.assertEqual(engineOne.merge(engineOne.tally, engineTwo.tally), "-1234-9876")
        self.assertEqual(engineOne.action(5), "-1234-98765")

        self.assertEqual(engineTwo.action(5), "-98765")

    def testLiteralNull(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action: null
''')
        self.assertEqual(engine.action(None), None)

    def testLiteralBoolean(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action: true
''')
        self.assertEqual(engine.action(None), True)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: boolean
action: false
''')
        self.assertEqual(engine.action(None), False)

    def testLiteralInt(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action: 12
''')
        self.assertEqual(engine.action(None), 12)
        self.assertTrue(isinstance(engine.action(None), (int, long)))

    def testLiteralLong(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: long
action: {long: 12}
''')
        self.assertEqual(engine.action(None), 12)
        self.assertTrue(isinstance(engine.action(None), (int, long)))

    def testLiteralFloat(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: float
action: {float: 12}
''')
        self.assertEqual(engine.action(None), 12.0)
        self.assertTrue(isinstance(engine.action(None), float))

    def testLiteralDouble(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action: 12.4
''')
        self.assertEqual(engine.action(None), 12.4)
        self.assertTrue(isinstance(engine.action(None), float))

    def testLiteralString(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action: [["hello world"]]
''')
        self.assertEqual(engine.action(None), "hello world")
        self.assertTrue(isinstance(engine.action(None), basestring))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action: {string: "hello world"}
''')
        self.assertEqual(engine.action(None), "hello world")
        self.assertTrue(isinstance(engine.action(None), basestring))

    def testLiteralBase64(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: bytes
action: {base64: "aGVsbG8="}
''')
        self.assertEqual(engine.action(None), "hello")
        self.assertTrue(isinstance(engine.action(None), basestring))

    def testComplexLiterals(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: string}
action:
  type: {type: array, items: string}
  value: [one, two, three]
''')
        self.assertEqual(engine.action(None), ["one", "two", "three"])

    def testNewRecord(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
action:
  type: SimpleRecord
  new: {one: 1, two: 2.2, three: ["THREE"]}
''')
        self.assertEqual(engine.action(None), {"one": 1, "two": 2.2, "three": "THREE"})

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
action:
  type: SimpleRecord
  new: {one: {long: 1}, two: 2.2, three: ["THREE"]}
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
action:
  type: SimpleRecord
  new: {one: 1, two: 2.2, three: ["THREE"], four: 4.4}
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
action:
  type: SimpleRecord
  new: {one: 1, two: 2.2}
'''))

    def testRecordWithInlineTypes(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: SimpleRecord
action:
  type: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
  new: {one: 1, two: 2.2, three: ["THREE"]}
''')
        self.assertEqual(engine.action(None), {"one": 1, "two": 2.2, "three": "THREE"})

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: SimpleRecord
action:
  type: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
  new: {one: {long: 1}, two: 2.2, three: ["THREE"]}
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: SimpleRecord
action:
  type: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
  new: {one: 1, two: 2.2, three: ["THREE"], four: 4.4}
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: SimpleRecord
action:
  type: {type: record, name: SimpleRecord, fields: [{name: one, type: int}, {name: two, type: double}, {name: three, type: string}]}
  new: {one: 1, two: 2.2}
'''))

    def testNewMap(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: {type: map, values: int}
action:
  type: {type: map, values: int}
  new: {one: 1, two: 2, three: input}
''')
        self.assertEqual(engine.action(5), {"one": 1, "two": 2, "three": 5})

    def testNewArray(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: {type: array, items: int}
action:
  type: {type: array, items: int}
  new: [1, 2, input]
''')
        self.assertEqual(engine.action(5), [1, 2, 5])

    def collectLogs(self, engine):
        out = []
        def log(message, namespace):
            out.append((namespace, message))

        engine.log = log
        engine.action(None)
        return out

    def testWriteToLogs(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  log: [{string: "hello"}]
''')
        self.assertEqual(self.collectLogs(engine), [(None, ["hello"])])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  log: [1, 2, 3]
''')
        self.assertEqual(self.collectLogs(engine), [(None, [1, 2, 3])])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  log: [[hello]]
''')
        self.assertEqual(self.collectLogs(engine), [(None, ["hello"])])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  log: [[hello]]
  namespace: filter-me
''')
        self.assertEqual(self.collectLogs(engine), [("filter-me", ["hello"])])

    def testVariableDeclarations(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - let: {x: [hello]}
  - x
''')
        self.assertEqual(engine.action(None), "hello")

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - x
  - let: {x: [hello]}
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - let: {x: [hello], y: 12}
  - y
''')
        self.assertEqual(engine.action(None), 12)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - let: {x: [hello], y: x}
  - x
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - let: {x: {let: {y: [stuff]}}}
  - x
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - let:
      x:
        do:
          - {let: {y: [stuff]}}
          - y
  - x
''')
        self.assertEqual(engine.action(None), "stuff")

    def testVariableReassignment(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - let: {x: [hello]}
  - set: {x: [there]}
  - x
''')
        self.assertEqual(engine.action(None), "there")

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - set: {x: [there]}
  - let: {x: [hello]}
  - x
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - let: {x: [hello]}
  - set: {x: 12}
  - x
'''))

    def testCallFunctions(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action: {+: [2, 2]}
''')
        self.assertEqual(engine.action(None), 4)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action: {+: [2, [hello]]}
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action: {+: [{+: [2, 2]}, 2]}
''')
        self.assertEqual(engine.action(None), 6)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action: {+: [{let: {x: 5}}, 2]}
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  +:
    - do: [{let: {x: 5}}, x]
    - 2
''')
        self.assertEqual(engine.action(None), 7)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - let: {x: 5}
  - {+: [x, 2]}
''')
        self.assertEqual(engine.action(None), 7)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 5}}
  - "+":
      - {do: [{let: {x: 5}}, x]}
      - 2
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 5}}
  - "+":
      - {do: [{set: {x: 10}}, x]}
      - 2
'''))

    def testIfExpressions(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  if: true
  then: 3
''')
        self.assertEqual(engine.action(None), None)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  if: [hello]
  then: 3
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 3}}
  - if: true
    then:
      - {set: {x: 99}}
  - x
''')
        self.assertEqual(engine.action(None), 99)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 3}}
  - if: true
    then:
      - {let: {x: 99}}
  - x
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 3}}
  - if: true
    then:
      - {set: {x: 99}}
    else:
      - {set: {x: 55}}
  - x
''')
        self.assertEqual(engine.action(None), 99)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 3}}
  - if: false
    then:
      - {set: {x: 99}}
    else:
      - {set: {x: 55}}
  - x
''')
        self.assertEqual(engine.action(None), 55)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  if: true
  then:
    - 20
  else:
    - 30
''')
        self.assertEqual(engine.action(None), 20)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  if: false
  then:
    - 20
  else:
    - 30
''')
        self.assertEqual(engine.action(None), 30)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: [string, int]
action:
  if: false
  then:
    - 20
  else:
    - [string]
''')
        self.assertEqual(engine.action(None), "string")

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  if: true
  then:
    - {let: {x: 999}}
    - x
  else:
    - 50
''')
        self.assertEqual(engine.action(None), 999)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  if: true
  then:
    - {let: {x: 999}}
    - x
  else:
    - x
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  if: true
  then:
    - x
  else:
    - {let: {x: 999}}
    - x
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  if: {"!=": [2, 3]}
  then:
    - {+: [5, 5]}
  else:
    - {+: [123, 456]}
''')
        self.assertEqual(engine.action(None), 10)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  "+":
    - if: true
      then: [5]
      else: [2]
    - 100
''')
        self.assertEqual(engine.action(None), 105)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - {log: [{string: "one"}]}
  - if: true
    then: [{log: [{string: "two"}]}]
    else: [{log: [{string: "ARG!"}]}]
  - if: false
    then: [{log: [{string: "ARGY-ARG-ARG!"}]}]
    else: [{log: [{string: "three"}]}]
  - if: true
    then: [{log: [{string: "four"}]}]
  - if: false
    then: [{log: [{string: "AAAAAAAAARG!"}]}]
''')
        self.assertEqual(self.collectLogs(engine), [(None, ["one"]), (None, ["two"]), (None, ["three"]), (None, ["four"])])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  if: true
  then:
    - if: true
      then:
        - if: true
          then:
            - {log: [{string: "HERE"}]}
''')
        self.assertEqual(self.collectLogs(engine), [(None, ["HERE"])])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  if: true
  then:
    - if: true
      then:
        - if: true
          then:
            - {log: [{string: "HERE"}]}
          else:
            - {log: [{string: "AAAARG!"}]}
''')
        self.assertEqual(self.collectLogs(engine), [(None, ["HERE"])])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  if: true
  then:
    - if: true
      then:
        - if: true
          then:
            - {log: [{string: "HERE"}]}
      else:
        - {log: [{string: "BOO!"}]}
''')
        self.assertEqual(self.collectLogs(engine), [(None, ["HERE"])])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - if: true
    then:
      - {let: {x: 99}}
    else:
      - {let: {x: 99}}
  - 123
''')
        self.assertEqual(engine.action(None), 123)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  if: true
  then: [1]
  else: [2]
''')
        self.assertEqual(engine.action(None), 1)

    def testCondExpressions(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  cond:
    - {if: false, then: [1]}
    - {if: true, then: [2]}
    - {if: true, then: [3]}
  else: [4]
''')
        self.assertEqual(engine.action(None), 2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  cond:
    - {if: false, then: [1]}
    - {if: false, then: [2]}
    - {if: false, then: [3]}
  else: [4]
''')
        self.assertEqual(engine.action(None), 4)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  cond:
    - {if: false, then: [1]}
    - {if: false, then: [2]}
    - {if: false, then: [3]}
''')
        self.assertEqual(engine.action(None), None)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  cond:
    - {if: false, then: [{let: {x: 5}}, 1]}
    - {if: false, then: [{let: {x: 5}}, 2]}
    - {if: false, then: [{let: {x: 5}}, 3]}
  else: [{let: {x: 5}}, 4]
''')
        self.assertEqual(engine.action(None), 4)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  cond:
    - {if: false, then: [{let: {x: 5}}, 1]}
    - {if: false, then: [{let: {x: 5}}, 2]}
    - {if: false, then: [{let: {x: 5}}, 3]}
''')
        self.assertEqual(engine.action(None), None)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  cond:
    - {if: false, then: [{let: {x: 5}}, 1]}
    - {if: false, then: [{let: {x: 5}}, 2]}
    - {if: false, then: [{let: {x: 5}}, 3]}
  else: [{set: {x: 5}}, 4]
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  cond:
    - {if: false, then: [{let: {x: 5}}, 1]}
    - {if: false, then: [{set: {x: 5}}, 2]}
    - {if: false, then: [{set: {x: 5}}, 3]}
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  cond:
    - {if: {do: [{let: {x: 5}}, false]}, then: [{let: {x: 5}}, 1]}
    - {if: {do: [{let: {x: 5}}, false]}, then: [{let: {x: 5}}, 2]}
    - {if: {do: [{let: {x: 5}}, false]}, then: [{let: {x: 5}}, 3]}
  else: [{let: {x: 5}}, 4]
''')
        self.assertEqual(engine.action(None), 4)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  cond:
    - {if: {do: [{let: {x: 5}}, true]}, then: [{let: {x: 5}}, 1]}
    - {if: {do: [{let: {x: 5}}, false]}, then: [{let: {x: 5}}, 2]}
    - {if: {do: [{let: {x: 5}}, false]}, then: [{let: {x: 5}}, 3]}
  else: [{let: {x: 5}}, 4]
''')
        self.assertEqual(engine.action(None), 1)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 3}}
  - cond:
      - {if: {do: [{let: {x: 5}}, true]}, then: [1]}
      - {if: {do: [{let: {x: 5}}, false]}, then: [2]}
      - {if: {do: [{let: {x: 5}}, false]}, then: [3]}
    else: [{let: {x: 5}}, 4]
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 3}}
  - cond:
      - {if: {do: [{set: {x: 1}}, true]}, then: [1]}
      - {if: {do: [{set: {x: 2}}, false]}, then: [2]}
      - {if: {do: [{set: {x: 3}}, false]}, then: [3]}
    else: [4]
  - x
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: [string, int, double]
action:
  cond:
    - {if: false, then: [1]}
    - {if: true, then: [[two]]}
    - {if: true, then: [3.0]}
  else: [4]
''')
        self.assertEqual(engine.action(None), "two")

    def testWhileLoops(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - {let: {x: 0}}
  - while: {"!=": [x, 5]}
    do:
      - {log: [x]}
      - {set: {x: {+: [x, 1]}}}
''')
        self.assertEqual(self.collectLogs(engine), [(None, [0]), (None, [1]), (None, [2]), (None, [3]), (None, [4])])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 0}}
  - while: {"!=": [x, 5]}
    do:
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
''')
        self.assertEqual(engine.action(None), 5)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 0}}
  - while: {+: [2, 2]}
    do:
      - {log: [x]}
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 0}}
  - while: {let: {y: 12}}
    do:
      - {log: [x]}
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 0}}
  - while: {do: [{+: [2, 2]}, {"!=": [x, 5]}]}
    do:
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
''')
        self.assertEqual(engine.action(None), 5)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 0}}
  - {let: {y: 0}}
  - while: {do: [{set: {y: 5}}, {"!=": [x, y]}]}
    do:
      - {set: {x: {+: [x, 1]}}}
      - x
  - x
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 0}}
  - while: {"!=": [x, 5]}
    do:
      - {set: {x: {+: [x, 1]}}}
  - x
''')
        self.assertEqual(engine.action(None), 5)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 0}}
  - while: {"!=": [y, 5]}
    do:
      - {let: {y: {+: [x, 1]}}}
      - {set: {x: y}}
  - x
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - {let: {y: [before], x: 0}}
  - while: {"!=": [x, 0]}
    do:
      - {set: {y: [after]}}
  - y
''')
        self.assertEqual(engine.action(None), "before")

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - {let: {x: 0}}
  - do:
      - {log: [x]}
      - {set: {x: {+: [x, 1]}}}
    until: {==: [x, 5]}
''')
        self.assertEqual(self.collectLogs(engine), [(None, [0]), (None, [1]), (None, [2]), (None, [3]), (None, [4])])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - {let: {x: 0}}
  - do:
      - {set: {x: {+: [x, 1]}}}
      - x
    until: {==: [x, 5]}
''')
        self.assertEqual(engine.action(None), None)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 0}}
  - do:
      - {set: {x: {+: [x, 1]}}}
      - x
    until: {==: [x, 5]}
  - x
''')
        self.assertEqual(engine.action(None), 5)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 0}}
  - do:
      - {set: {x: {+: [x, 1]}}}
    until: {==: [x, 5]}
  - x
''')
        self.assertEqual(engine.action(None), 5)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 0}}
  - do:
      - {let: {y: {+: [x, 1]}}}
      - {set: {x: y}}
    until: {==: [y, 5]}
  - x
''')
        self.assertEqual(engine.action(None), 5)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 0}}
  - do:
      - {let: {y: {+: [x, 1]}}}
      - {set: {x: y}}
    until: {==: [y, 5]}
  - y
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - {let: {y: [before], x: 0}}
  - do:
      - {set: {y: [after]}}
    until: {==: [x, 0]}
  - y
''')
        self.assertEqual(engine.action(None), "after")

    def testForLoops(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  for: {x: 0}
  while: {"!=": [x, 5]}
  step: {x: {+: [x, 1]}}
  do:
    - {log: [x]}
''')
        self.assertEqual(self.collectLogs(engine), [(None, [0]), (None, [1]), (None, [2]), (None, [3]), (None, [4])])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - for: {x: 0}
    while: {"!=": [x, 5]}
    step: {x: {+: [x, 1]}}
    do:
      - x
''')
        self.assertEqual(engine.action(None), None)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - for: {x: 0}
    while: {"!=": [x, 5]}
    step: {x: {+: [x, 1]}}
    do:
      - x
  - x
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 0}}
  - for: {dummy: null}
    while: {"!=": [x, 5]}
    step: {x: {+: [x, 1]}}
    do:
      - x
  - x
''')
        self.assertEqual(engine.action(None), 5)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  for: {x: {+: [99, -99]}}
  while: {"!=": [x, {+: [2, 3]}]}
  step: {x: {+: [x, {-: [3, 2]}]}}
  do:
    - x
''')
        self.assertEqual(engine.action(None), None)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  for: {x: {let: {y: 0}}}
  while: {"!=": [x, {+: [2, 3]}]}
  step: {x: {+: [x, {-: [3, 2]}]}}
  do:
    - x
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  for: {x: {do: [{let: {y: 0}}, y]}}
  while: {"!=": [x, {+: [2, 3]}]}
  step: {x: {+: [x, {-: [3, 2]}]}}
  do:
    - x
''')
        self.assertEqual(engine.action(None), None)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  for: {x: 0}
  while: {"!=": [x, 5]}
  step: {x: {+: [x, 1]}}
  do:
    - {let: {y: x}}
    - y
''')
        self.assertEqual(engine.action(None), None)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  for: {x: 0}
  while: {"!=": [x, 5]}
  step: {x: {+: [y, 1]}}
  do:
    - {let: {y: x}}
    - y
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  for: {x: 0}
  while: {"!=": [y, 5]}
  step: {x: {+: [x, 1]}}
  do:
    - {let: {y: x}}
    - y
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  for: {x: 0}
  while: {"!=": [x, 0]}
  step: {x: {+: [x, 1]}}
  do:
    - x
''')
        self.assertEqual(engine.action(None), None)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - {let: {y: [before]}}
  - for: {x: 0}
    while: {"!=": [x, 0]}
    step: {x: {+: [x, 1]}}
    do:
      - {set: {y: [after]}}
  - y
''')
        self.assertEqual(engine.action(None), "before")

    def testForeachLoops(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  foreach: x
  in: {type: {type: array, items: string}, value: [one, two, three]}
  do:
    - {log: [x]}
''')
        self.assertEqual(self.collectLogs(engine), [(None, ["one"]), (None, ["two"]), (None, ["three"])])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  foreach: x
  in: {type: {type: array, items: string}, value: [one, two, three]}
  do:
    - x
''')
        self.assertEqual(engine.action(None), None)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  foreach: x
  in: {type: {type: array, items: string}, value: [one, two, three]}
  do:
    - {let: {y: x}}
    - y
''')
        self.assertEqual(engine.action(None), None)

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {y: [zero]}}
  - foreach: x
    in: {type: {type: array, items: string}, value: [one, two, three]}
    do:
      - {set: {y: x}}
      - y
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - {let: {y: [zero]}}
  - foreach: x
    in: {type: {type: array, items: string}, value: [one, two, three]}
    do:
      - {set: {y: x}}
      - y
    seq: true
''')
        self.assertEqual(engine.action(None), None)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - {let: {y: [zero]}}
  - foreach: x
    in: {type: {type: array, items: string}, value: [one, two, three]}
    do:
      - {set: {y: x}}
    seq: true
  - y
''')
        self.assertEqual(engine.action(None), "three")

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {y: [zero], array: {type: {type: array, items: string}, value: [one, two, three]}}}
  - foreach: x
    in: array
    do:
      - {set: {y: x}}
  - y
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - {let: {y: [zero], array: {type: {type: array, items: string}, value: [one, two, three]}}}
  - foreach: x
    in: array
    do:
      - {set: {y: x}}
    seq: true
  - y
''')
        self.assertEqual(engine.action(None), "three")

        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - {let: {y: [zero], array: {type: {type: array, items: string}, value: [one, two, three]}}}
  - foreach: x
    in: array
    do:
      - {set: {array: {type: {type: array, items: string}, value: [zero]}}}
      - {set: {y: x}}
    seq: true
  - y
''')
        self.assertEqual(engine.action(None), "three")

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  foreach: x
  in: {type: {type: array, items: string}, value: []}
  do:
    - x
''')
        self.assertEqual(engine.action(None), None)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  foreach: x
  in: {type: {type: array, items: double}, value: [1, 2, 3]}
  do:
    - x
''')
        self.assertEqual(engine.action(None), None)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - {let: {y: 0.0}}
  - foreach: x
    in: {type: {type: array, items: double}, value: [1, 2, 3]}
    do:
      - {set: {y: x}}
    seq: true
  - y
''')
        self.assertEqual(engine.action(None), 3.0)

    def testForkeyvalLoops(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  forkey: n
  forval: v
  in: {type: {type: map, values: int}, value: {one: 1, two: 2, three: 3}}
  do:
    - {log: [n, v]}
''')
        self.assertEqual(sorted(self.collectLogs(engine)), sorted([(None, ["one", 1]), (None, ["two", 2]), (None, ["three", 3])]))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: 0}}
  - forkey: n
    forval: v
    in: {type: {type: map, values: int}, value: {one: 1, two: 2, three: 3}}
    do:
      - {set: {x: v}}
  - x
''')
        engine.action(None)

    def testTypeCastBlocks(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{log: [x, y]}]}
      - {as: "double", named: y, do: [{log: [x, y]}]}
      - {as: "string", named: y, do: [{log: [x, y]}]}
''')
        self.assertEqual(self.collectLogs(engine), [(None, [{"double": 2.2}, 2.2])])

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{log: [x, y]}]}
      - {as: "double", named: y, do: [{log: [x, y]}]}
      - {as: "string", named: y, do: [{log: [x, y]}]}
      - {as: "bytes", named: y, do: [{log: [x, y]}]}
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{log: [x, y]}]}
      - {as: "double", named: y, do: [{log: [x, y]}]}
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{log: [x, y]}]}
      - {as: "double", named: y, do: [{log: [x, y]}]}
    partial: true
''')
        self.assertEqual(self.collectLogs(engine), [(None, [{"double": 2.2}, 2.2])])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: [double, string]
action:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - {let: {z: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{set: {z: y}}]}
      - {as: "double", named: y, do: [{set: {z: y}}]}
      - {as: "string", named: y, do: [{set: {z: y}}]}
  - z
''')
        self.assertEqual(engine.action(None), 2.2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: [double, string]
action:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - {let: {z: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: [{set: {z: y}}]}
      - {as: "double", named: y, do: [{set: {z: y}}]}
      - {as: "string", named: y, do: [{set: {z: y}}]}
    partial: true
  - z
''')
        self.assertEqual(engine.action(None), 2.2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: 888.88}
      - {as: "double", named: y, do: [y]}
      - {as: "string", named: y, do: 999.99}
''')
        self.assertEqual(engine.action(None), 2.2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - {let: {x: {type: ["int", "double", "string"], value: {"double": 2.2}}}}
  - cast: x
    cases:
      - {as: "int", named: y, do: 888.88}
      - {as: "double", named: y, do: [y]}
      - {as: "string", named: y, do: 999.99}
    partial: true
''')
        self.assertEqual(engine.action(None), None)

    def testUpcast(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: double
action: {upcast: 3, as: "double"}
''')
        self.assertEqual(engine.action(None), 3.0)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: [double, string]
action: {upcast: 3, as: ["double", "string"]}
''')
        self.assertEqual(engine.action(None), {"double": 3.0})

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: double
action:
  - {let: {fred: {upcast: 3, as: "double"}}}
  - {set: {fred: [hello]}}
  - fred
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: "null"
output: string
action:
  - {let: {fred: {upcast: 3, as: "double"}}}
  - {set: {fred: [hello]}}
  - fred
'''))

        engine, = PFAEngine.fromYaml('''
input: "null"
output: [double, string]
action:
  - {let: {fred: {upcast: 3, as: ["double", "string"]}}}
  - {set: {fred: [hello]}}
  - fred
''')
        self.assertEqual(engine.action(None), "hello")

    def testIfNotNull(self):
        engine, = PFAEngine.fromYaml('''
input: [double, "null"]
output: double
action:
  ifnotnull: {x: input}
  then: x
  else: 12
''')
        self.assertEqual(engine.action(5.0), 5.0)
        self.assertEqual(engine.action(None), 12.0)

        engine, = PFAEngine.fromYaml('''
input: [double, "null"]
output: double
action:
  ifnotnull: {x: input, y: input}
  then: {+: [x, y]}
  else: 12
''')
        self.assertEqual(engine.action(5.0), 10.0)
        self.assertEqual(engine.action(None), 12.0)

        engine, = PFAEngine.fromYaml('''
input: [double, "null"]
output: "null"
action:
  ifnotnull: {x: input, y: input}
  then: {+: [x, y]}
''')
        self.assertEqual(engine.action(5.0), None)
        self.assertEqual(engine.action(None), None)

        engine, = PFAEngine.fromYaml('''
input: [double, "null"]
output: double
action:
  - let: {z: -3.0}
  - ifnotnull: {x: input, y: input}
    then: {set: {z: {+: [x, y]}}}
  - z
''')
        self.assertEqual(engine.action(5.0), 10.0)
        self.assertEqual(engine.action(None), -3.0)

        engine, = PFAEngine.fromYaml('''
input: [double, "null"]
output: double
action:
  - let: {z: -3.0}
  - ifnotnull: {x: input, y: input}
    then: {set: {z: {+: [x, y]}}}
    else: {set: {z: 999.9}}
  - z
''')
        self.assertEqual(engine.action(5.0), 10.0)
        self.assertEqual(engine.action(None), 999.9)

        engine, = PFAEngine.fromYaml('''
input: [double, string, "null"]
output: [double, string]
action:
  - ifnotnull: {x: input}
    then: x
    else: [[whatever]]
''')
        self.assertEqual(engine.action(5.0), {"double": 5.0})
        self.assertEqual(engine.action("hello"), {"string": "hello"})
        self.assertEqual(engine.action(None), "whatever")

    def testPackSignedBigEndian(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: bytes
action:
  pack: [{pad: null}, {boolean: true}, {boolean: false}, {byte: 12}, {short: 12}, {int: 12}, {long: 12}, {float: 12}, {double: 12}]
''')
        self.assertEqual(map(signed, engine.action(None)), [0, 1, 0, 12, 0, 12, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 12, 65, 64, 0, 0, 64, 40, 0, 0, 0, 0, 0, 0])

    def testPackUnsignedBigEndian(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: bytes
action:
  pack: [{"pad": null}, {"boolean": true}, {"boolean": false}, {"unsigned byte": 12}, {"unsigned short": 12}, {"unsigned int": 12}, {"unsigned long": 12}, {"float": 12}, {"double": 12}]
''')
        self.assertEqual(map(signed, engine.action(None)), [0, 1, 0, 12, 0, 12, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 12, 65, 64, 0, 0, 64, 40, 0, 0, 0, 0, 0, 0])

        engine2, = PFAEngine.fromYaml('''
input: "null"
output: bytes
action:
  pack: [{"pad": null}, {"boolean": true}, {"boolean": false}, {"unsigned byte": 255}, {"unsigned short": 65535}, {"unsigned int": 4294967295}, {"unsigned long": {double: 1000000000000000000}}, {"float": 12}, {"double": 12}]
''')
        self.assertEqual(map(signed, engine2.action(None)), [0, 1, 0, -1, -1, -1, -1, -1, -1, -1, 13, -32, -74, -77, -89, 100, 0, 0, 65, 64, 0, 0, 64, 40, 0, 0, 0, 0, 0, 0])


    def testPackSignedLittleEndian(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: bytes
action:
  pack: [{pad: null}, {boolean: true}, {boolean: false}, {byte: 12}, {"little short": 12}, {"little int": 12}, {"little long": 12}, {"little float": 12}, {"little double": 12}]
''')
        self.assertEqual(map(signed, engine.action(None)), [0, 1, 0, 12, 12, 0, 12, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 65, 0, 0, 0, 0, 0, 0, 40, 64])

    def testPackUnsignedLittleEndian(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: bytes
action:
  pack: [{"pad": null}, {"boolean": true}, {"boolean": false}, {"unsigned byte": 12}, {"little unsigned short": 12}, {"little unsigned int": 12}, {"little unsigned long": 12}, {"little float": 12}, {"little double": 12}]
''')
        self.assertEqual(map(signed, engine.action(None)), [0, 1, 0, 12, 12, 0, 12, 0, 0, 0, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 64, 65, 0, 0, 0, 0, 0, 0, 40, 64])

        engine2, = PFAEngine.fromYaml('''
input: "null"
output: bytes
action:
  pack: [{"pad": null}, {"boolean": true}, {"boolean": false}, {"unsigned byte": 255}, {"little unsigned short": 65535}, {"little unsigned int": 4294967295}, {"little unsigned long": {double: 1000000000000000000}}, {"little float": 12}, {"little double": 12}]
''')
        self.assertEqual(map(signed, engine2.action(None)), [0, 1, 0, -1, -1, -1, -1, -1, -1, -1, 0, 0, 100, -89, -77, -74, -32, 13, 0, 0, 64, 65, 0, 0, 0, 0, 0, 0, 40, 64])

    def testPackByteArraysAsRaw(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: bytes
method: fold
zero: ""
action:
  pack: [{raw: tally}, {raw: input}]
merge:
  pack: [{raw: tallyOne}, {raw: tallyTwo}]
''')
        self.assertEqual(map(signed, engine.action("".join(map(chr, [1, 2, 3])))), [1, 2, 3])
        self.assertEqual(map(signed, engine.action("".join(map(chr, [4, 5, 6])))), [1, 2, 3, 4, 5, 6])
        self.assertEqual(map(signed, engine.action("".join(map(chr, [7, 8, 9])))), [1, 2, 3, 4, 5, 6, 7, 8, 9])

    def testPackByteArraysAsRaw3(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: bytes
method: fold
zero: ""
action:
  pack: [{raw: tally}, {raw3: input}]
merge:
  pack: [{raw: tallyOne}, {raw: tallyTwo}]
''')
        self.assertEqual(map(signed, engine.action("".join(map(chr, [1, 2, 3])))), [1, 2, 3])
        self.assertEqual(map(signed, engine.action("".join(map(chr, [4, 5, 6])))), [1, 2, 3, 4, 5, 6])
        self.assertEqual(map(signed, engine.action("".join(map(chr, [7, 8, 9])))), [1, 2, 3, 4, 5, 6, 7, 8, 9])
        self.assertRaises(PFARuntimeException, lambda: engine.action("".join(map(chr, [0, 0]))))

    def testPackByteArraysAsNullTerminated(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: bytes
method: fold
zero: ""
action:
  pack: [{"null terminated": tally}, {"null terminated": input}]
merge:
  pack: [{"null terminated": tallyOne}, {"null terminated": tallyTwo}]
''')
        self.assertEqual(map(signed, engine.action("".join(map(chr, [1, 2, 3])))), [0, 1, 2, 3, 0])
        self.assertEqual(map(signed, engine.action("".join(map(chr, [4, 5, 6])))), [0, 1, 2, 3, 0, 0, 4, 5, 6, 0])
        self.assertEqual(map(signed, engine.action("".join(map(chr, [7, 8, 9])))), [0, 1, 2, 3, 0, 0, 4, 5, 6, 0, 0, 7, 8, 9, 0])

    def testPackByteArraysAsLengthPrefixed(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: bytes
method: fold
zero: ""
action:
  pack: [{"length prefixed": tally}, {"length prefixed": input}]
merge:
  pack: [{"length prefixed": tallyOne}, {"length prefixed": tallyTwo}]
''')
        self.assertEqual(map(signed, engine.action("".join(map(chr, [1, 2, 3])))), [0, 3, 1, 2, 3])
        self.assertEqual(map(signed, engine.action("".join(map(chr, [4, 5, 6])))), [5, 0, 3, 1, 2, 3, 3, 4, 5, 6])
        self.assertEqual(map(signed, engine.action("".join(map(chr, [7, 8, 9])))), [10, 5, 0, 3, 1, 2, 3, 3, 4, 5, 6, 3, 7, 8, 9])

    def testUnpackNull(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: "null"
action:
  unpack: input
  format: [{x: pad}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engine.action("".join(map(unsigned, [10]))), None)
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, []))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2]))))

    def testUnpackBoolean(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: boolean
action:
  unpack: input
  format: [{x: boolean}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engine.action("".join(map(unsigned, [0]))), False)
        self.assertEqual(engine.action("".join(map(unsigned, [1]))), True)
        self.assertEqual(engine.action("".join(map(unsigned, [8]))), True)
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, []))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2]))))

    def testUnpackByte(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: int
action:
  unpack: input
  format: [{x: byte}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engine.action("".join(map(unsigned, [10]))), 10)
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, []))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2]))))

        engineUnsigned, = PFAEngine.fromYaml('''
input: bytes
output: int
action:
  unpack: input
  format: [{x: "unsigned byte"}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engineUnsigned.action("".join(map(unsigned, [127]))), 127)
        self.assertEqual(engineUnsigned.action("".join(map(unsigned, [-128]))), 128)
        self.assertEqual(engineUnsigned.action("".join(map(unsigned, [-1]))), 255)

    def testUnpackShort(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: int
action:
  unpack: input
  format: [{x: short}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engine.action("".join(map(unsigned, [0, 4]))), 4)
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2, 3]))))

        engineLittleEndian, = PFAEngine.fromYaml('''
input: bytes
output: int
action:
  unpack: input
  format: [{x: "little short"}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engineLittleEndian.action("".join(map(unsigned, [4, 0]))), 4)

        engineUnsigned, = PFAEngine.fromYaml('''
input: bytes
output: int
action:
  unpack: input
  format: [{x: "unsigned short"}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engineUnsigned.action("".join(map(unsigned, [127, -1]))), 32767)
        self.assertEqual(engineUnsigned.action("".join(map(unsigned, [-128, 0]))), 32768)
        self.assertEqual(engineUnsigned.action("".join(map(unsigned, [-1, -1]))), 65535)

        engineUnsignedLittleEndian, = PFAEngine.fromYaml('''
input: bytes
output: int
action:
  unpack: input
  format: [{x: "little unsigned short"}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engineUnsignedLittleEndian.action("".join(map(unsigned, [-1, 127]))), 32767)
        self.assertEqual(engineUnsignedLittleEndian.action("".join(map(unsigned, [0, -128]))), 32768)
        self.assertEqual(engineUnsignedLittleEndian.action("".join(map(unsigned, [-1, -1]))), 65535)

    def testUnpackInt(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: int
action:
  unpack: input
  format: [{x: int}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engine.action("".join(map(unsigned, [0, 0, 0, 4]))), 4)
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2, 3]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2, 3, 4, 5]))))

        engineLittleEndian, = PFAEngine.fromYaml('''
input: bytes
output: int
action:
  unpack: input
  format: [{x: "little int"}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engineLittleEndian.action("".join(map(unsigned, [4, 0, 0, 0]))), 4)

        engineUnsigned, = PFAEngine.fromYaml('''
input: bytes
output: long
action:
  unpack: input
  format: [{x: "unsigned int"}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engineUnsigned.action("".join(map(unsigned, [127, -1, -1, -1]))), 2147483647)
        self.assertEqual(engineUnsigned.action("".join(map(unsigned, [-128, 0, 0, 0]))), 2147483648)
        self.assertEqual(engineUnsigned.action("".join(map(unsigned, [-1, -1, -1, -1]))), 4294967295)

        engineUnsignedLittleEndian, = PFAEngine.fromYaml('''
input: bytes
output: long
action:
  unpack: input
  format: [{x: "little unsigned int"}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engineUnsignedLittleEndian.action("".join(map(unsigned, [-1, -1, -1, 127]))), 2147483647)
        self.assertEqual(engineUnsignedLittleEndian.action("".join(map(unsigned, [0, 0, 0, -128]))), 2147483648)
        self.assertEqual(engineUnsignedLittleEndian.action("".join(map(unsigned, [-1, -1, -1, -1]))), 4294967295)

    def testUnpackLong(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: long
action:
  unpack: input
  format: [{x: long}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engine.action("".join(map(unsigned, [0, 0, 0, 0, 0, 0, 0, 4]))), 4)
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2, 3, 4, 5, 6, 7]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2, 3, 4, 5, 6, 7, 8, 9]))))

        engineLittleEndian, = PFAEngine.fromYaml('''
input: bytes
output: long
action:
  unpack: input
  format: [{x: "little long"}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertEqual(engineLittleEndian.action("".join(map(unsigned, [4, 0, 0, 0, 0, 0, 0, 0]))), 4)

        engineUnsigned, = PFAEngine.fromYaml('''
input: bytes
output: double
action:
  unpack: input
  format: [{x: "unsigned long"}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertAlmostEqual(engineUnsigned.action("".join(map(unsigned, [0, 0, 0, 0, 0, 0, 0, 4]))), 4, places=4)

        engineUnsignedLittleEndian, = PFAEngine.fromYaml('''
input: bytes
output: double
action:
  unpack: input
  format: [{x: "little unsigned long"}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertAlmostEqual(engineUnsignedLittleEndian.action("".join(map(unsigned, [4, 0, 0, 0, 0, 0, 0, 0]))), 4, places=4)

    def testUnpackFloat(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: float
action:
  unpack: input
  format: [{x: float}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertAlmostEqual(engine.action("".join(map(unsigned, [64, 72, -11, -61]))), 3.14, places=6)
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2, 3]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2, 3, 4, 5]))))

        engineLittleEndian, = PFAEngine.fromYaml('''
input: bytes
output: float
action:
  unpack: input
  format: [{x: "little float"}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertAlmostEqual(engineLittleEndian.action("".join(map(unsigned, [-61, -11, 72, 64]))), 3.14, places=6)

    def testUnpackDouble(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: double
action:
  unpack: input
  format: [{x: double}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertAlmostEqual(engine.action("".join(map(unsigned, [64, 9, 30, -72, 81, -21, -123, 31]))), 3.14, places=6)
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2, 3, 4, 5, 6, 7]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2, 3, 4, 5, 6, 7, 8, 9]))))

        engineLittleEndian, = PFAEngine.fromYaml('''
input: bytes
output: double
action:
  unpack: input
  format: [{x: "little double"}]
  then: x
  else: {error: "Ack!"}
''')
        self.assertAlmostEqual(engineLittleEndian.action("".join(map(unsigned, [31, -123, -21, 81, -72, 30, 9, 64]))), 3.14, places=6)

    def testUnpackRaw(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  unpack: input
  format: [{x: raw5}]
  then: {bytes.decodeAscii: x}
  else: {error: "Ack!"}
''')
        self.assertEqual(engine.action("".join(map(unsigned, [104, 101, 108, 108, 111]))), "hello")
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2, 3, 4]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [1, 2, 3, 4, 5, 6]))))

    def testUnpackToNull(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  unpack: input
  format: [{x: nullterminated}]
  then: {bytes.decodeAscii: x}
  else: {error: "Ack!"}
''')
        self.assertEqual(engine.action("".join(map(unsigned, [104, 101, 108, 108, 111, 0]))), "hello")
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [104, 101, 108, 108, 111]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, []))))

    def testUnpackLengthPrefixed(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  unpack: input
  format: [{x: lengthprefixed}]
  then: {bytes.decodeAscii: x}
  else: {error: "Ack!"}
''')
        self.assertEqual(engine.action("".join(map(unsigned, [5, 104, 101, 108, 108, 111]))), "hello")
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [5, 104, 101, 108, 108, 111, 99]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [5, 104, 101, 108, 108]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, []))))

    def testUnpackMultipleWithRaw(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  unpack: input
  format: [{x: pad}, {y: raw5}, {z: int}]
  then: {bytes.decodeAscii: y}
  else: {error: "Ack!"}
''')
        self.assertEqual(engine.action("".join(map(unsigned, [99, 104, 101, 108, 108, 111, 0, 0, 0, 4]))), "hello")
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [99, 104, 101, 108, 108, 111, 0, 0, 0, 4, 1]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [99, 104, 101, 108, 108, 111, 0, 0, 0]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, []))))

        engineLittleEndian, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  unpack: input
  format: [{x: "little int"}, {y: raw5}, {z: int}]
  then: {bytes.decodeAscii: y}
  else: {error: "Ack!"}
''')
        self.assertEqual(engineLittleEndian.action("".join(map(unsigned, [0, 0, 0, 4, 104, 101, 108, 108, 111, 0, 0, 0, 4]))), "hello")
        self.assertRaises(PFAUserException, lambda: engineLittleEndian.action("".join(map(unsigned, [0, 0, 0, 4, 104, 101, 108, 108, 111, 0, 0, 0, 4, 1]))))
        self.assertRaises(PFAUserException, lambda: engineLittleEndian.action("".join(map(unsigned, [0, 0, 0, 4, 104, 101, 108, 108, 111, 0, 0, 0]))))
        self.assertRaises(PFAUserException, lambda: engineLittleEndian.action("".join(map(unsigned, []))))

    def testUnpackMultipleWithToNull(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  unpack: input
  format: [{x: pad}, {y: nullterminated}, {z: int}]
  then: {bytes.decodeAscii: y}
  else: {error: "Ack!"}
''')
        self.assertEqual(engine.action("".join(map(unsigned, [99, 104, 101, 108, 108, 111, 0, 0, 0, 0, 4]))), "hello")
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [99, 104, 101, 108, 108, 111, 0, 0, 0, 0, 4, 1]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [99, 104, 101, 108, 108, 111, 0, 0, 0, 0]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, []))))

        engineLittleEndian, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  unpack: input
  format: [{x: "little int"}, {y: nullterminated}, {z: int}]
  then: {bytes.decodeAscii: y}
  else: {error: "Ack!"}
''')
        self.assertEqual(engineLittleEndian.action("".join(map(unsigned, [0, 0, 0, 4, 104, 101, 108, 108, 111, 0, 0, 0, 0, 4]))), "hello")
        self.assertRaises(PFAUserException, lambda: engineLittleEndian.action("".join(map(unsigned, [0, 0, 0, 4, 104, 101, 108, 108, 111, 0, 0, 0, 0, 4, 1]))))
        self.assertRaises(PFAUserException, lambda: engineLittleEndian.action("".join(map(unsigned, [0, 0, 0, 4, 104, 101, 108, 108, 111, 0, 0, 0, 0]))))
        self.assertRaises(PFAUserException, lambda: engineLittleEndian.action("".join(map(unsigned, []))))

    def testUnpackMultipleWithLengthPrefixed(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  unpack: input
  format: [{x: pad}, {y: prefixed}, {z: int}]
  then: {bytes.decodeAscii: y}
  else: {error: "Ack!"}
''')
        self.assertEqual(engine.action("".join(map(unsigned, [99, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4]))), "hello")
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [99, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4, 1]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, [99, 5, 104, 101, 108, 108, 111, 0, 0, 0]))))
        self.assertRaises(PFAUserException, lambda: engine.action("".join(map(unsigned, []))))

        engineLittleEndian, = PFAEngine.fromYaml('''
input: bytes
output: string
action:
  unpack: input
  format: [{x: "little int"}, {y: prefixed}, {z: int}]
  then: {bytes.decodeAscii: y}
  else: {error: "Ack!"}
''')
        self.assertEqual(engineLittleEndian.action("".join(map(unsigned, [0, 0, 0, 4, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4]))), "hello")
        self.assertRaises(PFAUserException, lambda: engineLittleEndian.action("".join(map(unsigned, [0, 0, 0, 4, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4, 1]))))
        self.assertRaises(PFAUserException, lambda: engineLittleEndian.action("".join(map(unsigned, [0, 0, 0, 4, 5, 104, 101, 108, 108, 111, 0, 0, 0]))))
        self.assertRaises(PFAUserException, lambda: engineLittleEndian.action("".join(map(unsigned, []))))

    def testUnpackNotBreakOtherVariables(self):
        engine, = PFAEngine.fromYaml('''
input: bytes
output: boolean
action:
  - let: {successful: false}
  - unpack: input
    format: [{x: pad}, {y: prefixed}, {z: int}]
    then: {bytes.decodeAscii: y}
    else: {error: "Ack!"}
  - successful
''')
        self.assertEqual(engine.action("".join(map(unsigned, [99, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4]))), False)

        engine2, = PFAEngine.fromYaml('''
input: bytes
output: int
action:
  - let: {tmpInteger: 12}
  - unpack: input
    format: [{x: pad}, {y: prefixed}, {z: int}]
    then: {bytes.decodeAscii: y}
    else: {error: "Ack!"}
  - tmpInteger
''')
        self.assertEqual(engine2.action("".join(map(unsigned, [99, 5, 104, 101, 108, 108, 111, 0, 0, 0, 4]))), 12)

    def testDoc(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: [int, "null"]
action:
  if: true
  then:
    - {doc: "This is very nice"}
  else:
    - {+: [5, 5]}
''')
        self.assertEqual(engine.action(None), None)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: [int, "null"]
action:
  if: true
  then:
    - {doc: "This is very nice"}
    - if: true
      then:
        - {+: [5, 5]}
''')
        self.assertEqual(engine.action(None), None)

    def testError(self):
        def callme():
            engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action: {do: {error: "This is bad"}}
''')
            engine.action(None)
        self.assertRaises(PFAUserException, callme)

        try:
            engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action: {do: {error: "This is bad", code: -12}}
''')
            engine.action(None)
            raise Exception
        except PFAUserException as err:
            self.assertEqual(err.code, -12)

        def callme2():
            engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  if: true
  then:
    - {error: "This is bad"}
''')
            engine.action(None)
        self.assertRaises(PFAUserException, callme2)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  if: false
  then:
    - {error: "This is bad"}
''')
        self.assertEqual(engine.action(None), None)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action:
  if: false
  then:
    - {error: "This is bad"}
    - [hello]
  else:
    - [there]
''')
        self.assertEqual(engine.action(None), "there")

    def testProperlyDealWithTheExceptionType(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action:
  if: true
  then: {string: "hello"}
  else: {error: "This is bad"}
''')
        self.assertEqual(engine.action(None), "hello")

        engine, = PFAEngine.fromYaml('''
input: "null"
output: ["null", string]
action:
  if: true
  then: {string: "hello"}
  else: {do: {error: "This is bad"}}
''')
        self.assertEqual(engine.action(None), "hello")

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  if: true
  then: {error: "err 1"}
  else: {error: "err 2"}
''')[0].action(None)
        self.assertRaises(PFAUserException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: string
action:
  if: false
  then: {string: "hello"}
  else: {error: "This is bad"}
''')[0].action(None)
        self.assertRaises(PFAUserException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  if: true
  then: {error: "This is bad"}
''')[0].action(None)
        self.assertRaises(PFAUserException, bad)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: string
action:
  cond:
    - if: false
      then: {error: "err 1"}
    - if: false
      then: {error: "err 2"}
    - if: true
      then: {string: hey}
    - if: false
      then: {error: "err 3"}
  else:
    {error: "err 4"}
''')
        self.assertEqual(engine.action(None), "hey")

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  cond:
    - if: false
      then: {error: "err 1"}
    - if: false
      then: {error: "err 2"}
    - if: false
      then: {error: "err 3"}
  else:
    {error: "err 4"}
''')[0].action(None)
        self.assertRaises(PFAUserException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - let:
      x: {error: "should not be able to assign the bottom type"}
  - 123
''')[0].action(None)
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - let:
      x: null
  - set:
      x: {error: "even if it\'s a null object"}
  - 123
''')[0].action(None)
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: int
cells:
  someobj:
    type:
      type: record
      name: Whatever
      fields:
        - {name: x, type: "null"}
    init:
      {x: null}
action:
  - attr: {cell: someobj}
    path: [{string: x}]
    to: {error: "even if it\'s a null object"}
  - 123
''')[0].action(None)
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: int
cells:
  someobj:
    type:
      type: record
      name: Whatever
      fields:
        - {name: x, type: "null"}
    init:
      {x: null}
action:
  - cell: someobj
    path: [{string: x}]
    to: {error: "even if it\'s a null object"}
  - 123
''')[0].action(None)
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: int
pools:
  someobj:
    type:
      type: record
      name: Whatever
      fields:
        - {name: x, type: "null"}
    init:
      whatev:
        {x: null}
action:
  - pool: someobj
    path: [{string: whatev}, {string: x}]
    to: {error: "even if it\'s a null object"}
    init:
      type: Whatever
      value: {x: null}
  - 123
''')[0].action(None)
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: "null"
action: {u.somefcn: []}
fcns:
  somefcn:
    params: []
    ret: "null"
    do: {error: "but this is okay"}
''')[0].action(None)
        self.assertRaises(PFAUserException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: int
action: {u.somefcn: []}
fcns:
  somefcn:
    params: []
    ret: int
    do: {error: "this, too"}
''')[0].action(None)
        self.assertRaises(PFAUserException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: int
action:
  u.callme: {error: "not in argument lists, no!"}
fcns:
  callme:
    params: [{x: "null"}]
    ret: int
    do: 12
''')[0].action(None)
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: int
action:
  u.callme: {do: {error: "but do sanitizes it"}}
fcns:
  callme:
    params: [{x: "null"}]
    ret: int
    do: 12
''')[0].action(None)
        self.assertRaises(PFAUserException, bad)

        engine, = PFAEngine.fromYaml('''
input: "null"
cells:
  someobj:
    type: [int, string]
    init: {int: 12}
output: int
action:
  cast: {cell: someobj}
  cases:
    - as: int
      named: x
      do: x
    - as: string
      named: x
      do: {error: "really oughta be an int"}
''')
        self.assertEqual(engine.action(None), 12)

        engine, = PFAEngine.fromYaml('''
input: "null"
cells:
  someobj:
    type: [int, string]
    init: {int: 12}
output: "null"
action:
  cast: {cell: someobj}
  cases:
    - as: int
      named: x
      do: x
    - as: string
      named: x
      do: {error: "really oughta be an int"}
  partial: true
''')
        self.assertEqual(engine.action(None), None)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
cells:
  someobj:
    type: [int, string]
    init: {string: woohoo}
output: int
action:
  cast: {cell: someobj}
  cases:
    - as: int
      named: x
      do: x
    - as: string
      named: x
      do: {error: "really oughta be an int"}
''')[0].action(None)
        self.assertRaises(PFAUserException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
cells:
  someobj:
    type: [int, string]
    init: {string: woohoo}
output: "null"
action:
  cast: {cell: someobj}
  cases:
    - as: int
      named: x
      do: x
    - as: string
      named: x
      do: {error: "really oughta be an int"}
  partial: true
''')[0].action(None)
        self.assertRaises(PFAUserException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  upcast: {error: "wtf?"}
  as: "null"
''')[0].action(None)
        self.assertRaises(PFASemanticException, bad)

        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
cells:
  someobj:
    type: [int, "null"]
    init: {int: 12}
action:
  ifnotnull: {x: {cell: someobj}}
  then: x
  else: {error: "I really wanted an int"}
''')
        self.assertEqual(engine.action(None), 12)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: int
cells:
  someobj:
    type: [int, "null"]
    init: null
action:
  ifnotnull: {x: {cell: someobj}}
  then: x
  else: {error: "I really wanted an int"}
''')[0].action(None)
        self.assertRaises(PFAUserException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: int
action:
  ifnotnull: {x: {error: "wtf?"}}
  then: x
''')[0].action(None)
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - log: {error: "wtf?"}
  - null
''')[0].action(None)
        self.assertRaises(PFAUserException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - log: [12, {error: "wtf?"}]
  - null
''')[0].action(None)
        self.assertRaises(PFAUserException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: "null"
action:
  - log: [{error: "wtf?"}, 12]
  - null
''')[0].action(None)
        self.assertRaises(PFAUserException, bad)

    def testMinimallyWork(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - input
''')
        self.assertEqual(engine.action("hello"), "hello")

    def testHandleNestedScopes(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - do:
    - input
''')
        self.assertEqual(engine.action("hello"), "hello")

    def testCallFunctions2(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {+: [input, input]}
''')
        self.assertEqual(engine.action(2), 4)

    def testIdentifyTypeErrors1(self):
        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: double
output: string
action:
  - {+: [input, input]}
'''))

    def testIdentifyTypeErrors2(self):
        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: string
output: string
action:
  - {+: [input, input]}
'''))

    def testDefineFunctions(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
action:
  - {u.plus: [input, input]}
fcns:
  plus:
    params: [{x: double}, {y: double}]
    ret: double
    do:
      - {+: [x, y]}
  minus:
    params: [{x: double}, {y: double}]
    ret: double
    do:
      - {-: [x, y]}
''')
        self.assertEqual(engine.action(2), 4)

    def testMinimallyWorkForEmitTypeEngines(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
method: emit
action:
  - {emit: [input]}
  - input
''')

        out = []
        engine.emit = lambda x: out.append(x)
        engine.action("hello")
        self.assertEqual(out, ["hello"])

    def testEmitInUserFunctions(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
method: emit
action:
  - emit: [input]
  - u.callme: [input]
fcns:
  callme:
    params:
      - x: string
    ret: "null"
    do:
      - emit: [x]
''')

        out = []
        engine.emit = lambda x: out.append(x)
        engine.action("hello")
        self.assertEqual(out, ["hello", "hello"])

    def testMinimallyWorkForFoldTypeEngines(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
method: fold
zero: 0
action: {+: [input, tally]}
merge: {+: [tallyOne, tallyTwo]}
''')
        
        self.assertEqual(engine.action(5), 5.0)
        self.assertEqual(engine.action(3), 8.0)
        self.assertEqual(engine.action(2), 10.0)
        self.assertEqual(engine.action(20), 30.0)

        engine.tally = 1.0
        self.assertEqual(engine.action(5), 6.0)

    def testRefuseTallyInUserFunctions(self):
        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: double
output: double
method: fold
zero: 0
action:
  - u.callme: [input]
fcns:
  callme:
    params:
      - x: double
    ret: double
    do:
      - {+: [x, tally]}
merge: {+: [tallyOne, tallyTwo]}
'''))

    def testButPassingTallyInIsFine(self):
        engine, = PFAEngine.fromYaml('''
input: double
output: double
method: fold
zero: 0
action:
  - u.callme: [input, tally]
fcns:
  callme:
    params:
      - x: double
      - t: double
    ret: double
    do:
      - {+: [x, t]}
merge: {+: [tallyOne, tallyTwo]}
''')
        
        self.assertEqual(engine.action(5), 5.0)
        self.assertEqual(engine.action(3), 8.0)
        self.assertEqual(engine.action(2), 10.0)
        self.assertEqual(engine.action(20), 30.0)

        engine.tally = 1.0
        self.assertEqual(engine.action(5), 6.0)

    def testExtractDeepWithinAnObject(self):
        engine, = PFAEngine.fromYaml('''
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {attr: input, path: [[one], 2, x]}
''')
        self.assertEqual(engine.action({"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}), 2)

    def testExtractFromAnOnTheFlyGeneratedObject(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: [two]}}
  - attr:
      value: {"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}
      type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
    path: [[one], 2, x]
''')
        self.assertEqual(engine.action(None), 2)
        
    def testNotAcceptBadIndexes(self):
        engine, = PFAEngine.fromYaml('''
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {attr: input, path: [[one], 3, x]}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action({"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}))

        engine, = PFAEngine.fromYaml('''
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [TWO]}}
  - {attr: input, path: [[one], 2, x]}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action({"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}))

    def testNotAcceptBadIndexTypes(self):
        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {attr: input, path: [[ONE], 2, x]}
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {attr: input, path: [[one], x, x]}
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {attr: input, path: [[one], 2, 2]}
'''))

    def testChangeADeepObject(self):
        engine, = PFAEngine.fromYaml('''
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {let: {something: {attr: input, path: [[one], 2, x], to: 999}}}
  - {attr: something, path: [[one], 2, x]}
''')
        self.assertEqual(engine.action({"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}), 999)

    def testChangeAnOnTheFlyGeneratedObject(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: int
action:
  - {let: {x: [two]}}
  - let:
      something:
        attr:
          value: {"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}
          type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
        path: [[one], 2, x]
        to: 999
  - {attr: something, path: [[one], 2, x]}
''')
        self.assertEqual(engine.action(None), 999)

    def testChangeADeepObjectWithFcnDef(self):
        engine, = PFAEngine.fromYaml('''
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {let: {something: {attr: input, path: [[one], 2, x], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}}}
  - {attr: something, path: [[one], 2, x]}
''')
        self.assertEqual(engine.action({"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}), 3)

    def testChangeADeepObjectWithFcnRef(self):
        engine, = PFAEngine.fromYaml('''
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {let: {something: {attr: input, path: [[one], 2, x], to: {fcn: u.inc}}}}
  - {attr: something, path: [[one], 2, x]}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action({"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}), 3)

        engine, = PFAEngine.fromYaml('''
input: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}
output: int
action:
  - {let: {x: [two]}}
  - {let: {something: {attr: input, path: [[one], 2, x], to: {fcn: u.inc, fill: {uno: 1}}}}}
  - {attr: something, path: [[one], 2, x]}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
''')
        self.assertEqual(engine.action({"one": [{"zero": 0}, {"one": 1}, {"two": 2}]}), 3)

    def testExtractPrivateCells(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - cell: x
cells:
  x: {type: int, init: 12}
''')
        self.assertEqual(engine.action("whatever"), 12)

    def testExtractPublicCells(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - cell: x
cells:
  x: {type: int, init: 12, shared: true}
''')
        self.assertEqual(engine.action("whatever"), 12)

    def testExtractDeepPrivateCells(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
''')
        self.assertEqual(engine.action("two"), 2)

    def testExtractDeepPublicCells(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
''')
        self.assertEqual(engine.action("two"), 2)

    def testNotFindNonExistentCells(self):
        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: string
output: int
action:
  - cell: y
cells:
  x: {type: int, init: 12}
'''))

    def testNotAcceptBadIndexes2(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], 3, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("two"))

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("TWO"))

    def testNotAcceptBadIndexTypes2(self):
        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[ONE], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], input, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], 2, 2]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
'''))
        
    def testChangePrivateCells(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: 999}
  - {cell: x}
cells:
  x: {type: int, init: 12}
''')
        self.assertEqual(engine.action("whatever"), 999)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: 999}
cells:
  x: {type: int, init: 12}
''')
        self.assertEqual(engine.action("whatever"), 999)

    def testChangePrivateCellsWithFcnDef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
  - {cell: x}
cells:
  x: {type: int, init: 12}
''')
        self.assertEqual(engine.action("whatever"), 13)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
cells:
  x: {type: int, init: 12}
''')
        self.assertEqual(engine.action("whatever"), 13)

    def testChangePrivateCellsWithFcnRef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: {fcn: u.inc}}
  - {cell: x}
cells:
  x: {type: int, init: 12}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("whatever"), 13)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: {fcn: u.inc, fill: {uno: 1}}}
  - {cell: x}
cells:
  x: {type: int, init: 12}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
''')
        self.assertEqual(engine.action("whatever"), 13)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: {fcn: u.inc}}
cells:
  x: {type: int, init: 12}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("whatever"), 13)

    def testChangeDeepPrivateCells(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: 999}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
''')
        self.assertEqual(engine.action("two"), 999)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: 999}
    path: [[one], 2, input]
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
''')
        self.assertEqual(engine.action("two"), 999)

    def testChangeDeepPrivateCellsWithFcnDef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
    path: [[one], 2, input]
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
''')
        self.assertEqual(engine.action("two"), 3)

    def testChangeDeepPrivateCellsWithFcnRef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: {fcn: u.inc}}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: {fcn: u.inc, fill: {uno: 1}}}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: {fcn: u.inc}}
    path: [[one], 2, input]
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("two"), 3)

    def testChangePublicCells(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: 999}
  - {cell: x}
cells:
  x: {type: int, init: 12, shared: true}
''')
        self.assertEqual(engine.action("whatever"), 999)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: 999}
cells:
  x: {type: int, init: 12, shared: true}
''')
        self.assertEqual(engine.action("whatever"), 999)

    def testChangePublicCellsWithFcnDef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
  - {cell: x}
cells:
  x: {type: int, init: 12, shared: true}
''')
        self.assertEqual(engine.action("whatever"), 13)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
cells:
  x: {type: int, init: 12, shared: true}
''')
        self.assertEqual(engine.action("whatever"), 13)

    def testChangePublicCellsWithFcnRef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: {fcn: u.inc}}
  - {cell: x}
cells:
  x: {type: int, init: 12, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("whatever"), 13)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: {fcn: u.inc, fill: {uno: 1}}}
  - {cell: x}
cells:
  x: {type: int, init: 12, shared: true}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
''')
        self.assertEqual(engine.action("whatever"), 13)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, to: {fcn: u.inc}}
cells:
  x: {type: int, init: 12, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("whatever"), 13)

    def testChangeDeepPublicCells(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: 999}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
''')
        self.assertEqual(engine.action("two"), 999)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: 999}
    path: [[one], 2, input]
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
''')
        self.assertEqual(engine.action("two"), 999)

    def testChangeDeepPublicCellsWithFcnDef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}}
    path: [[one], 2, input]
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
''')
        self.assertEqual(engine.action("two"), 3)

    def testChangeDeepPublicCellsWithFcnRef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: {fcn: u.inc}}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {cell: x, path: [[one], 2, input], to: {fcn: u.inc, fill: {uno: 1}}}
  - {cell: x, path: [[one], 2, input]}
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - attr: {cell: x, path: [[one], 2, input], to: {fcn: u.inc}}
    path: [[one], 2, input]
cells:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {one: [{zero: 0}, {one: 1}, {two: 2}]}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("two"), 3)

    def testExtractPrivatePools(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
''')
        self.assertEqual(engine.action("whatever"), 12)

    def testExtractPublicPools(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
''')
        self.assertEqual(engine.action("whatever"), 12)

    def testExtractDeepPublicPools(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
''')
        self.assertEqual(engine.action("two"), 2)

    def testExtractDeepPublicPools2(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
''')
        self.assertEqual(engine.action("two"), 2)

    def testNotFindNonExistentPools(self):
        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: y, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
'''))

    def testNotAcceptBadIndexes3(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 3, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("two"))

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("TWO"))

    def testNotAcceptBadIndexTypes3(self):
        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [ONE], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], input, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
'''))

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, 2]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
'''))

    def testChangePrivatePools(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: 999, init: 123}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
''')
        self.assertEqual(engine.action("whatever"), 999)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: 999, init: 123}
pools:
  x: {type: int, init: {whatever: 12}}
''')
        self.assertEqual(engine.action("whatever"), 999)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: 999, init: 123}
pools:
  x: {type: int, init: {whatever: 12}}
''')
        self.assertEqual(engine.action("somethingelse"), 999)

    def testChangePrivatePoolsWithFcnDef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: 0}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
''')
        self.assertEqual(engine.action("whatever"), 13)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: 0}
pools:
  x: {type: int, init: {whatever: 12}}
''')
        self.assertEqual(engine.action("whatever"), 13)

    def testChangePrivatePoolsWithFcnRef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: {fcn: u.inc}, init: 0}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("whatever"), 13)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: {fcn: u.inc, fill: {uno: 1}}, init: 0}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
''')
        self.assertEqual(engine.action("whatever"), 13)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: {fcn: u.inc}, init: 0}
pools:
  x: {type: int, init: {whatever: 12}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("whatever"), 13)

    def testChangeDeepPrivatePools(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: 999, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
''')
        self.assertEqual(engine.action("two"), 999)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: 999, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
''')
        self.assertEqual(engine.action("two"), 999)

        engine, = PFAEngine.fromYaml('''
input: string
output: {type: map, values: int}
action:
  - attr: {pool: x, path: [[somethingelse], [one], 2, input], to: 999, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102, glue: 103}]}, type: SimpleRecord}}
    path: [[one], 2]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
''')
        self.assertEqual(engine.action("two"), {"two": 999, "glue": 103})

    def testChangeDeepPrivatePoolsWithFcnDef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  attr:
    pool: x
    path: [[somethingelse], [one], 2, input]
    to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
    init:
      value: {one: [{zero: 100}, {one: 101}, {two: 102}]}
      type: SimpleRecord
  path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
''')
        self.assertEqual(engine.action("two"), 103)

    def testChangeDeepPrivatePoolsWithFcnRef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {fcn: u.inc}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {fcn: u.inc, fill: {uno: 1}}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: {fcn: u.inc}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  attr:
    pool: x
    path: [[somethingelse], [one], 2, input]
    to: {fcn: u.inc}
    init:
      value: {one: [{zero: 100}, {one: 101}, {two: 102}]}
      type: SimpleRecord
  path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("two"), 103)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  attr:
    pool: x
    path: [[somethingelse], [one], 2, input]
    to: {fcn: u.inc, fill: {uno: 1}}
    init:
      value: {one: [{zero: 100}, {one: 101}, {two: 102}]}
      type: SimpleRecord
  path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
''')
        self.assertEqual(engine.action("two"), 103)

    def testChangePublicPools(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: 999, init: 123}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
''')
        self.assertEqual(engine.action("whatever"), 999)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: 999, init: 123}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
''')
        self.assertEqual(engine.action("whatever"), 999)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: 999, init: 123}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
''')
        self.assertEqual(engine.action("somethingelse"), 999)

    def testChangePublicPoolsWithFcnDef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: 0}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
''')
        self.assertEqual(engine.action("whatever"), 13)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: 0}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
''')
        self.assertEqual(engine.action("whatever"), 13)

    def testChangePublicPoolsWithFcnRef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: {fcn: u.inc}, init: 0}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("whatever"), 13)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: {fcn: u.inc, fill: {uno: 1}}, init: 0}
  - {pool: x, path: [input]}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
''')
        self.assertEqual(engine.action("whatever"), 13)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [input], to: {fcn: u.inc}, init: 0}
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("whatever"), 13)

    def testChangeDeepPublicPools(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: 999, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
''')
        self.assertEqual(engine.action("two"), 999)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: 999, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
''')
        self.assertEqual(engine.action("two"), 999)

        engine, = PFAEngine.fromYaml('''
input: string
output: {type: map, values: int}
action:
  - attr: {pool: x, path: [[somethingelse], [one], 2, input], to: 999, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102, glue: 103}]}, type: SimpleRecord}}
    path: [[one], 2]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
''')
        self.assertEqual(engine.action("two"), {"two": 999, "glue": 103})

    def testChangeDeepPublicPoolsWithFcnDef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  attr:
    pool: x
    path: [[somethingelse], [one], 2, input]
    to: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
    init:
      value: {one: [{zero: 100}, {one: 101}, {two: 102}]}
      type: SimpleRecord
  path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
''')
        self.assertEqual(engine.action("two"), 103)

    def testChangeDeepPublicPoolsWithFcnRef(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {fcn: u.inc}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {pool: x, path: [[whatever], [one], 2, input], to: {fcn: u.inc, fill: {uno: 1}}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
  - {pool: x, path: [[whatever], [one], 2, input]}
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - attr: {pool: x, path: [[whatever], [one], 2, input], to: {fcn: u.inc}, init: {value: {one: [{zero: 100}, {one: 101}, {two: 102}]}, type: SimpleRecord}}
    path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("two"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  attr:
    pool: x
    path: [[somethingelse], [one], 2, input]
    to: {fcn: u.inc}
    init:
      value: {one: [{zero: 100}, {one: 101}, {two: 102}]}
      type: SimpleRecord
  path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
fcns:
  inc: {params: [{z: int}], ret: int, do: [{+: [z, 1]}]}
''')
        self.assertEqual(engine.action("two"), 103)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  attr:
    pool: x
    path: [[somethingelse], [one], 2, input]
    to: {fcn: u.inc, fill: {uno: 1}}
    init:
      value: {one: [{zero: 100}, {one: 101}, {two: 102}]}
      type: SimpleRecord
  path: [[one], 2, input]
pools:
  x: {type: {type: record, name: SimpleRecord, fields: [{name: one, type: {type: array, items: {type: map, values: int}}}]}, init: {whatever: {one: [{zero: 0}, {one: 1}, {two: 2}]}}, shared: true}
fcns:
  inc: {params: [{z: int}, {uno: int}], ret: int, do: [{+: [z, uno]}]}
''')
        self.assertEqual(engine.action("two"), 103)

    def testHandleNullCases(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {cell: notempty}
  - input
cells:
  notempty: {type: "null", init: null}
''')
        engine.action("hey")

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: string
output: string
action:
  - {cell: empty}
  - input
cells:
  notempty: {type: "null", init: null}
'''))

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [[set]]}
  - input
cells:
  notempty: {type: {type: map, values: "null"}, init: {this: null, is: null, a: null, set: null}}
''')
        engine.action("hey")

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [[notinset]]}
  - input
cells:
  notempty: {type: {type: map, values: "null"}, init: {this: null, is: null, a: null, set: null}}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("hey"))

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [0]}
  - input
cells:
  notempty: {type: {type: array, items: "null"}, init: [null, null, null, null]}
''')
        engine.action("hey")

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [999]}
  - input
cells:
  notempty: {type: {type: array, items: "null"}, init: [null, null, null, null]}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("hey"))

    def testHandleNullCases2(self):
        self.assertRaises(PFASyntaxException, lambda: PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: []}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}}
'''))

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}}
''')
        engine.action("hey")

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: [[shouldfail]]}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("hey"))

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], [set]]}
  - input
pools:
  notempty: {type: {type: map, values: "null"}, init: {whatever: {this: null, is: null, a: null, set: null}}}
''')
        engine.action("hey")

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], [notinset]]}
  - input
pools:
  notempty: {type: {type: map, values: "null"}, init: {whatever: {this: null, is: null, a: null, set: null}}}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("hey"))

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], 0]}
  - input
pools:
  notempty: {type: {type: array, items: "null"}, init: {whatever: [null, null, null, null]}}
''')
        engine.action("hey")

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], 999]}
  - input
pools:
  notempty: {type: {type: array, items: "null"}, init: {whatever: [null, null, null, null]}}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("hey"))

    def testHandleNullCases3(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {cell: notempty}
  - input
cells:
  notempty: {type: "null", init: null, shared: true}
''')
        engine.action("hey")

        self.assertRaises(PFASemanticException, lambda: PFAEngine.fromYaml('''
input: string
output: string
action:
  - {cell: empty}
  - input
cells:
  notempty: {type: "null", init: null, shared: true}
'''))

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [[set]]}
  - input
cells:
  notempty: {type: {type: map, values: "null"}, init: {this: null, is: null, a: null, set: null}, shared: true}
''')
        engine.action("hey")

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [[notinset]]}
  - input
cells:
  notempty: {type: {type: map, values: "null"}, init: {this: null, is: null, a: null, set: null}, shared: true}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("hey"))

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [0]}
  - input
cells:
  notempty: {type: {type: array, items: "null"}, init: [null, null, null, null], shared: true}
''')
        engine.action("hey")

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {cell: notempty}
  - {cell: notempty, path: [999]}
  - input
cells:
  notempty: {type: {type: array, items: "null"}, init: [null, null, null, null], shared: true}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("hey"))

    def testHandleNullCases4(self):
        self.assertRaises(PFASyntaxException, lambda: PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: []}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}, shared: true}
'''))

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}, shared: true}
''')
        engine.action("hey")

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: [[shouldfail]]}
  - input
pools:
  notempty: {type: "null", init: {whatever: null}, shared: true}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("hey"))

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], [set]]}
  - input
pools:
  notempty: {type: {type: map, values: "null"}, init: {whatever: {this: null, is: null, a: null, set: null}}, shared: true}
''')
        engine.action("hey")

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], [notinset]]}
  - input
pools:
  notempty: {type: {type: map, values: "null"}, init: {whatever: {this: null, is: null, a: null, set: null}}, shared: true}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("hey"))

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], 0]}
  - input
pools:
  notempty: {type: {type: array, items: "null"}, init: {whatever: [null, null, null, null]}, shared: true}
''')
        engine.action("hey")

        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {pool: notempty, path: [[whatever]]}
  - {pool: notempty, path: [[whatever], 999]}
  - input
pools:
  notempty: {type: {type: array, items: "null"}, init: {whatever: [null, null, null, null]}, shared: true}
''')
        self.assertRaises(PFARuntimeException, lambda: engine.action("hey"))

    def testHandleATypicalUseCase(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
method: map
action:
  - {pool: tally, path: [[x]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[x]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[x]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[y]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[y]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[x]]}
pools:
  tally: {type: int, init: {}}
fcns:
  inc: {params: [{i: int}], ret: int, do: [{+: [i, 1]}]}
''')
        self.assertEqual(engine.action("hey"), 3)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
method: map
action:
  - {pool: tally, path: [[x]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[x]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[x]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[y]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[y]], to: {fcn: u.inc}, init: 0}
  - {pool: tally, path: [[y]]}
pools:
  tally: {type: int, init: {}}
fcns:
  inc: {params: [{i: int}], ret: int, do: [{+: [i, 1]}]}
''')
        self.assertEqual(engine.action("hey"), 2)

    def testRemovePrivatePoolItems(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
pools:
  x: {type: int, init: {whatever: 12}}
action:
  - {pool: x, del: input}
  - {pool: x, path: [{string: "whatever"}]}
''')
        self.assertEqual(engine.action("somesuch"), 12)
        self.assertRaises(PFARuntimeException, lambda: engine.action("whatever"))

    def testRemovePublicPoolItems(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
pools:
  x: {type: int, init: {whatever: 12}, shared: true}
action:
  - {pool: x, del: input}
  - {pool: x, path: [{string: "whatever"}]}
''')
        self.assertEqual(engine.action("somesuch"), 12)
        self.assertRaises(PFARuntimeException, lambda: engine.action("whatever"))

    def testStopIntRunningProcess(self):
        def go():
            engine, = PFAEngine.fromYaml('''
input: string
output: "null"
action:
  - for: {x: 0}
    while: {"!=": [x, -5]}
    step: {x: {+: [x, 1]}}
    do: [x]
options:
  timeout: 1000
''')
            engine.action("hey")
        self.assertRaises(PFATimeoutException, go)

    def testBeginAndEnd(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
begin:
  - {cell: x, to: {params: [{in: int}], ret: int, do: [{+: [in, 100]}]}}
action:
  - {cell: x, to: {params: [{in: int}], ret: int, do: [{+: [in, 33]}]}}
  - {cell: x}
end:
  - {cell: x, to: {params: [{in: int}], ret: int, do: [{+: [in, 40]}]}}
cells:
  x: {type: int, init: 0}
''')
        engine.begin()
        engine.action("hey")
        engine.end()
        self.assertEqual(engine.action("hey"), 206)
        
    def testCallGraph(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [], ret: int, do: [{+: [1, 1]}]}
  b: {params: [], ret: int, do: [{u.a: []}]}
  c: {params: [], ret: int, do: [{u.c: []}]}
  d: {params: [], ret: int, do: [{+: [{u.b: []}, {u.c: []}]}]}
  c1: {params: [], ret: int, do: [{u.c2: []}]}
  c2: {params: [], ret: int, do: [{u.c1: []}]}
""")
        self.assertEqual(engine.callGraph, {"u.a": set(["+", "(int)"]), "u.b": set(["u.a"]), "u.c": set(["u.c"]), "u.d": set(["+", "u.b", "u.c"]), "u.c1": set(["u.c2"]), "u.c2": set(["u.c1"]), "(begin)": set([]), "(action)": set([]), "(end)": set([])})

        self.assertEqual(engine.calledBy("u.d"), set(["+", "u.a", "u.b", "u.c", "(int)"]))
        self.assertTrue(engine.isRecursive("u.c"))
        self.assertTrue(engine.isRecursive("u.c1"))
        self.assertFalse(engine.isRecursive("u.d"))
        self.assertTrue(engine.hasRecursive("u.d"))

        self.assertEqual(engine.callDepth("u.a"), 1)
        self.assertEqual(engine.callDepth("u.b"), 2)
        self.assertEqual(engine.callDepth("u.d"), float("inf"))
        self.assertEqual(engine.callDepth("u.c"), float("inf"))

    def testRefuseSituationsThatCouldLeadToDeadlock(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{cell: x, to: {fcn: u.b}}]}
  b: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}
  c: {params: [{x: "null"}], ret: "null", do: [null]}
cells:
  x: {type: "null", init: null}
  y: {type: "null", init: null}
""")

        engine, = PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{cell: x, to: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}}]}
  c: {params: [{x: "null"}], ret: "null", do: [null]}
cells:
  x: {type: "null", init: null}
  y: {type: "null", init: null}
""")

        self.assertRaises(PFAInitializationException, lambda: PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{cell: x, to: {fcn: u.b}}]}
  b: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}
  c: {params: [{x: "null"}], ret: "null", do: [{cell: y, to: null}]}
cells:
  x: {type: "null", init: null}
  y: {type: "null", init: null}
"""))

        self.assertRaises(PFAInitializationException, lambda: PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{cell: x, to: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}}]}
  c: {params: [{x: "null"}], ret: "null", do: [{cell: y, to: null}]}
cells:
  x: {type: "null", init: null}
  y: {type: "null", init: null}
"""))

        engine, = PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{pool: x, path: [[whatever]], to: {fcn: u.b}, init: null}]}
  b: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}
  c: {params: [{x: "null"}], ret: "null", do: [null]}
pools:
  x: {type: "null", init: {whatever: null}}
  y: {type: "null", init: {whatever: null}}
""")

        engine, = PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{pool: x, path: [[whatever]], to: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}, init: null}]}
  c: {params: [{x: "null"}], ret: "null", do: [null]}
pools:
  x: {type: "null", init: {whatever: null}}
  y: {type: "null", init: {whatever: null}}
""")

        self.assertRaises(PFAInitializationException, lambda: PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{pool: x, path: [[whatever]], to: {fcn: u.b}, init: null}]}
  b: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}
  c: {params: [{x: "null"}], ret: "null", do: [{pool: y, path: [[whatever]], to: null, init: null}]}
pools:
  x: {type: "null", init: {whatever: null}}
  y: {type: "null", init: {whatever: null}}
"""))

        self.assertRaises(PFAInitializationException, lambda: PFAEngine.fromYaml("""
input: string
output: string
action:
  - input
fcns:
  a: {params: [{x: "null"}], ret: "null", do: [{pool: x, path: [[whatever]], to: {params: [{x: "null"}], ret: "null", do: [{u.c: [null]}]}, init: null}]}
  c: {params: [{x: "null"}], ret: "null", do: [{pool: y, path: [[whatever]], to: null, init: null}]}
pools:
  x: {type: "null", init: {whatever: null}}
  y: {type: "null", init: {whatever: null}}
"""))

    def testCellRollback(self):
        engine, = PFAEngine.fromYaml("""
input: boolean
output: int
action:
  - cell: x
    to:
      params: [{y: int}]
      ret: int
      do: {+: [y, 1]}
  - if: input
    then: {error: "crash!"}
  - cell: x
cells:
  x: {type: int, init: 0, rollback: false}
""")
        self.assertEqual(engine.action(False), 1)
        self.assertEqual(engine.action(False), 2)
        try:
            engine.action(True)
        except PFAUserException:
            pass
        self.assertEqual(engine.action(False), 4)
        try:
            engine.action(True)
        except PFAUserException:
            pass
        try:
            engine.action(True)
        except PFAUserException:
            pass
        try:
            engine.action(True)
        except PFAUserException:
            pass
        self.assertEqual(engine.action(False), 8)
        self.assertEqual(engine.action(False), 9)

        engine, = PFAEngine.fromYaml("""
input: boolean
output: int
action:
  - cell: x
    to:
      params: [{y: int}]
      ret: int
      do: {+: [y, 1]}
  - if: input
    then: {error: "crash!"}
  - cell: x
cells:
  x: {type: int, init: 0, rollback: true}
""")
        self.assertEqual(engine.action(False), 1)
        self.assertEqual(engine.action(False), 2)
        try:
            engine.action(True)
        except PFAUserException:
            pass
        self.assertEqual(engine.action(False), 3)
        try:
            engine.action(True)
        except PFAUserException:
            pass
        try:
            engine.action(True)
        except PFAUserException:
            pass
        try:
            engine.action(True)
        except PFAUserException:
            pass
        self.assertEqual(engine.action(False), 4)
        self.assertEqual(engine.action(False), 5)

    def testPoolRollback(self):
        engine, = PFAEngine.fromYaml("""
input: boolean
output: int
action:
  - pool: x
    path: [[z]]
    init: 0
    to:
      params: [{y: int}]
      ret: int
      do: {+: [y, 1]}
  - if: input
    then: {error: "crash!"}
  - pool: x
    path: [[z]]
pools:
  x: {type: int, init: {z: 0}, rollback: false}
""")
        self.assertEqual(engine.action(False), 1)
        self.assertEqual(engine.action(False), 2)
        try:
            engine.action(True)
        except PFAUserException:
            pass
        self.assertEqual(engine.action(False), 4)
        try:
            engine.action(True)
        except PFAUserException:
            pass
        try:
            engine.action(True)
        except PFAUserException:
            pass
        try:
            engine.action(True)
        except PFAUserException:
            pass
        self.assertEqual(engine.action(False), 8)
        self.assertEqual(engine.action(False), 9)

        engine, = PFAEngine.fromYaml("""
input: boolean
output: int
action:
  - pool: x
    path: [[z]]
    init: 0
    to:
      params: [{y: int}]
      ret: int
      do: {+: [y, 1]}
  - if: input
    then: {error: "crash!"}
  - pool: x
    path: [[z]]
pools:
  x: {type: int, init: {z: 0}, rollback: true}
""")
        self.assertEqual(engine.action(False), 1)
        self.assertEqual(engine.action(False), 2)
        try:
            engine.action(True)
        except PFAUserException:
            pass
        self.assertEqual(engine.action(False), 3)
        try:
            engine.action(True)
        except PFAUserException:
            pass
        try:
            engine.action(True)
        except PFAUserException:
            pass
        try:
            engine.action(True)
        except PFAUserException:
            pass
        self.assertEqual(engine.action(False), 4)
        self.assertEqual(engine.action(False), 5)

    def testCallUserFunction(self):
        engine, = PFAEngine.fromYaml('''
input:
  type: enum
  name: Input
  symbols: [one, two, three]
output: double
action:
  call: input
  args: [100.0]
fcns:
  one:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.1]}
  two:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.2]}
  three:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.3]}
''')
        self.assertAlmostEqual(engine.action("one"), 100.1, places=2)
        self.assertAlmostEqual(engine.action("two"), 100.2, places=2)
        self.assertAlmostEqual(engine.action("three"), 100.3, places=2)

        self.assertEqual(engine.callGraph["(action)"], set(["u.one", "u.two", "u.three", "(double)"]))

        engine, = PFAEngine.fromYaml('''
input:
  type: enum
  name: Input
  symbols: [one, two, three]
output: [double, string]
action:
  call: input
  args: [100.0]
fcns:
  one:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.1]}
  two:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.2]}
  three:
    params: [{x: double}]
    ret: string
    do: {string: hello}
''')
        self.assertAlmostEqual(engine.action("one"), 100.1, places=2)
        self.assertAlmostEqual(engine.action("two"), 100.2, places=2)
        self.assertEqual(engine.action("three"), "hello")

        def bad():
            PFAEngine.fromYaml('''
input:
  type: enum
  name: Input
  symbols: [one, two, three]
output: double
action:
  call: input
  args: [100.0]
fcns:
  one:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.1]}
  two:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.2]}
  three:
    params: [{x: double}]
    ret: string
    do: {string: hello}
''')
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
input:
  type: enum
  name: Input
  symbols: [one, two, three]
output: double
action:
  call: input
  args: [100.0]
fcns:
  one:
    params: [{x: double}]
    ret: double
    do: {+: [x, 0.1]}
  two:
    params: [{x: string}]
    ret: double
    do: 999.9
  three:
    params: [{x: double}]
    ret: string
    do: {string: hello}
''')
        self.assertRaises(PFASemanticException, bad)

    def testFcnRefFillUserFunctions(self):
        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: int}
action:
  a.map:
    - value: [1, 2, 3, 4, 5]
      type: {type: array, items: int}
    - fcn: u.callme
      fill: {y: 2}
fcns:
  callme:
    params: [{x: int}, {y: int}]
    ret: int
    do: {"*": [{+: [x, 1000]}, y]}
''')
        self.assertEqual(engine.action(None), [2002, 2004, 2006, 2008, 2010])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: int}
action:
  a.map:
    - value: [1, 2, 3, 4, 5]
      type: {type: array, items: int}
    - fcn: u.callme
      fill: {x: 2}
fcns:
  callme:
    params: [{x: int}, {y: int}]
    ret: int
    do: {"*": [{+: [x, 1000]}, y]}
''')
        self.assertEqual(engine.action(None), [1002, 2004, 3006, 4008, 5010])

        engine, = PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: int}
action:
  a.map:
    - value: [1, 2, 3, 4, 5]
      type: {type: array, items: int}
    - fcn: u.callme
      fill: {x: 10, y: 100}
fcns:
  callme:
    params: [{x: int}, {y: int}, {z: int}]
    ret: int
    do: {+: [x, {+: [y, z]}]}
''')
        self.assertEqual(engine.action(None), [111, 112, 113, 114, 115])

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: int}
action:
  a.map:
    - value: [1, 2, 3, 4, 5]
      type: {type: array, items: int}
    - fcn: u.callme
      fill: {x: 10, yy: 100}
fcns:
  callme:
    params: [{x: int}, {y: int}, {z: int}]
    ret: int
    do: {+: [x, {+: [y, z]}]}
''')
        self.assertRaises(PFASemanticException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: "null"
output: {type: array, items: int}
action:
  a.map:
    - value: [1, 2, 3, 4, 5]
      type: {type: array, items: int}
    - fcn: u.callme
      fill: {}
fcns:
  callme:
    params: [{x: int}, {y: int}, {z: int}]
    ret: int
    do: {+: [x, {+: [y, z]}]}
''')
        self.assertRaises(PFASyntaxException, bad)

    def testFcnRefFillLibraryFunctions(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: {type: array, items: string}
action:
  a.map:
    - value: ["abcdefghijklmnopqrstuvwxyz", "ABCDEFGHIJKLMNOPQRSTUVWXYZ"]
      type: {type: array, items: string}
    - fcn: s.substr
      fill: {start: 0, end: input}
''')
        self.assertEqual(engine.action(1), ["a", "A"])
        self.assertEqual(engine.action(3), ["abc", "ABC"])
        self.assertEqual(engine.action(5), ["abcde", "ABCDE"])
        self.assertEqual(engine.action(10), ["abcdefghij", "ABCDEFGHIJ"])
        self.assertEqual(engine.action(15), ["abcdefghijklmno", "ABCDEFGHIJKLMNO"])

    def testTryCatchWithoutFiltering(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: [int, "null"]
action:
  try:
    if: {"==": [input, 3]}
    then: {error: "ouch"}
    else: input
''')
        self.assertEqual(engine.action(1), 1)
        self.assertEqual(engine.action(2), 2)
        self.assertEqual(engine.action(3), None)
        self.assertEqual(engine.action(4), 4)
        self.assertEqual(engine.action(5), 5)

    def testTryCatchWithFiltering(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: [int, "null"]
action:
  try:
    cond:
      - if: {"==": [input, 3]}
        then: {error: "ouch"}
      - if: {"==": [input, 4]}
        then: {error: "yowzers"}
    else: input
  filter:
    - ouch
''')
        self.assertEqual(engine.action(1), 1)
        self.assertEqual(engine.action(2), 2)
        self.assertEqual(engine.action(3), None)
        self.assertRaises(PFAUserException, lambda: engine.action(4))
        self.assertEqual(engine.action(5), 5)

    def testTryCatchWithFiltering2(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: [int, "null"]
action:
  try:
    cond:
      - if: {"==": [input, 3]}
        then: {error: "ouch", code: -99}
      - if: {"==": [input, 4]}
        then: {error: "yowzers", code: -100}
    else: input
  filter:
    - -99
''')
        self.assertEqual(engine.action(1), 1)
        self.assertEqual(engine.action(2), 2)
        self.assertEqual(engine.action(3), None)
        self.assertRaises(PFAUserException, lambda: engine.action(4))
        self.assertEqual(engine.action(5), 5)

if __name__ == "__main__":
    unittest.main()
