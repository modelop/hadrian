#!/usr/bin/env python

import unittest

from titus.genpy import PFAEngine
from titus.errors import *
    
class TestLib1String(unittest.TestCase):
    def testGetLength(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {s.len: [input]}
''')
        self.assertEqual(engine.action("hello"), 5)

    def testGetSubstring(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: string
action:
  - {s.substr: [[ABCDEFGHIJKLMNOPQRSTUVWXYZ], 5, input]}
''')
        self.assertEqual(engine.action(10), "FGHIJ")
        self.assertEqual(engine.action(-10), "FGHIJKLMNOP")
        self.assertEqual(engine.action(0), "")
        self.assertEqual(engine.action(1), "")
        self.assertEqual(engine.action(100), "FGHIJKLMNOPQRSTUVWXYZ")

    def testGetSubstring(self):
        engine, = PFAEngine.fromYaml('''
input: int
output: string
action:
  - {s.substrto: [[ABCDEFGHIJKLMNOPQRSTUVWXYZ], 5, input, [...]]}
''')
        self.assertEqual(engine.action(10), "ABCDE...KLMNOPQRSTUVWXYZ")
        self.assertEqual(engine.action(-10), "ABCDE...QRSTUVWXYZ")
        self.assertEqual(engine.action(0), "ABCDE...FGHIJKLMNOPQRSTUVWXYZ")
        self.assertEqual(engine.action(1), "ABCDE...FGHIJKLMNOPQRSTUVWXYZ")
        self.assertEqual(engine.action(100), "ABCDE...")

    def testDoContains(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  - {s.contains: [input, [DEFG]]}
''')
        self.assertTrue(engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
        self.assertFalse(engine.action("ack! ack! ack!"))

    def testDoCount(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {s.count: [input, [ack!]]}
''')
        self.assertEqual(engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), 0)
        self.assertEqual(engine.action("ack! ack! ack!"), 3)
        self.assertEqual(engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsf"), 2)
        self.assertEqual(engine.action("adfasdfadack!asdfasdfasdf"), 1)

    def testDoIndex(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {s.index: [input, [ack!]]}
''')
        self.assertEqual(engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), -1)
        self.assertEqual(engine.action("ack! ack! ack!"), 0)
        self.assertEqual(engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsf"), 10)
        self.assertEqual(engine.action("adfasdfadack!asdfasdfasdf"), 9)

    def testDoRIndex(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {s.rindex: [input, [ack!]]}
''')
        self.assertEqual(engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ"), -1)
        self.assertEqual(engine.action("ack! ack! ack!"), 10)
        self.assertEqual(engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsf"), 36)
        self.assertEqual(engine.action("adfasdfadack!asdfasdfasdf"), 9)

    def testDoStartswith(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  - {s.startswith: [input, [ack!]]}
''')
        self.assertFalse(engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
        self.assertTrue(engine.action("ack! ack! ack!"))
        self.assertFalse(engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsf"))
        self.assertFalse(engine.action("adfasdfadack!asdfasdfasdf"))

    def testDoEndswith(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  - {s.endswith: [input, [ack!]]}
''')
        self.assertFalse(engine.action("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
        self.assertTrue(engine.action("ack! ack! ack!"))
        self.assertTrue(engine.action("adfasdfadfack!asdfasdf ackasdfadfd! ack!asdfadsfack!"))
        self.assertFalse(engine.action("adfasdfadack!asdfasdfasdf"))

    def testDoJoin(self):
        engine, = PFAEngine.fromYaml('''
input: {type: array, items: string}
output: string
action:
  - {s.join: [input, [", "]]}
''')
        self.assertEqual(engine.action(["one", "two", "three"]), "one, two, three")

    def testDoSplit(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: {type: array, items: string}
action:
  - {s.split: [input, [", "]]}
''')
        self.assertEqual(engine.action("one, two, three"), ["one", "two", "three"])

    def testDoJoin2(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.concat: [[one], input]}
''')
        self.assertEqual(engine.action("two"), "onetwo")

    def testRepeat(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.repeat: [input, 5]}
''')
        self.assertEqual(engine.action("hey"), "heyheyheyheyhey")

    def testDoLower(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.lower: [input]}
''')
        self.assertEqual(engine.action("hey"), "hey")
        self.assertEqual(engine.action("Hey"), "hey")
        self.assertEqual(engine.action("HEY"), "hey")
        self.assertEqual(engine.action("hEy"), "hey")
        self.assertEqual(engine.action("heY"), "hey")

    def testDoUpper(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.upper: [input]}
''')
        self.assertEqual(engine.action("hey"), "HEY")
        self.assertEqual(engine.action("Hey"), "HEY")
        self.assertEqual(engine.action("HEY"), "HEY")
        self.assertEqual(engine.action("hEy"), "HEY")
        self.assertEqual(engine.action("heY"), "HEY")

    def testDoLStrip(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.lstrip: [input, ["h "]]}
''')
        self.assertEqual(engine.action("hey"), "ey")
        self.assertEqual(engine.action(" hey"), "ey")
        self.assertEqual(engine.action("  hey"), "ey")
        self.assertEqual(engine.action("hey "), "ey ")
        self.assertEqual(engine.action("Hey"), "Hey")
        self.assertEqual(engine.action(" Hey"), "Hey")
        self.assertEqual(engine.action("  Hey"), "Hey")
        self.assertEqual(engine.action("Hey "), "Hey ")

    def testDoRStrip(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.rstrip: [input, ["y "]]}
''')
        self.assertEqual(engine.action("hey"), "he")
        self.assertEqual(engine.action("hey "), "he")
        self.assertEqual(engine.action("hey  "), "he")
        self.assertEqual(engine.action(" hey"), " he")
        self.assertEqual(engine.action("heY"), "heY")
        self.assertEqual(engine.action("heY "), "heY")
        self.assertEqual(engine.action("heY  "), "heY")
        self.assertEqual(engine.action(" heY"), " heY")

    def testDoStrip(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.strip: [input, ["hy "]]}
''')
        self.assertEqual(engine.action("hey"), "e")
        self.assertEqual(engine.action("hey "), "e")
        self.assertEqual(engine.action("hey  "), "e")
        self.assertEqual(engine.action(" hey"), "e")
        self.assertEqual(engine.action("HEY"), "HEY")
        self.assertEqual(engine.action("HEY "), "HEY")
        self.assertEqual(engine.action("HEY  "), "HEY")
        self.assertEqual(engine.action(" HEY"), "HEY")

    def testDoReplaceAll(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.replaceall: [input, [ey], [EY]]}
''')
        self.assertEqual(engine.action("hey"), "hEY")
        self.assertEqual(engine.action("hey hey hey"), "hEY hEY hEY")
        self.assertEqual(engine.action("abc"), "abc")
        self.assertEqual(engine.action("yeh yeh yeh"), "yeh yeh yeh")

    def testDoReplaceFirst(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.replacefirst: [input, [ey], [EY]]}
''')
        self.assertEqual(engine.action("hey"), "hEY")
        self.assertEqual(engine.action("hey hey hey"), "hEY hey hey")
        self.assertEqual(engine.action("abc"), "abc")
        self.assertEqual(engine.action("yeh yeh yeh"), "yeh yeh yeh")

    def testDoReplaceLast(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.replacelast: [input, [ey], [EY]]}
''')
        self.assertEqual(engine.action("hey"), "hEY")
        self.assertEqual(engine.action("hey hey hey"), "hey hey hEY")
        self.assertEqual(engine.action("abc"), "abc")
        self.assertEqual(engine.action("yeh yeh yeh"), "yeh yeh yeh")

    def testDoTranslate(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: string
action:
  - {s.translate: [[ABCDEFGHIJKLMNOPQRSTUVWXYZ], [AEIOU], input]}
''')
        self.assertEqual(engine.action("aeiou"), "aBCDeFGHiJKLMNoPQRSTuVWXYZ")
        self.assertEqual(engine.action("aeio"), "aBCDeFGHiJKLMNoPQRSTVWXYZ")
        self.assertEqual(engine.action(""), "BCDFGHJKLMNPQRSTVWXYZ")
        self.assertEqual(engine.action("aeiouuuu"), "aBCDeFGHiJKLMNoPQRSTuVWXYZ")

if __name__ == "__main__":
    unittest.main()
