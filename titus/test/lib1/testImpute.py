#!/usr/bin/env python

import unittest

from titus.genpy import PFAEngine
from titus.errors import *
    
class TestLib1Impute(unittest.TestCase):
    def testErrorOnNull(self):
        engine, = PFAEngine.fromYaml('''
input: [int, "null"]
output: int
action:
  - {impute.errorOnNull: [input]}
''')
        self.assertEqual(engine.action(3), 3)
        self.assertEqual(engine.action(12), 12)
        self.assertRaises(PFARuntimeException, lambda: engine.action(None))
        self.assertEqual(engine.action(5), 5)

    def testDefaultOnNull(self):
        engine, = PFAEngine.fromYaml('''
input: [int, "null"]
output: int
action:
  - {impute.defaultOnNull: [input, 12]}
''')
        self.assertEqual(engine.action(3), 3)
        self.assertEqual(engine.action(12), 12)
        self.assertEqual(engine.action(None), 12)
        self.assertEqual(engine.action(5), 5)

        engine, = PFAEngine.fromYaml('''
input: [string, "null"]
output: string
action:
  - {impute.defaultOnNull: [input, [oops]]}
''')
        self.assertEqual(engine.action("one"), "one")
        self.assertEqual(engine.action("two"), "two")
        self.assertEqual(engine.action(None), "oops")
        self.assertEqual(engine.action("four"), "four")

        def bad():
            PFAEngine.fromYaml('''
input: ["null", "null"]
output: "null"
action:
  - {impute.defaultOnNull: [input, "null"]}
''')
        self.assertRaises(SchemaParseException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: [[int, string], "null"]
output: [int, string]
action:
  - {impute.defaultOnNull: [input, 12]}
''')
        self.assertRaises(SchemaParseException, bad)

        def bad():
            PFAEngine.fromYaml('''
input: [int, string, "null"]
output: int
action:
  - {impute.defaultOnNull: [input, 12]}
''')
        self.assertRaises(PFASemanticException, bad)

if __name__ == "__main__":
    unittest.main()
