#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 
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
# See: string the License for the specific language governing permissions and
# limitations under the License.

import unittest
import math
import struct

from titus.genpy import PFAEngine
from titus.errors import *

# libc regexp library has no support for multibyte characters.  This causes a difference between 
# hadrian and titus regex libs.  Unittests for multibye characters (non-ascii) are commented out.
class TestLib1Regex(unittest.TestCase):
    def testMemory(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.rindex: [input, [ab(c|d)*]]}
""")
        import resource, time
        memusage_1 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        for i in range(0, 10000):
            engine.action("abcccdc")
        memusage_2 = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss

        print("\nMemory usage before: {0}, after: {1}".format(memusage_1, memusage_2))


    def testPosix(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "[hc]+at"}]}
''')
        self.assertEqual(engine.action("hat"),  [0,3])
        self.assertEqual(engine.action("cat"),  [0,3])
        self.assertEqual(engine.action("hhat"), [0,4])
        self.assertEqual(engine.action("chat"), [0,4])
        self.assertEqual(engine.action("hcat"), [0,4])
        self.assertEqual(engine.action("cchchat"), [0,7])
        self.assertEqual(engine.action("at"),  [])

        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "[hc]?at"}]}
""")
        self.assertEqual(engine.action("hat"), [0,3])
        self.assertEqual(engine.action("cat"), [0,3])
        self.assertEqual(engine.action("at"),  [0,2])
        self.assertEqual(engine.action("dog"), [])


        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "[hc]*at"}]}
""")
        self.assertEqual(engine.action("hat"),     [0,3])
        self.assertEqual(engine.action("cat"),     [0,3])
        self.assertEqual(engine.action("hhat"),    [0,4])
        self.assertEqual(engine.action("chat"),    [0,4])
        self.assertEqual(engine.action("hcat"),    [0,4])
        self.assertEqual(engine.action("cchchat"), [0,7])
        self.assertEqual(engine.action("at"),      [0,2])

        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "cat|dog"}]}
""")
        self.assertEqual(engine.action("dog"),  [0,3])
        self.assertEqual(engine.action("cat"),  [0,3])
        self.assertEqual(engine.action("mouse"),[])

        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "(abc){2}|(def){2}"}]}
""")
        self.assertEqual(engine.action("abcabc"),  [0,6])
        self.assertEqual(engine.action("defdef"),  [0,6])
        self.assertEqual(engine.action("XKASGJ8"), [])

       # backreferences
        engine, = PFAEngine.fromYaml(r"""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, [(the )\1]]}
""")
        self.assertEqual(engine.action("Paris in the the spring"), [9,17])

        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "[[:upper:]ab]"}]}
""")
        self.assertEqual(engine.action("GHab"), [0,1])
        self.assertEqual(engine.action("ab"),   [0,1])
        self.assertEqual(engine.action("p"),    [])


    def testIndex(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, {string: "ab(c|d)*"}]}
""")
        self.assertEqual(engine.action("abcccdc"), [0,7])
        self.assertEqual(engine.action("abddddd"), [0,7])
        self.assertEqual(engine.action("XKASGJ8"), [])

        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.index: [input, [dog]]}
""")
        self.assertEqual(engine.action("999dogggggg"), [3,6])
        self.assertEqual(engine.action("cat"),         [])

# test non ascii strings
#        engine, = PFAEngine.fromYaml('''
#input: string
#output: {type: array, items: int}
#action:
#  - {re.index: [input, {string: "对讲(机|p)*"}]}
#''')
#        self.assertEqual(engine.action("对讲机机机机机机"),  [0,8])
#        self.assertEqual(engine.action("对讲pppppppppp"), [0,12])

# check byte input
        engine, = PFAEngine.fromYaml('''
input: bytes
output: {type: array, items: int}
action:
  - re.index: [input, {bytes.encodeUtf8: {string: "ab(c|d)*"}}]
''')
        self.assertEqual(engine.action("abcccdc"), [0,7])
        self.assertEqual(engine.action("对讲机abcccdc"), [9,16])
        self.assertEqual(engine.action("对讲机abcccdc讲机"), [9,16])


    def testContains(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  - {re.contains: [input, [ab(c|d)*]]}
''')
        self.assertEqual(engine.action("wio239fj6abcccdc"), True)
        self.assertEqual(engine.action("938736362abddddd"), True)
        self.assertEqual(engine.action("938272XKASGJ8"),    False)

        engine, = PFAEngine.fromYaml('''
input: string
output: boolean
action:
  - {re.contains: [input, [dog]]}
''')
        self.assertEqual(engine.action("9999doggggggg"), True)
        self.assertEqual(engine.action("928373cat"),     False)

# check non ascii strings
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: boolean
#action:
#  - {re.contains: [input, [对讲机(讲|机)*]]}
#""")
#        self.assertEqual(engine.action("abcccdc"),  False)
#        self.assertEqual(engine.action("xyzzzz对讲机机abcc"), True)


# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: boolean
action:
  - re.contains: [input, {bytes.encodeUtf8: {string: "对讲机(讲|机)*"}}]
""")
        self.assertEqual(engine.action("abcccdc"), False)
        self.assertEqual(engine.action("xyzzzz对讲机机abcc"), True)


    def testCount(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {re.count: [input, [ab(c|d)*]]}
''')
        self.assertEqual(engine.action("938272XKASGJ8"), 0)
        self.assertEqual(engine.action("iabc1abc2abc2abc"), 4)
        self.assertEqual(engine.action("938736362abddddd"), 1)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {re.count: [input, [dog]]}
''')
        self.assertEqual(engine.action("999doggggggg"), 1)
        self.assertEqual(engine.action("9233857cat"),   0)
        self.assertEqual(engine.action("dogdogdogdogdog"), 5)
        self.assertEqual(engine.action("dogDogdogdogdog"), 4)
        self.assertEqual(engine.action("dogdog \n dogdogdog"), 5)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {re.count: [input, [a*]]}
''')
        self.assertEqual(engine.action("aaaaaaaaaaaaaaa"), 1)

        engine, = PFAEngine.fromYaml('''
input: string
output: int
action:
  - {re.count: [input, [ba]]}
''')
        self.assertEqual(engine.action("ababababababababababa"), 10)

# check non ascii strings
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: int
#action:
#  - {re.count: [input, [对+]]}
#""")
#        self.assertEqual(engine.action("abcccdc"), 0)
#        self.assertEqual(engine.action("xyzzzz对对对对讲机机abcc对讲机机mmmmm对对对讲机机aa"), 3)

# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: int
action:
  - re.count: [input, {bytes.encodeUtf8: {string: "对讲机(讲|机)*"}}]
""")
        self.assertEqual(engine.action("abcccdc"), 0)
        self.assertEqual(engine.action("xyzzzz对讲机机机abcc对讲机机机mmmmm对讲机机aa"), 3)


    def testrIndex(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.rindex: [input, [ab(c|d)*]]}
""")
        self.assertEqual(engine.action("abcccdc"), [0,7])
        self.assertEqual(engine.action("abddddd"), [0,7])
        self.assertEqual(engine.action("XKASGJ8"), [])

        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: int}
action:
  - {re.rindex: [input, [dog]]}
""")
        self.assertEqual(engine.action("999dogggggg"),  [3,6])
        self.assertEqual(engine.action("cat"),          [])
        self.assertEqual(engine.action("catdogpppdog"), [9,12])


# check non-ascii string input
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: {type: array, items: int}
#action:
#  - {re.rindex: [input, [对讲机(讲|机)*]]}
#""")
#        self.assertEqual(engine.action("abcccdc"), [])
#        self.assertEqual(engine.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa"), [23,27])

# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: int}
action:
  - re.rindex: [input, {bytes.encodeUtf8: {string: "对讲机(讲|机)*"}}]
""")
        self.assertEqual(engine.action("abcccdc"), [])
        self.assertEqual(engine.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa"), [39,51])


    def testGroups(self):
        engine, = PFAEngine.fromYaml('''
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.groups: [input, {string: "(a(b)c)d"}]}
''')
        self.assertEqual(engine.action("abcd"), [[0,4], [0,3], [1,2]])

        engine, = PFAEngine.fromYaml('''
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.groups: [input, {string: "(the )+"}]}
''')
        self.assertEqual(engine.action("Paris in the the spring"), [[9,17], [13,17]])

        engine, = PFAEngine.fromYaml(r'''
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.groups: [input, {string: (the )\1}]}
''')
        self.assertEqual(engine.action("Paris in the the spring"), [[9,17], [9,13]])

        engine, = PFAEngine.fromYaml('''
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.groups: [input, {string: "()(a)bc(def)ghijk"}]}
''')
        self.assertEqual(engine.action("abcdefghijk"), [[0,11], [0,0], [0,1], [3,6]])

# check non-ascii string input
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: {type: array, items: {type: array, items: int}}
#action:
#  - {re.groups: [input, [对讲机(讲|机)*]]}
#""")
#        self.assertEqual(engine.action("abcccdc"), [])
#        self.assertEqual(engine.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa"), [[6,10], [9,10]])

# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: {type: array, items: int}}
action:
  - re.groups: [input, {bytes.encodeUtf8: {string: "对讲机(讲|机)*"}}]
""")
        self.assertEqual(engine.action("abcccdc"), [])
        self.assertEqual(engine.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa"), [[6,18], [15,18]])


    def testindexAll(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.indexall: [input, [ab]]}
""")
        self.assertEqual(engine.action("abcabcabc"),  [[0,2], [3,5], [6,8]])
        self.assertEqual(engine.action("88cabcc"),    [[3,5]])

# backref (include r in string)
        engine, = PFAEngine.fromYaml(r"""
input: string
output: {type: array, items: {type: array, items: int}}
action:
  - {re.indexall: [input, [(the )\1]]}
""")
        self.assertEqual(engine.action("Paris in the the spring, LA in the the summer"), [[9,17], [31,39]])


# check non-ascii string input
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: {type: array, items: {type: array, items: int}}
#action:
#  - {re.indexall: [input, [对讲机(讲|机)*]]}
#""")
#        self.assertEqual(engine.action("abcccdc"), [])
#        self.assertEqual(engine.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa"), [[6,10], [14,18], [23,27]])

# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: {type: array, items: int}}
action:
  - re.indexall: [input, {bytes.encodeUtf8: {string: "对讲机(讲|机)*"}}]   
""")
        self.assertEqual(engine.action("abcccdc"), [])
        self.assertEqual(engine.action("xyzzzz对讲机机abcc对讲机机mmmmm对讲机机aa"), [[6,18], [22,34], [39,51]])


    def testfindAll(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.findall: [input, [ab]]}
""")
        self.assertEqual(engine.action("abcabcabc"), ["ab","ab", "ab"])
        self.assertEqual(engine.action("88cabcc"),   ["ab"])


# check non-ascii string input
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: {type: array, items: string}
#action:
#  - {re.findall: [input, [猫机+猫]]}
#""")
#        self.assertEqual(engine.action("猫机猫oooo猫机机猫ppp猫机机机猫bbbb猫机aaaa猫机机"), ["猫机猫" ,"猫机机猫","猫机机机猫"])

# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: bytes}
action:
  - re.findall: [input, {bytes.encodeUtf8: {string: "ab+"}}]
""")
        self.assertEqual(engine.action("xyz"), [])
        self.assertEqual(engine.action("abc机机abcabc"), ["ab", "ab","ab"] )


    def testfindFirst(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: [string, "null"]
action:
  - {re.findfirst: [input, [ab]]}
""")
        self.assertEqual(engine.action("88ccc555"),  None)
        self.assertEqual(engine.action("abcabcabc"), {"string": "ab"})

# check non-ascii input
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: [string, "null"]
#action:
#  - {re.findfirst: [input, [机机+]]}
#""")
#        self.assertEqual(engine.action("abc机机机abca机机bc  asdkj 机机机sd"), "机机机")
#        self.assertEqual(engine.action("abdefg"), None)

# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: [bytes, "null"]
action:
  - re.findfirst: [input, {bytes.encodeUtf8: {string: "对讲机(讲|机)*"}}]
""")
        self.assertEqual(engine.action("abcde对讲机讲fgg对讲机讲h"), {"bytes": "对讲机讲"})
        self.assertEqual(engine.action("abcdefghijk"), None)


    def testfindGroupsFirst(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.findgroupsfirst: [input, [ab]]}
""")
        self.assertEqual(engine.action("abcabcabc"), ["ab"])
        self.assertEqual(engine.action("88ccc"),     [])

        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.findgroupsfirst: [input, [()(a)bc(def)ghijk]]}
""")
        self.assertEqual(engine.action("abcdefghijk"), ["abcdefghijk", "", "a", "def"])

        engine, = PFAEngine.fromYaml(r"""
input: string
output: {type: array, items: string}
action:
  - {re.findgroupsfirst: [input, [(the.)\1]]}
""")
        self.assertEqual(engine.action("Paris in the the spring"), ["the the ", "the "])

# check non-ascii input
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: {type: array, items: string}
#action:
#  - {re.findgroupsfirst: [input, [机+(机)]]}
#""")
#        self.assertEqual(engine.action("abc机机机机abca机机bc"), ["机机机机","机"] )
#        self.assertEqual(engine.action("abcd"), [])

# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: bytes}
action:
  - re.findgroupsfirst: [input, {bytes.encodeUtf8: {string: "机(机)"}}]
""")
        self.assertEqual(engine.action("abc机机abca机机bc"), ["机机","机"] )
        self.assertEqual(engine.action("abcd"), [])


    def testfindGroupsAll(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: string}}
action:
  - {re.findgroupsall: [input, [ab]]}
""")
        self.assertEqual(engine.action("aabb"),         [["ab"]])
        self.assertEqual(engine.action("kkabkkabkkab"), [["ab"], ["ab"], ["ab"]])

        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: string}}
action:
  - {re.findgroupsall: [input, [()(a)bc(def)ghijk]]}
""")
        self.assertEqual(engine.action("abcdefghijkMMMMMabcdefghijkMMMM"), [["abcdefghijk", "", "a", "def"], ["abcdefghijk","", "a", "def"]])

# check non-ascii input
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: {type: array, items: {type: array, items: string}}
#action:
#  - {re.findgroupsall: [input, [机+(机)]]}
#""")
#        self.assertEqual(engine.action("abc机机机机abca机机bc"), [["机机机机", "机"], ["机机", "机"]])

# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: {type: array, items: bytes}}
action:
  - re.findgroupsall: [input, {bytes.encodeUtf8: {string: "机(机)"}}]
""")
        self.assertEqual(engine.action("abc机机abca机机bc"), [["机机","机"], ["机机","机"]])
        self.assertEqual(engine.action("abcd"), [])


    def testgroupsAll(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: {type: array, items: {type: array, items: int}}}
action:
  - {re.groupsall: [input, [()(a)bc(def)ghijk]]}
""")
        self.assertEqual(engine.action("abcdefghijkMMMMMabcdefghijkMMMM"), [[[0,11], [0,0], [0,1], [3,6]], [[16, 27],[16,16],[16,17], [19,22]]])

## check non-ascii input
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: {type: array, items: {type: array, items: {type: array, items: int}}}
#action:
#  - {re.groupsall: [input, [(机)机]]}
#""")
#        self.assertEqual(engine.action("abc机机abca机机bc"), [[[3,5], [3,4]], [[9,11], [9,10]]])

# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: {type: array, items: {type: array, items: int}}}
action:
  - re.groupsall: [input, {bytes.encodeUtf8: {string: "(机)机"}}]
""")
        self.assertEqual(engine.action("abc机机abca机机bc"), [[[3,9], [3,6]], [[13,19], [13,16]]])


    def testreplaceFirst(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replacefirst: [input, ["ab(c|d)*"], ["person"]]}
""")
        self.assertEqual(engine.action("abcccdcPPPP"),     "personPPPP")
        self.assertEqual(engine.action("PPPPabcccdcPPPP"), "PPPPpersonPPPP")
        self.assertEqual(engine.action("PPPPPPPP"), "PPPPPPPP") 

        engine, = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replacefirst: [input, ["ab(c|d)*"], ["walkie talkie"]]}
""")
        self.assertEqual(engine.action("This abcccdc works better than that abcccdc."), "This walkie talkie works better than that abcccdc.")
# check non-ascii input
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: string
#action:
#  - {re.replacefirst: [input, [对讲机+], ["walkie talkie"]]}
#""")
#        self.assertEqual(engine.action("This 对讲机 works better than that 对讲机."), "This walkie talkie works better than that 对讲机.")

# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: bytes
action:
  - {re.replacefirst: [input, {bytes.encodeUtf8: {string: "对讲机+"}}, {bytes.encodeUtf8: {string: "walkie talkie"}}]}
""")
        self.assertEqual(engine.action("This 对讲机 works better than that 对讲机."), "This walkie talkie works better than that 对讲机.")


    def testreplaceLast(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replacelast: [input, ["ab(c|d)*"], ["person"]]}
""")
        self.assertEqual(engine.action("abcccdcPPPPabcccdc"),     "abcccdcPPPPperson")
        self.assertEqual(engine.action("abcccdcPPPPabcccdcPPPP"), "abcccdcPPPPpersonPPPP")

# check non-ascii input
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: string
#action:
#  - {re.replacelast: [input, [对讲机+], ["walkie talkie"]]}
#""")
#        self.assertEqual(engine.action("This 对讲机 works better than that 对讲机."), "This 对讲机 works better than that walkie talkie.")

# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: bytes
action:
  - {re.replacelast: [input, {bytes.encodeUtf8: {string: "对讲机+"}}, {bytes.encodeUtf8: {string: "walkie talkie"}}]}
""")
        self.assertEqual(engine.action("This 对讲机 works better than that 对讲机."), "This 对讲机 works better than that walkie talkie.")


    def testreplaceAll(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replaceall: [input, [cow], [doggy]]}
""")
        self.assertEqual(engine.action("pcowppcowpppcow"), "pdoggyppdoggypppdoggy")
        self.assertEqual(engine.action("cowpcowppcowppp"), "doggypdoggyppdoggyppp")

        engine, = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replaceall: [input, [cow], [Y]]}
""")
        self.assertEqual(engine.action("cowpcowppcowppp"), "YpYppYppp")
        self.assertEqual(engine.action("pcowppcowpppcow"), "pYppYpppY")

        engine, = PFAEngine.fromYaml("""
input: string
output: string
action:
  - {re.replaceall: [input, [ab(c|d)*], [cow]]}
""")
        self.assertEqual(engine.action("abcccdcPPPP"), "cowPPPP")
        self.assertEqual(engine.action("PPPPabcccdc"), "PPPPcow")
        self.assertEqual(engine.action("PPabcdddcPPabcccdcPPabcccdcPP"), "PPcowPPcowPPcowPP")

# check non-ascii input
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: string
#action:
#  - {re.replaceall: [input, [对讲机+], ["walkie talkie"]]}
#""")
#        self.assertEqual(engine.action("This 对讲机机 works better than that 对讲机机."), "This walkie talkie works better than that walkie talkie.")

# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: bytes
action:
  - {re.replaceall: [input, {bytes.encodeUtf8: {string: "对讲机+"}}, {bytes.encodeUtf8: {string: "walkie talkie"}}]}
""")
        self.assertEqual(engine.action("This 对讲机 works better than that 对讲机."), "This walkie talkie works better than that walkie talkie.")


    def testsplit(self):
        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.split: [input, [cow]]}
""")
        self.assertEqual(engine.action("cowpcowppcowppp"), ["p","pp","ppp"])
        self.assertEqual(engine.action("pcowppcowpppcow"), ["p","pp","ppp"])

        engine, = PFAEngine.fromYaml("""
input: string
output: {type: array, items: string}
action:
  - {re.split: [input, [ab(c|d)*]]}
""")
        self.assertEqual(engine.action("abcccdcPPPP"), ["PPPP"])
        self.assertEqual(engine.action("PPPPabcccdc"), ["PPPP"])
        self.assertEqual(engine.action("PPabcccdcPPabcccdcPPabcccdcPP"), ["PP","PP","PP","PP"])

# check non-ascii string input
#        engine, = PFAEngine.fromYaml("""
#input: string
#output: {type: array, items: string}
#action:
#  - {re.split: [input, [机+]]}
#""")
#        self.assertEqual(engine.action("abc机机机abca机机机bc  asdkj 机机sd"), ["abc","abca","bc  asdkj ", "sd" ])

# check byte input
        engine, = PFAEngine.fromYaml("""
input: bytes
output: {type: array, items: bytes}
action:
  - re.split: [input, {bytes.encodeUtf8: {string: "机机+"}}]
""")
        self.assertEqual(engine.action("xyz"), ["xyz"])
        self.assertEqual(engine.action("ab机机ab机机abc机机abc"), ["ab","ab", "abc", "abc"])













