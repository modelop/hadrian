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
import re
import unittest

import numpy
from avro.datafile import DataFileReader
from avro.io import DatumReader

import titus.prettypfa
from titus.genpy import PFAEngine
from titus.producer.kmeans import *

class TestClustering(unittest.TestCase):
    def runEngine(self, engine):
        last = [None]

        if engine.config.method == "emit":
            def emit(x):
                last[0] = x
            engine.emit = emit

            for record in DataFileReader(open("test/prettypfa/exoplanets.avro", "r"), DatumReader()):
                engine.action(record)

        else:
            for record in DataFileReader(open("test/prettypfa/exoplanets.avro", "r"), DatumReader()):
                last[0] = engine.action(record)

        return last[0]

    def testTop5List(self):
        pfaDocument = titus.prettypfa.jsonNode('''
input: <<INPUT>>
output: array(string)

cells:
  mostPlanets(array(Star)) = []

action:
  // update the list of stars, keeping only the 5 with the most planets
  var currentList =
    mostPlanets to fcn(old: array(Star) -> array(Star))
        stat.sample.topN(input, old, 5, u.morePlanets);

  // map this top 5 list of stars to their names
  a.map(currentList, fcn(x: Star -> string) x.name)

fcns:
  // our comparison function
  morePlanets = fcn(x: Star, y: Star -> boolean) a.len(x.planets) < a.len(y.planets)

'''.replace("<<INPUT>>", open("test/prettypfa/exoplanetsSchema.ppfa").read()), check=False, lineNumbers=False)

        engine, = PFAEngine.fromJson(pfaDocument)
        self.assertEqual(self.runEngine(engine), ["KOI-351", "HD 40307", "GJ 667C", "Kepler-11", "HD 10180"])

    def testHistogram2d(self):
        pfaDocument = titus.prettypfa.jsonNode('''
input: <<INPUT>>
output: Histogram

cells:
  histogram(record(Histogram,
                   xnumbins: int,
                   xlow: double,
                   xhigh: double,
                   ynumbins: int,
                   ylow: double,
                   yhigh: double,
                   values: array(array(double)))) = {
      xnumbins: 10, xlow: 0.0, xhigh: 3.0,
      ynumbins: 10, ylow: 0.0, yhigh: 3.0,
      values: [[0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
               [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
               [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
               [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
               [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
               [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
               [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
               [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
               [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0],
               [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]]}

method: emit      
action:
  ifnotnull(mass: input.mass, radius: input.radius)
      emit(histogram to fcn(old: Histogram -> Histogram)
          stat.sample.fillHistogram2d(mass, radius, 1.0, old))

'''.replace("<<INPUT>>", open("test/prettypfa/exoplanetsSchema.ppfa").read()), check=False, lineNumbers=False)

        engine, = PFAEngine.fromJson(pfaDocument)
        self.assertEqual(self.runEngine(engine), {"values": [[6.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [3.0, 33.0, 3.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 8.0, 118.0, 28.0, 6.0, 0.0, 1.0, 1.0, 0.0, 0.0], [0.0, 0.0, 33.0, 184.0, 72.0, 25.0, 8.0, 4.0, 0.0, 1.0], [0.0, 0.0, 1.0, 12.0, 45.0, 34.0, 20.0, 3.0, 4.0, 1.0], [0.0, 0.0, 0.0, 1.0, 1.0, 4.0, 4.0, 4.0, 0.0, 0.0], [0.0, 0.0, 0.0, 1.0, 0.0, 1.0, 1.0, 1.0, 1.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 2.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0], [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]], "xhigh": 3.0, "yhigh": 3.0, "ynumbins": 10, "xnumbins": 10, "ylow": 0.0, "xlow": 0.0})

if __name__ == "__main__":
    unittest.main()
