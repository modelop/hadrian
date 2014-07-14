#!/usr/bin/env python

import time
import unittest

from titus.genpy import PFAEngine
from titus.errors import *
    
class TestSpeed(unittest.TestCase):
    def testTree(self):
        engine, = PFAEngine.fromJson(open("test/hipparcos_numerical_10.pfa"))

        data = []
        for line in open("test/hipparcos_numerical.csv"):
            ra, dec, dist, mag, absmag, x, y, z, vx, vy, vz, spectrum = line.split(",")
            data.append({"ra": float(ra), "dec": float(dec), "dist": float(dist), "mag": float(mag), "absmag": float(absmag), "x": float(x), "y": float(y), "z": float(z), "vx": float(vx), "vy": float(vy), "vz": float(vz)})

        i = 0
        startTime = time.time()
        for datum in data:
            engine.action(datum)
            i += 1
            if i % 5000 == 0:
                print "{}, {}".format(time.time() - startTime, i)

if __name__ == "__main__":
    unittest.main()
