#!/usr/bin/env python

import random
import unittest

import numpy

from titus.genpy import PFAEngine
from titus.producer.tools import look
from titus.producer.kmeans import *

class TestProducerKMeans(unittest.TestCase):
    @staticmethod
    def data(*centers):
        while True:
            center, = random.sample(centers, 1)
            x = random.gauss(center[0], 1)
            y = random.gauss(center[1], 1)
            z = random.gauss(center[2], 1)
            yield (x, y, z)

    def assertArrayAlmostEqual(self, xarray, yarray, *args, **kwds):
        for x, y in zip(xarray, yarray):
            self.assertAlmostEqual(x, y, *args, **kwds)

    def testKMeans(self):
        random.seed(12345)
        numpy.seterr(divide="ignore", invalid="ignore")

        dataset = numpy.empty((100000, 3), dtype=numpy.dtype(float))
        for i, x in enumerate(TestProducerKMeans.data([1, 1, 1], [3, 2, 5], [8, 2, 7], [5, 8, 5], [1, 1, 9])):
            if i >= dataset.shape[0]:
                break
            dataset[i,:] = x

        kmeans = KMeans(5, dataset)
        kmeans.optimize(whileall(moving(), maxIterations(1000)))

        centers = kmeans.centers()
        self.assertArrayAlmostEqual(centers[0], [1.00, 1.01, 1.00], places=2)
        self.assertArrayAlmostEqual(centers[1], [1.01, 1.00, 9.01], places=2)
        self.assertArrayAlmostEqual(centers[2], [3.01, 2.01, 5.00], places=2)
        self.assertArrayAlmostEqual(centers[3], [4.99, 8.00, 4.99], places=2)
        self.assertArrayAlmostEqual(centers[4], [8.02, 2.00, 7.01], places=2)

        doc = kmeans.pfaDocument("Cluster", ["one", "two", "three", "four", "five"])
        # look(doc, maxDepth=8)

        self.assertArrayAlmostEqual(doc["cells"]["clusters"]["init"][0]["center"], [1.00, 1.01, 1.00], places=2)
        self.assertArrayAlmostEqual(doc["cells"]["clusters"]["init"][1]["center"], [1.01, 1.00, 9.01], places=2)
        self.assertArrayAlmostEqual(doc["cells"]["clusters"]["init"][2]["center"], [3.01, 2.01, 5.00], places=2)
        self.assertArrayAlmostEqual(doc["cells"]["clusters"]["init"][3]["center"], [4.99, 8.00, 4.99], places=2)
        self.assertArrayAlmostEqual(doc["cells"]["clusters"]["init"][4]["center"], [8.02, 2.00, 7.01], places=2)

        engine, = PFAEngine.fromJson(doc)

        self.assertEqual(engine.action([1.00, 1.01, 1.00]), "one")
        self.assertEqual(engine.action([1.01, 1.00, 9.01]), "two")
        self.assertEqual(engine.action([3.01, 2.01, 5.00]), "three")
        self.assertEqual(engine.action([4.99, 8.00, 4.99]), "four")
        self.assertEqual(engine.action([8.02, 2.00, 7.01]), "five")

if __name__ == "__main__":
    unittest.main()
