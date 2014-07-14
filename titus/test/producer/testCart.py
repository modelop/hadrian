#!/usr/bin/env python

import random
import unittest

import numpy

from titus.genpy import PFAEngine
from titus.producer.tools import look
from titus.producer.cart import *

class TestProducerCart(unittest.TestCase):
    @staticmethod
    def data():
        while True:
            x = random.uniform(0, 10)
            y = random.uniform(0, 10)
            if x < 4.0:
                if y < 6.0:
                    z = random.gauss(5, 1)
                else:
                    z = random.gauss(8, 1)
            else:
                if y < 2.0:
                    z = random.gauss(1, 1)
                else:
                    z = random.gauss(2, 1)
            if z < 0.0:
                z = 0.0
            elif z >= 10.0:
                z = 9.99999

            a = "A" + str(int(x))
            b = "B" + str(int(y/2) * 2)
            c = "C" + str(int(z/3) * 3)

            yield (x, y, z, a, b, c)

    def testCartMustBuildNumericalNumerical(self):
        random.seed(12345)
        numpy.seterr(divide="ignore", invalid="ignore")
        dataset = Dataset.fromIterable(((x, y, z) for (x, y, z, a, b, c) in TestProducerCart.data()), 100000, ("x", "y", "z"))

        tree = TreeNode.fromWholeDataset(dataset, "z")
        tree.splitMaxDepth(2)

        doc = tree.pfaDocument({"type": "record", "name": "Datum", "fields": [{"name": "x", "type": "double"}, {"name": "y", "type": "double"}]}, "TreeNode")
        # look(doc, maxDepth=8)

        self.assertEqual(doc["cells"]["tree"]["init"]["field"], "x")
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["value"], 4.00, places=2)
        self.assertEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["field"], "y")
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["value"], 6.00, places=2)
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["pass"]["double"], 5.00, places=2)
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["fail"]["double"], 8.02, places=2)
        self.assertEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["field"], "y")
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["value"], 2.00, places=2)
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["pass"]["double"], 1.09, places=2)
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["fail"]["double"], 2.00, places=2)

        engine, = PFAEngine.fromJson(doc)
        self.assertAlmostEqual(engine.action({"x": 2.0, "y": 3.0}), 5.00, places=2)
        self.assertAlmostEqual(engine.action({"x": 2.0, "y": 8.0}), 8.02, places=2)
        self.assertAlmostEqual(engine.action({"x": 7.0, "y": 1.0}), 1.09, places=2)
        self.assertAlmostEqual(engine.action({"x": 7.0, "y": 5.0}), 2.00, places=2)

        doc = tree.pfaDocument(
            {"type": "record", "name": "Datum", "fields": [{"name": "x", "type": "double"}, {"name": "y", "type": "double"}]},
            "TreeNode",
            nodeScores=True, datasetSize=True, predictandUnique=True, nTimesVariance=True, gain=True)
        # look(doc, maxDepth=8)
        engine, = PFAEngine.fromJson(doc)

    def testCartMustBuildNumericalCategorical(self):
        random.seed(12345)
        numpy.seterr(divide="ignore", invalid="ignore")
        dataset = Dataset.fromIterable(((x, y, c) for (x, y, z, a, b, c) in TestProducerCart.data()), 100000, ("x", "y", "c"))

        tree = TreeNode.fromWholeDataset(dataset, "c")
        tree.splitMaxDepth(2)

        doc = tree.pfaDocument({"type": "record", "name": "Datum", "fields": [{"name": "x", "type": "double"}, {"name": "y", "type": "double"}]}, "TreeNode")
        # look(doc, maxDepth=8)

        self.assertEqual(doc["cells"]["tree"]["init"]["field"], "x")
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["value"], 4.00, places=2)
        self.assertEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["field"], "y")
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["value"], 6.00, places=2)
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["pass"]["string"], "C3", places=2)
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["fail"]["string"], "C6", places=2)
        self.assertEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["field"], "y")
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["value"], 2.00, places=2)
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["pass"]["string"], "C0", places=2)
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["fail"]["string"], "C0", places=2)

        engine, = PFAEngine.fromJson(doc)
        self.assertEqual(engine.action({"x": 2.0, "y": 3.0}), "C3")
        self.assertEqual(engine.action({"x": 2.0, "y": 8.0}), "C6")
        self.assertEqual(engine.action({"x": 7.0, "y": 1.0}), "C0")
        self.assertEqual(engine.action({"x": 7.0, "y": 5.0}), "C0")

        doc = tree.pfaDocument(
            {"type": "record", "name": "Datum", "fields": [{"name": "x", "type": "double"}, {"name": "y", "type": "double"}]},
            "TreeNode",
            nodeScores=True, datasetSize=True, predictandDistribution=True, predictandUnique=True, entropy=True, gain=True)
        # look(doc, maxDepth=8)
        engine, = PFAEngine.fromJson(doc)

    def testCartMustBuildCategoricalNumerical(self):
        random.seed(12345)
        numpy.seterr(divide="ignore", invalid="ignore")
        dataset = Dataset.fromIterable(((a, b, z) for (x, y, z, a, b, c) in TestProducerCart.data()), 100000, ("a", "b", "z"))

        tree = TreeNode.fromWholeDataset(dataset, "z")
        tree.splitMaxDepth(2)

        doc = tree.pfaDocument({"type": "record", "name": "Datum", "fields": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]}, "TreeNode")
        # look(doc, maxDepth=8)

        self.assertEqual(doc["cells"]["tree"]["init"]["field"], "a")
        self.assertEqual(doc["cells"]["tree"]["init"]["value"], ["A0", "A1", "A2", "A3"])
        self.assertEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["field"], "b")
        self.assertEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["value"], ["B6", "B8"])
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["pass"]["double"], 8.02, places=2)
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["fail"]["double"], 5.00, places=2)
        self.assertEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["field"], "b")
        self.assertEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["value"], ["B0"])
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["pass"]["double"], 1.09, places=2)
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["fail"]["double"], 2.00, places=2)

        engine, = PFAEngine.fromJson(doc)
        self.assertAlmostEqual(engine.action({"a": "A1", "b": "B6"}), 8.02, places=2)
        self.assertAlmostEqual(engine.action({"a": "A1", "b": "B2"}), 5.00, places=2)
        self.assertAlmostEqual(engine.action({"a": "A5", "b": "B0"}), 1.09, places=2)
        self.assertAlmostEqual(engine.action({"a": "A5", "b": "B4"}), 2.00, places=2)

        doc = tree.pfaDocument(
            {"type": "record", "name": "Datum", "fields": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]},
            "TreeNode",
            nodeScores=True, datasetSize=True, predictandUnique=True, nTimesVariance=True, gain=True)
        # look(doc, maxDepth=8)
        engine, = PFAEngine.fromJson(doc)

    def testCartMustBuildCategoricalCategorical(self):
        random.seed(12345)
        numpy.seterr(divide="ignore", invalid="ignore")
        dataset = Dataset.fromIterable(((a, b, c) for (x, y, z, a, b, c) in TestProducerCart.data()), 100000, ("a", "b", "c"))

        tree = TreeNode.fromWholeDataset(dataset, "c")
        tree.splitMaxDepth(2)

        doc = tree.pfaDocument({"type": "record", "name": "Datum", "fields": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]}, "TreeNode")
        # look(doc, maxDepth=8)

        self.assertEqual(doc["cells"]["tree"]["init"]["field"], "a")
        self.assertEqual(doc["cells"]["tree"]["init"]["value"], ["A0", "A1", "A2", "A3"])
        self.assertEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["field"], "b")
        self.assertEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["value"], ["B6", "B8"])
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["pass"]["string"], "C6", places=2)
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["pass"]["TreeNode"]["fail"]["string"], "C3", places=2)
        self.assertEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["field"], "b")
        self.assertEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["value"], ["B0"])
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["pass"]["string"], "C0", places=2)
        self.assertAlmostEqual(doc["cells"]["tree"]["init"]["fail"]["TreeNode"]["fail"]["string"], "C0", places=2)

        engine, = PFAEngine.fromJson(doc)
        self.assertAlmostEqual(engine.action({"a": "A1", "b": "B6"}), "C6", places=2)
        self.assertAlmostEqual(engine.action({"a": "A1", "b": "B2"}), "C3", places=2)
        self.assertAlmostEqual(engine.action({"a": "A5", "b": "B0"}), "C0", places=2)
        self.assertAlmostEqual(engine.action({"a": "A5", "b": "B4"}), "C0", places=2)

        doc = tree.pfaDocument(
            {"type": "record", "name": "Datum", "fields": [{"name": "a", "type": "string"}, {"name": "b", "type": "string"}]},
            "TreeNode",
            nodeScores=True, datasetSize=True, predictandDistribution=True, predictandUnique=True, entropy=True, gain=True)
        # look(doc, maxDepth=8)
        engine, = PFAEngine.fromJson(doc)

if __name__ == "__main__":
    unittest.main()
