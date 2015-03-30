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
import re
import unittest

import numpy
from avro.datafile import DataFileReader
from avro.io import DatumReader

import titus.prettypfa
from titus.genpy import PFAEngine
from titus.producer.kmeans import *

class TestClustering(unittest.TestCase):
    # most examples use the same clusters; only compute them the first time
    clusterNames = ["cluster{0:d}".format(x) for x in range(5)]
    kmeansResult = None

    def doKmeans(self):
        numpy.seterr(divide="ignore", invalid="ignore")

        # get a dataset for the k-means generator
        dataset = []
        for record in DataFileReader(open("test/prettypfa/exoplanets.avro", "r"), DatumReader()):
            mag, dist, mass, radius = record.get("mag"), record.get("dist"), record.get("mass"), record.get("radius")
            if mag is not None and dist is not None and mass is not None and radius is not None:
                dataset.append([mag, dist, mass, radius])

        # set up and run the k-means generator
        TestClustering.kmeansResult = KMeans(len(self.clusterNames), numpy.array(dataset))
        TestClustering.kmeansResult.optimize(whileall(moving(), maxIterations(1000)))

    def runEngine(self, engine):
        if engine.config.method == "emit":
            engine.emit = lambda x: x

        for record in DataFileReader(open("test/prettypfa/exoplanets.avro", "r"), DatumReader()):
            engine.action(record)

    #################################################################################################################
    def testSimpleKMeansWithStrings(self):
        # define the workflow, leaving clusters as an empty array for now
        pfaDocument = titus.prettypfa.jsonNode('''
input: <<INPUT>>
output: string

cells:
    clusters(array(record(id: string, center: array(double)))) = []

action:
    // ifnotnull runs the first block if all four expressions are not null
    // input.mag has type union(double, null) while mag has type double, etc.

    ifnotnull(mag: input.mag,
              dist: input.dist,
              mass: input.mass,
              radius: input.radius)
        model.cluster.closest(new(array(double), mag, dist, mass, radius),
                              clusters,
                              metric.simpleEuclidean)["id"]
    else
        "MISSING"

'''.replace("<<INPUT>>", open("test/prettypfa/exoplanetsSchema.ppfa").read()))

        # fill in the clusters with the k-means result
        if self.kmeansResult is None: self.doKmeans()
        pfaDocument["cells"]["clusters"]["init"] = self.kmeansResult.pfaValue(self.clusterNames)

        # build a scoring engine and test it
        engine, = PFAEngine.fromJson(pfaDocument)
        self.runEngine(engine)

    #################################################################################################################
    def testSimpleKMeansWithEnums(self):
        # same as the above using enums rather than strings and compacted a bit
        pfaDocument = titus.prettypfa.jsonNode('''
input: <<INPUT>>
output: enum([cluster0, cluster1, cluster2, cluster3, cluster4, MISSING], ClusterId)

cells:
    clusters(array(record(id: ClusterId, center: array(double)))) = []

action:
    ifnotnull(mag: input.mag, dist: input.dist, mass: input.mass, radius: input.radius)
        model.cluster.closest(new(array(double), mag, dist, mass, radius),
                              clusters,
                              metric.simpleEuclidean)["id"]
    else
        ClusterId@MISSING

'''.replace("<<INPUT>>", open("test/prettypfa/exoplanetsSchema.ppfa").read()))

        if self.kmeansResult is None: self.doKmeans()
        pfaDocument["cells"]["clusters"]["init"] = self.kmeansResult.pfaValue(self.clusterNames)
        engine, = PFAEngine.fromJson(pfaDocument)
        self.runEngine(engine)

    #################################################################################################################
    def testSimpleKMeansEmitExample(self):
        # the emit method allows us to ignore the "else" clause in ifnotnull
        pfaDocument = titus.prettypfa.jsonNode('''
input: <<INPUT>>
output: string
method: emit

cells:
    clusters(array(record(id: string, center: array(double)))) = []

action:
    ifnotnull(mag: input.mag, dist: input.dist, mass: input.mass, radius: input.radius)
        emit(model.cluster.closest(new(array(double), mag, dist, mass, radius),
                                   clusters,
                                   metric.simpleEuclidean)["id"])

'''.replace("<<INPUT>>", open("test/prettypfa/exoplanetsSchema.ppfa").read()))

        if self.kmeansResult is None: self.doKmeans()
        pfaDocument["cells"]["clusters"]["init"] = self.kmeansResult.pfaValue(self.clusterNames)
        engine, = PFAEngine.fromJson(pfaDocument)
        self.runEngine(engine)

    #################################################################################################################
    def testDistanceToClosestCluster(self):
        # now that the ifnotnull clause has become three lines long, notice that it needs to be
        # surrounded by curly brackets and expressions must be separated by semicolons
        # (the last semicolon is optional: they're delimiters, not line terminators)
        pfaDocument = titus.prettypfa.jsonNode('''
input: <<INPUT>>
output: union(double, null)
cells:
    clusters(array(record(id: string, center: array(double)))) = []

action:
    ifnotnull(mag: input.mag, dist: input.dist, mass: input.mass, radius: input.radius) {
        var datum = new(array(double), mag, dist, mass, radius);
        var closestCluster = model.cluster.closest(datum, clusters, metric.simpleEuclidean);
        metric.simpleEuclidean(datum, closestCluster["center"])
    }
    else
        null
'''.replace("<<INPUT>>", open("test/prettypfa/exoplanetsSchema.ppfa").read()))

        if self.kmeansResult is None: self.doKmeans()
        pfaDocument["cells"]["clusters"]["init"] = self.kmeansResult.pfaValue(self.clusterNames)
        engine, = PFAEngine.fromJson(pfaDocument)
        self.runEngine(engine)

    #################################################################################################################
    def testPopulationOfClosestCluster(self):
        # now that the ifnotnull clause has become three lines long, notice that it needs to be
        # surrounded by curly brackets and expressions must be separated by semicolons
        # (the last semicolon is optional: they're delimiters, not line terminators)
        pfaDocument = titus.prettypfa.jsonNode('''
input: <<INPUT>>
output: union(int, null)
cells:
    clusters(array(record(id: string, center: array(double), population: int))) = []

action:
    ifnotnull(mag: input.mag, dist: input.dist, mass: input.mass, radius: input.radius)
        model.cluster.closest(new(array(double), mag, dist, mass, radius),
                              clusters,
                              metric.simpleEuclidean)["population"]
    else
        null
'''.replace("<<INPUT>>", open("test/prettypfa/exoplanetsSchema.ppfa").read()))

        if self.kmeansResult is None: self.doKmeans()
        pfaDocument["cells"]["clusters"]["init"] = self.kmeansResult.pfaValue(self.clusterNames, populations=True)
        engine, = PFAEngine.fromJson(pfaDocument)
        self.runEngine(engine)

    #################################################################################################################
    def testNormalized(self):
        # for k-means on normalized data, we have to explicitly normalize,
        # re-compute the clusters, and put the same transformation into PFA

        # get a dataset for the k-means generator
        dataset = []
        for record in DataFileReader(open("test/prettypfa/exoplanets.avro", "r"), DatumReader()):
            mag, dist, mass, radius = record.get("mag"), record.get("dist"), record.get("mass"), record.get("radius")
            if mag is not None and dist is not None and mass is not None and radius is not None:
                dataset.append([mag, dist, mass, radius])
        dataset = numpy.array(dataset)

        # compute the normalization (1st to 99th percentile instead of strict min/max)
        maglow,    maghigh    = numpy.percentile(dataset[:,0], [1, 99])
        distlow,   disthigh   = numpy.percentile(dataset[:,1], [1, 99])
        masslow,   masshigh   = numpy.percentile(dataset[:,2], [1, 99])
        radiuslow, radiushigh = numpy.percentile(dataset[:,3], [1, 99])

        # transform the data
        normalized = numpy.empty_like(dataset)
        normalized[:,0] = (dataset[:,0] - maglow) / (maghigh - maglow)
        normalized[:,1] = (dataset[:,1] - distlow) / (disthigh - distlow)
        normalized[:,2] = (dataset[:,2] - masslow) / (masshigh - masslow)
        normalized[:,3] = (dataset[:,3] - radiuslow) / (radiushigh - radiuslow)

        # set up and run the k-means generator
        kmeansResult = KMeans(len(self.clusterNames), normalized)
        kmeansResult.optimize(whileall(moving(), maxIterations(1000)))

        # put the transformation into PFA by string replacement
        # this re.subs will replace French quotes (<< >>) with Python variable values
        inputSchema = open("test/prettypfa/exoplanetsSchema.ppfa").read()
        namesToSubstitute = locals()
        pfaDocument = titus.prettypfa.jsonNode(
            re.sub("<<[A-Za-z0-9]+>>",
                   lambda x: str(namesToSubstitute[x.group().lstrip("<<").rstrip(">>")]),
                   '''
input: <<inputSchema>>
output: string
cells:
    clusters(array(record(id: string, center: array(double)))) = []

action:
    ifnotnull(mag: input.mag, dist: input.dist, mass: input.mass, radius: input.radius) {
        var normmag = (mag - <<maglow>>) / (<<maghigh>> - <<maglow>>);
        var normdist = (dist - <<distlow>>) / (<<disthigh>> - <<distlow>>);
        var normmass = (mass - <<masslow>>) / (<<masshigh>> - <<masslow>>);
        var normradius = (radius - <<radiuslow>>) / (<<radiushigh>> - <<radiuslow>>);

        model.cluster.closest(new(array(double), normmag, normdist, normmass, normradius),
                              clusters,
                              metric.simpleEuclidean)["id"]
    }
    else
        "MISSING"
'''))

        # now put the clusters in and run the scoring engine
        pfaDocument["cells"]["clusters"]["init"] = kmeansResult.pfaValue(self.clusterNames)
        engine, = PFAEngine.fromJson(pfaDocument)
        self.runEngine(engine)

if __name__ == "__main__":
    unittest.main()
