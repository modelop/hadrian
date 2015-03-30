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

import math
import random
from collections import OrderedDict

import numpy

from titus.producer.transformation import Transformation
import titus.prettypfa

### metrics are nested objects like Euclidean(AbsDiff()), and they may
### be user-defined.  They mirror the metrics and similarity functions
### available in PFA.

### interfaces

def _NotImplementedError():
    raise NotImplementedError
class Similarity(object):
    def __init__(self):
        self.calculate = lambda dataset, cluster: _NotImplementedError()
    def pfa(self):
        raise NotImplementedError
class Metric(object):
    def __init__(self):
        self.calculate = lambda dataset, cluster: _NotImplementedError()
    def pfa(self):
        raise NotImplementedError

### similarity

class AbsDiff(Similarity):
    def __init__(self):
        self.calculate = lambda dataset, cluster: numpy.absolute(dataset - cluster)
    def pfa(self):
        return {"fcn": "metric.absDiff"}

class GaussianSimilarity(Similarity):
    def __init__(self, sigma):
        self.calculate = lambda dataset, cluster: numpy.exp(-numpy.log(2) * numpy.square(dataset - cluster) / sigma**2)
        self.sigma = sigma
    def pfa(self):
        x = "similarityX"
        y = "similarityY"
        return {"params": [{x: "double"}, {y: "double"}],
                "ret": "double",
                "do": {"metric.gaussianSimilarity": [x, y, self.sigma]}}

### metrics

class Euclidean(Metric):
    def __init__(self, similarity):
        self.calculate = lambda dataset, cluster: numpy.sqrt(numpy.sum(numpy.square(similarity.calculate(dataset, cluster)), axis=1))
        self.similarity = similarity
    def pfa(self, x, y):
        return {"metric.euclidean": [self.similarity.pfa(), x, y]}

class SquaredEuclidean(Metric):
    def __init__(self, similarity):
        self.calculate = lambda dataset, cluster: numpy.sum(numpy.square(similarity.calculate(dataset, cluster)), axis=1)
        self.similarity = similarity
    def pfa(self, x, y):
        x = "metricX"
        y = "metricY"
        return {"metric.squaredEuclidean": [self.similarity.pfa(), x, y]}

class Chebyshev(Metric):
    def __init__(self, similarity):
        self.calculate = lambda dataset, cluster: numpy.max(similarity.calculate(dataset, cluster), axis=1)
        self.similarity = similarity
    def pfa(self, x, y):
        x = "metricX"
        y = "metricY"
        return {"metric.chebyshev": [self.similarity.pfa(), x, y]}

class Taxicab(Metric):
    def __init__(self, similarity):
        self.calculate = lambda dataset, cluster: numpy.sum(similarity.calculate(dataset, cluster), axis=1)
        self.similarity = similarity
    def pfa(self, x, y):
        x = "metricX"
        y = "metricY"
        return {"metric.taxicab": [self.similarity.pfa(), x, y]}

class Minkowski(Metric):
    def __init__(self, similarity, p):
        self.calculate = lambda dataset, cluster: numpy.pow(numpy.sum(numpy.pow(similarity.calculate(dataset, cluster), p), axis=1), 1.0/p)
        self.similarity = similarity
        self.p = p
    def pfa(self, x, y):
        x = "metricX"
        y = "metricY"
        return {"metric.minkowski": [self.similarity.pfa(), x, y, self.p]}

### stopping conditions are functions that take iterationNumber (int),
# corrections (Python list of Numpy arrays), datasetSize (int) and
# return bool (continue iterating if True)

### they may be user-defined or constructed from these functions like
# whileall(printChange("6.4f"), halfChange(0.001), clusterJumped()) to
# print iteration data, stop when at least half change by less than
# 0.001, and keep going if one jumped

def printValue(format="g"):
    state = {"ready": False}
    def out(iterationNumber, corrections, values, datasetSize):
        if not state["ready"]:
            state["j"] = "{:5s} (jump)"
            for v in values:
                if v is not None:
                    state["n"] = "{0:5s}" + "".join((" {%d:%s}" % (i + 1, format)) for i in xrange(len(v)))
                    break
            if "n" in state:
                state["ready"] = True
            print "iter  values"
            print "----------------------------------"
        for index, v in enumerate(values):
            if index == 0:
                it = repr(iterationNumber)
            else:
                it = ""
            if v is None:
                print state["j"].format(it)
            else:
                print state["n"].format(it, *v)
        return True
    return out

def printChange(format="g"):
    state = {"ready": False}
    def out(iterationNumber, corrections, values, datasetSize):
        if not state["ready"]:
            state["j"] = "{:5s} (jump)"
            for corr in corrections:
                if corr is not None:
                    state["n"] = "{0:5s}" + "".join((" {%d:%s}" % (i + 1, format)) for i in xrange(len(corr)))
                    break
            if "n" in state:
                state["ready"] = True
            print "iter  changes"
            print "----------------------------------"
        for index, corr in enumerate(corrections):
            if index == 0:
                it = repr(iterationNumber)
            else:
                it = ""
            if corr is None:
                print state["j"].format(it)
            else:
                print state["n"].format(it, *corr)
        return True
    return out

def clusterJumped():
    return lambda iterationNumber, corrections, values, datasetSize: all(x is not None for x in corrections)

def maxIterations(number):
    return lambda iterationNumber, corrections, values, datasetSize: iterationNumber < number

def allChange(threshold):
    return lambda iterationNumber, corrections, values, datasetSize: not all((numpy.absolute(x) < threshold).all() for x in corrections if x is not None)

def halfChange(threshold):
    return lambda iterationNumber, corrections, values, datasetSize: numpy.sum([(numpy.absolute(x) < threshold).all() for x in corrections if x is not None], dtype=numpy.dtype(float)) / numpy.sum([x is not None for x in corrections], dtype=numpy.dtype(float)) < 0.5

def whileall(*conditions):
    return lambda *args: all(x(*args) for x in conditions)

def whileany(*conditions):
    return lambda *args: any(x(*args) for x in conditions)

def moving():
    return whileall(clusterJumped(), allChange(1e-15))

### the KMeans class

class KMeans(object):
    """Represents a k-means optimization by storing a dataset and
    performing all operations *in-place*.

    Usually, you would construct the object, possibly stepup, then
    optimize and export to pfaDocument."""

    def __init__(self, numberOfClusters, dataset, weights=None, metric=Euclidean(AbsDiff()), minPointsInCluster=None, maxPointsForClustering=None):
        """Construct a KMeans object, initializing cluster centers to
        unique, random points from the dataset.

        numberOfClusters is an integer number of clusters (the "k" in
        k-means).

        dataset is a two-dimensional Numpy array: dataset.shape[0] is
        the number of records (rows), dataset.shape[1] is the number
        of dimensions for each point (columns).

        weights may be None or an array of shape (dataset.shape[0],).
        Used to raise or lower significance of individual points (0
        means ignore datapoint, 1 means take it as usual).

        metric is a Metric object, such as Euclidean(AbsDiff()).

        minPointsInCluster is None or a minimum number of points
        before jumping (choosing a random point for a cluster during
        optimization).

        maxPointsForClustering is None or a maximum number of points
        in an optimization (if dataset.shape[0] exceeds this amount, a
        random subset is chosen)."""

        if len(dataset.shape) != 2:
            raise TypeError("dataset must be two-dimensional: dataset.shape[0] is the number of records (rows), dataset.shape[1] is the number of dimensions (columns)")

        self.dataset = dataset
        if weights is not None and weights.shape != (dataset.shape[0],):
            raise TypeError("weights must have as many records as the dataset and must be one dimensional")
        self.weights = weights
        self.metric = metric

        try:
            flattenedView = numpy.ascontiguousarray(self.dataset).view(numpy.dtype((numpy.void, self.dataset.dtype.itemsize * self.dataset.shape[1])))
            _, indexes = numpy.unique(flattenedView, return_index=True)
            self.uniques = self.dataset[indexes]
        except TypeError:
            self.uniques = self.dataset

        if self.uniques.shape[0] <= numberOfClusters:
            raise TypeError("the number of unique records in the dataset ({0} in this case) must be strictly greater than numberOfClusters ({1})".format(self.uniques.shape[0], numberOfClusters))
        self.numberOfClusters = numberOfClusters

        self.clusters = []
        for index in xrange(numberOfClusters):
            self.clusters.append(self.newCluster())

        self.minPointsInCluster = minPointsInCluster
        self.maxPointsForClustering = maxPointsForClustering

    def randomPoint(self):
        """Pick a random point from the dataset."""
        # make sure to copy it, so there are no hidden connections between dataset and clusters
        return self.uniques[random.randint(0, self.uniques.shape[0] - 1),:].copy()

    def newCluster(self):
        """Pick a random point from the dataset and ensure that it is different from all other cluster centers."""
        newCluster = self.randomPoint()
        while any(numpy.array_equal(x, newCluster) for x in self.clusters):
            newCluster = self.randomPoint()
        return newCluster

    def randomSubset(self, subsetSize):
        """Return a (dataset, weights) that are randomly chosen to
        have subsetSize records."""

        if subsetSize <= self.numberOfClusters:
            raise TypeError("subsetSize must be strictly greater than the numberOfClusters")

        indexes = random.sample(xrange(self.dataset.shape[0]), subsetSize)
        dataset = self.dataset[indexes,:]
        if self.weights is None:
            weights = None
        else:
            weights = self.weights[indexes]

        return dataset, weights

    def closestCluster(self, dataset=None, weights=None):
        """Identify the closest cluster to each element in the
        dataset.

        Return value is an array of integers corresponding to the
        indexes of the closest cluster for each datum."""

        if dataset is None:
            dataset = self.dataset
        if weights is None:
            weights = self.weights

        # distanceToCenter is the result of applying the metric to each point in the dataset, for each cluster
        # distanceToCenter.shape[0] is the number of records, distanceToCenter.shape[1] is the number of clusters

        distanceToCenter = numpy.empty((dataset.shape[0], self.numberOfClusters), dtype=numpy.dtype(float))
        for clusterIndex, cluster in enumerate(self.clusters):
            distanceToCenter[:, clusterIndex] = self.metric.calculate(dataset, cluster)

        # indexOfClosestCluster is the cluster classification for each point in the dataset
        return numpy.argmin(distanceToCenter, axis=1)

    def iterate(self, dataset, weights, iterationNumber, condition):
        """Perform one iteration step (in-place; modifies
        self.clusters).

        The dataset and weights are passed in so that this may be used
        with subsets.

        The return value is the result of condition(iterationNumber,
        corrections, values, dataset.shape[0])."""

        indexOfClosestCluster = self.closestCluster(dataset, weights)

        values = []
        corrections = []
        for clusterIndex, cluster in enumerate(self.clusters):
            # select by cluster classification
            selection = (indexOfClosestCluster == clusterIndex)
            residuals = (dataset - cluster)[selection]

            # weights scale the residuals
            if weights is not None:
                residuals *= weights[selection]

            if self.minPointsInCluster is not None and numpy.sum(selection) < self.minPointsInCluster:
                # too few points in this cluster; jump to a new random point
                self.clusters[clusterIndex] = self.newCluster()
                values.append(None)
                corrections.append(None)

            else:
                # compute the mean of the displacements of points associated with this cluster
                # (note that the similarity metric used here is the trivial one, possibly different from the classification metric)
                correction = residuals.mean(axis=0)
                numpy.add(cluster, correction, cluster)

                if not numpy.isfinite(cluster).all():
                    # mean of a component is NaN or Inf, possibly because of zero points in the cluster
                    self.clusters[clusterIndex] = self.newCluster()
                    values.append(None)
                    corrections.append(None)
                else:
                    # good step: correction is not None
                    values.append(cluster)
                    corrections.append(correction)

        # call user-supplied test for continuation
        return condition(iterationNumber, corrections, values, dataset.shape[0])

    def stepup(self, condition, base=2):
        """Optimize the cluster set in successively larger subsets of
        the dataset.  (This can be viewed as a cluster seeding
        technique.)

        If randomly seeded, optimizing the whole dataset can be slow
        to converge: a long time per iteration times many iterations.

        Optimizing a random subset takes as many iterations, but the
        time per iteration is short.  However, the final cluster
        centers are only approximate.

        Optimizing the whole dataset with approximate cluster starting
        points takes a long time per iteration but fewer iterations.

        This procedure runs the k-means optimization technique on
        random subsets with exponentially increasing sizes from the
        smallest base**x that is larger than minPointsInCluster (or
        numberOfClusters) to the largest base**x that is a subset of
        the whole dataset."""

        if self.minPointsInCluster is None:
            minPointsInCluster = self.numberOfClusters
        else:
            minPointsInCluster = max(self.numberOfClusters, self.minPointsInCluster)

        if self.maxPointsForClustering is None:
            maxPointsForClustering = self.dataset.shape[0]
        else:
            maxPointsForClustering = self.maxPointsForClustering

        trialSizes = [base**x for x in xrange(int(math.log(maxPointsForClustering, base)) + 1) if base**x > minPointsInCluster]

        for trialSize in trialSizes:
            dataset, weights = self.randomSubset(trialSize)
            
            iterationNumber = 0
            while self.iterate(dataset, weights, iterationNumber, condition):
                iterationNumber += 1

    def optimize(self, condition):
        """Run a standard k-means (Lloyd's algorithm) on the dataset,
        changing the clusters *in-place*."""

        if self.maxPointsForClustering is None:
            dataset, weights = self.dataset, self.weights
        else:
            dataset, weights = self.randomSubset(self.maxPointsForClustering)

        iterationNumber = 0
        while self.iterate(dataset, weights, iterationNumber, condition):
            iterationNumber += 1

    def centers(self, sort=True):
        """Get the cluster centers as a sorted Python list (canonical form)."""
        if sort:
            centers = sorted(map(list, self.clusters))
        else:
            centers = map(list, self.clusters)
        return centers

    def pfaType(self, clusterTypeName, idType="string", centerComponentType="double"):
        """Create a PFA type schema representing this cluster set."""

        return {"type": "array",
                "items": {"type": "record",
                          "name": clusterTypeName,
                          "fields": [{"name": "center", "type": {"type": "array", "items": centerComponentType}},
                                     {"name": "id", "type": idType}]}}
            
    def pfaValue(self, ids, populations=False, sort=True):
        """Create a PFA data structure representing this cluster set."""

        if len(ids) != self.numberOfClusters:
            raise TypeError("ids should be a list with length equal to the number of clusters")

        out = [{"center": x} for x in self.centers(sort=False)]

        if populations:
            indexOfClosestCluster = self.closestCluster(self.dataset, self.weights)
            for clusterIndex in xrange(len(self.clusters)):
                out[clusterIndex]["population"] = int(numpy.sum(indexOfClosestCluster == clusterIndex))

        if sort:
            indexes = [i for i, x in sorted(list(enumerate(self.clusters)), lambda a, b: cmp(list(a[1]), list(b[1])))]
            out = list(numpy.array(out, dtype=object)[indexes])

        for idi, cluster in zip(ids, out):
            cluster["id"] = idi

        return out

    def pfaDocument(self, clusterTypeName, ids, populations=False, sort=True, preprocess=None, idType="string", dataComponentType="double", centerComponentType="double"):
        """Create a PFA document to score with this cluster set."""

        clusters = self.pfaValue(ids, populations, sort)
        metric = self.metric.pfa("datum", "clusterCenter")
        if preprocess is None:
            preprocess = "input"

        return titus.prettypfa.jsonNode('''
types:
  ClusterType = record(<<clusterTypeName>>,
    id: <<idType>>,
    center: array(<<centerComponentType>>))
input: array(<<dataComponentType>>)
output: <<idType>>
cells:
  clusters(array(ClusterType)) = <<clusters>>
action:
  model.cluster.closest(<<preprocess>>, clusters,
    fcn(datum: array(<<dataComponentType>>),
        clusterCenter: array(<<centerComponentType>>) -> double)
      <<metric>>)["id"]
''', **vars())
