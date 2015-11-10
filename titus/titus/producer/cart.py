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

import itertools
import numbers
import math
import json
from collections import OrderedDict

import numpy

from titus.signature import LabelData
from titus.datatype import ForwardDeclarationParser
from titus.datatype import AvroUnion
import titus.prettypfa

class Dataset(object):
    """Canonical format for providing a dataset to the tree-builder.

    Constructors are __init__ and fromIterable.
    """

    class Field(object):
        """Represents a field of the dataset; usually created by a Dataset constructor.

        ``Dataset.Field`` objects may be in one of two states: Numpy and
        Python.  The Dataset constructors produce ``Dataset.Fields`` in
        their Numpy representation, which is required by the
        tree-builder.  In the Numpy representation, categorical string
        data are represented as integers from 0 to N-1, where N is the
        number of unique input strings, with each distinct integer
        representing a distinct input string.  Strings and integers
        can be converted through the ``intToStr`` and ``strToInt``
        dictionaries, or by converting the whole array into a Pythonic
        form with the ``toPython`` method.
        """

        def __init__(self, t):
            self.tpe = t
            self.data = []

        def __repr__(self):
            return "<Dataset.Field of type {0} at 0x{:08x}>".format("float" if self.tpe == numbers.Real else "str", id(self))

        def add(self, v):
            self.data.append(v)

        def toNumpy(self):
            """Changes this field into a Numpy representation *in-place* (destructively replaces the old representation)."""

            if self.tpe == numbers.Real:
                self.data = numpy.array(self.data, dtype=numpy.dtype(float))
            elif self.tpe == basestring:
                unique = sorted(set(self.data))
                intToStr = dict(enumerate(unique))
                strToInt = dict((x, i) for i, x in enumerate(unique))
                converted = numpy.empty(len(self.data), dtype=numpy.dtype(int))
                for i, x in enumerate(self.data):
                    converted[i] = strToInt[x]
                self.intToStr = intToStr
                self.strToInt = strToInt
                self.data = converted
            return self

        def select(self, selection):
            """Creates a new ``Dataset.Field`` from this one by applying a boolean-valued Numpy array of the same length.

            The new ``Dataset.Field`` is independent of the old one (this is a purely functional method).

            Assumes that the ``Dataset.Field`` is currently in a Numpy representation.

            :type selection: 1-d Numpy array of bool
            :param selection: data points to select
            :rtype: 1-d Numpy array
            :return: subset of the original ``self.data``
            """

            out = Dataset.Field(self.tpe)
            out.data = self.data[selection]
            if hasattr(self, "intToStr"):
                out.intToStr = self.intToStr
                out.strToInt = self.strToInt
            return out

        def toPython(self):
            """Changes this field into a Python representation *in-place* (destructively replaces the old representation)."""

            if self.tpe == numbers.Real:
                self.data = list(self.data)
            elif self.tpe == basestring:
                converter = numpy.array([x for i, x in sorted(self.intToStr.items())], dtype=numpy.dtype(object))
                self.data = list(converter[self.data])
            return self

    def __init__(self, fields, names):
        self.fields = fields
        self.names = names

    @classmethod
    def fromIterable(cls, iterable, limit=None, names=None):
        """Constructor for Dataset that takes a Python iterable (rows) of iterables (columns).

        Each row must have the same number of fields with the same types (``numbers.Real`` or ``basestring``).

        :type iterable: Python iterable
        :param iterable: input dataset
        :type limit: positive integer or ``None``
        :param limit: maximum number of input rows
        :type names: list of strings or ``None``
        :param names: names of the fields; if not provided, names like ``var0``, ``var1``, etc. will be generated.
        :rtype: titus.producer.cart.Dataset
        :return: a dataset
        """

        fields = []
        for lineNumber, line in enumerate(iterable):
            if lineNumber == 0:
                for word in line:
                    if isinstance(word, numbers.Real):
                        fields.append(cls.Field(numbers.Real))
                    elif isinstance(word, basestring):
                        fields.append(cls.Field(basestring))
                    else:
                        raise ValueError("record type must be a real number or a string, not {0}".format(type(word)))
                if names is None:
                    formatter = "var{0:0%dd}" % len(str(len(line)))
                    names = [formatter.format(i) for i in xrange(len(line))]
                else:
                    if len(names) != len(fields):
                        raise ValueError("number of columns in dataset is not the same as the number of names")

            if limit is not None and lineNumber >= limit:
                break

            if len(line) != len(fields):
                raise ValueError("number of columns in dataset is not the same as the first: {0} on line {1}".format(len(line), lineNumber))

            for columnNumber, (word, field) in enumerate(zip(line, fields)):
                if isinstance(word, field.tpe):
                    field.add(word)
                else:
                    raise ValueError("type of column {0} in dataset is not the same as the first: {1} on line {2}".format(columnNumber, type(word), lineNumber))

        return cls([x.toNumpy() for x in fields], names)

    def __repr__(self):
        return "<Dataset with {0} fields at 0x{:08x}>".format(len(self.fields), id(self))

class TreeNode(object):
    """Represents a tree node and applies the CART algorithm to build decision and regression trees.

    The constructors are ``__init__`` and ``fromWholeDataset``.

    Tree-building is initiated by calling ``splitUntil(condition)``, where ``condition(node, depth)`` is a user-supplied function that takes a node (titus.producer.cart.TreeNode) and depth (integer) and returns bool (``True``: continue splitting; ``False``: stop splitting).
    """

    @classmethod
    def fromWholeDataset(cls, wholeDataset, predictandName, maxSubsetSize=None):
        """Constructor for a tree from a dataset that includes the predictand (that which we try to purify in the leaves) as one of its fields.

        :type wholeDataset: titus.producer.cart.Dataset
        :param wholeDataset: dataset including the predictand
        :type predictandName: string
        :param predictandName: name of the predictand, to be taken out of the dataset
        :type maxSubsetSize: positive integer or ``None``
        :param maxSubsetSize: maximum size of subset splits of categorical regressors (approximation for optimization in ``categoricalEntropyGainTerm`` and ``categoricalNVarianceGainTerm``)
        :rtype: titus.producer.cart.TreeNode
        :return: an unsplit tree
        """

        predictandIndex = wholeDataset.names.index(predictandName)
        dataset = Dataset(wholeDataset.fields[:predictandIndex] + wholeDataset.fields[(predictandIndex + 1):],
                          wholeDataset.names[:predictandIndex] + wholeDataset.names[(predictandIndex + 1):])
        predictand = wholeDataset.fields[predictandIndex]
        maxSubsetSize = maxSubsetSize
        return cls(dataset, predictand, maxSubsetSize)

    def __init__(self, dataset, predictand, maxSubsetSize=None):
        """Constructor for a tree from a dataset of regressors (that which we split) and a predictand (that which we try to purify in the leaves).

        :type dataset: titus.producer.cart.Dataset
        :param dataset: dataset of regressors only
        :type predictand: 1-d Numpy array
        :param predictand: predictands in a separate array with the same number of rows as the ``dataset``
        :type maxSubsetSize: positive integer or ``None``
        :param maxSubsetSize: maximum size of subset splits of categorical regressors (approximation for optimization in ``categoricalEntropyGainTerm`` and ``categoricalNVarianceGainTerm``)
        """

        self.dataset = dataset
        self.predictand = predictand
        self.maxSubsetSize = maxSubsetSize

        self.datasetSize = len(self.predictand.data)

        if self.predictand.tpe == numbers.Real:
            try:
                self.predictandUnique = numpy.unique(self.predictand.data)
            except TypeError:
                self.predictandUnique = numpy.unique1d(self.predictand.data)

        elif self.predictand.tpe == basestring:
            if self.datasetSize > 0:
                self.predictandDistribution = []
                for category in xrange(len(self.predictand.intToStr)):
                    frac = 1.0 * numpy.sum(self.predictand.data == category) / len(self.predictand.data)
                    self.predictandDistribution.append(frac)
            else:
                self.predictandDistribution = [0.0] * len(self.predictand.intToStr)

        else:
            raise RuntimeError

    def splitComplete(self):
        """Convenience function for building up a tree until each leaf has only one unique value. Calls ``splitUntil``."""

        self.splitUntil(lambda node, depth: True)

    def splitMaxDepth(self, maxDepth):
        """Convenience function for building up trees until each leaf has only one unique value or the depth reaches ``maxDepth``. Calls ``splitUntil``.

        :type maxDepth: positive integer
        :param maxDepth: maximum allowed depth of the tree
        """

        self.splitUntil(lambda node, depth: depth < maxDepth)

    def splitUntil(self, condition, depth=1):
        """Performs a recursive tree-split, calling the user-supplied ``condition(node, depth)`` at each new node.

        If the predictand is numerical (``numbers.Real``), the node has attributes: ``datasetSize``, ``predictandUnique``, ``nTimesVariance``, and ``gain``.

        If the predictand is categorical (``basestring``), the node has attributes: ``datasetSize``, ``predictandDistribution``, ``entropy``, and ``gain``.

        Splits are performed *in-place*, changing this ``TreeNode``.

        :type condition: callable that takes node (titus.producer.cart.TreeNode) and depth (integer) and returns bool (``True``: continue splitting; ``False``: stop splitting).
        :param condition: splitting condition function
        :type depth: positive integer
        :param depth: current depth
        """

        if self.canSplit():
            self.splitOnce()
            if condition(self, depth):
                self.passBranch.splitUntil(condition, depth + 1)
                self.failBranch.splitUntil(condition, depth + 1)

    def canSplit(self):
        """Returns ``True`` if it is possible to split the predictand; ``False`` otherwise."""

        if self.predictand.tpe == numbers.Real:
            return len(self.predictandUnique) > 1
        elif self.predictand.tpe == basestring:
            return numpy.count_nonzero(self.predictandDistribution) > 0
        else:
            raise RuntimeError

    def score(self):
        """Returns the best score at this ``TreeNode``, which might or might not be a leaf."""

        if self.predictand.tpe == numbers.Real:
            return numpy.mean(self.predictand.data)
        elif self.predictand.tpe == basestring:
            return self.predictand.intToStr[numpy.argmax(self.predictandDistribution)]
        else:
            raise RuntimeError
    
    def splitOnce(self):
        """Compute an optimized split in one field, adding two new ``TreeNodes`` below this one.

        If the predictand is numerical (``numbers.Real``), the split minimizes entropy; if categorical (``basestring``), it minimizes n-times-variance.
        """

        # build a regression tree using n-times-variance as the metric to optimize
        if self.predictand.tpe == numbers.Real:
            self.nTimesVariance = len(self.predictand.data) * numpy.var(self.predictand.data)

            self.gain = None
            self.split = None
            self.fieldIndex = None
            self.field = None
            # for each field...
            for fieldIndex, field in enumerate(self.dataset.fields):
                if field.tpe == numbers.Real:
                    gainTerm, split = self.numericalNVarianceGainTerm(field)
                elif field.tpe == basestring:
                    gainTerm, split = self.categoricalNVarianceGainTerm(field, self.maxSubsetSize)
                else:
                    raise RuntimeError

                # the gainTerm functions don't include this constant (n-times-variance of the unsplit node)
                gainTerm += self.nTimesVariance

                # ... and keep track of the best field, best split
                if self.gain is None or gainTerm > self.gain:
                    self.gain = gainTerm
                    self.split = split
                    self.fieldIndex = fieldIndex
                    self.field = field
            
        # build a classification tree using entropy as the metric to optimize
        elif self.predictand.tpe == basestring:
            # find the best split by maximizing entropic gain
            self.entropy = 0.0
            for frac in self.predictandDistribution:
                if frac != 0.0:
                    self.entropy -= frac * numpy.log2(frac)

            self.gain = None
            self.split = None
            self.fieldIndex = None
            self.field = None
            # for each field...
            for fieldIndex, field in enumerate(self.dataset.fields):
                # go to a function that finds the best choice for that field
                if field.tpe == numbers.Real:
                    gainTerm, split = self.numericalEntropyGainTerm(field)
                elif field.tpe == basestring:
                    gainTerm, split = self.categoricalEntropyGainTerm(field, self.maxSubsetSize)
                else:
                    raise RuntimeError

                # the gainTerm functions don't include this constant (entropy of the unsplit node)
                gainTerm += self.entropy

                # ... and keep track of the best field, best split
                if self.gain is None or gainTerm > self.gain:
                    self.gain = gainTerm
                    self.split = split
                    self.fieldIndex = fieldIndex
                    self.field = field

        else:
            raise RuntimeError

        # construct a new dataset by splitting the best field, best split
        if self.field.tpe == numbers.Real:
            passSelection = self.field.data <= self.split
        elif self.field.tpe == basestring:
            passSelection = numpy.in1d(self.field.data, self.split)
        failSelection = numpy.logical_not(passSelection)

        passDataset = Dataset([], self.dataset.names)
        for field in self.dataset.fields:
            passDataset.fields.append(field.select(passSelection))

        failDataset = Dataset([], self.dataset.names)
        for field in self.dataset.fields:
            failDataset.fields.append(field.select(failSelection))
        
        passPredictand = self.predictand.select(passSelection)
        failPredictand = self.predictand.select(failSelection)

        # create two new tree nodes, one with the data that pass the cut, the other with the data that fail
        self.passBranch = TreeNode(passDataset, passPredictand, self.maxSubsetSize)
        self.failBranch = TreeNode(failDataset, failPredictand, self.maxSubsetSize)

    def numericalEntropyGainTerm(self, field):
        """Split a numerical predictor in such a way that maximizes entropic gain above and below the threshold of the split."""

        # sort values so that we can use running sums (numpy.cumsum)
        sortedIndexes = numpy.argsort(field.data, kind="heapsort")

        # work with the predictor (values) and predictand (categories) for ascending values of the predictor
        values = field.data[sortedIndexes]
        categories = self.predictand.data[sortedIndexes]
        
        # for normalizing
        numInSelection = numpy.arange(1, self.datasetSize + 1, dtype=numpy.dtype(float))
        numNotInSelection = numpy.arange(self.datasetSize, 0, -1, dtype=numpy.dtype(float))

        # sum over the values array using Numpy iteration
        selectionEntropy = numpy.zeros(self.datasetSize, dtype=numpy.dtype(float))
        antiSelectionEntropy = numpy.zeros(self.datasetSize, dtype=numpy.dtype(float))

        # sum over categories using a Python for loop
        # (dataset pre-processing has ensured that the N categories are integers from 0 to N-1, so xrange)
        for category in xrange(len(self.predictand.intToStr)):
            # number of instances of this category for value <= cut
            numAtAndBeforeIndex = numpy.cumsum(categories == category)

            # probability of the category in the (value <= cut) data subset
            frac = numAtAndBeforeIndex / numInSelection

            # for entropy, it is convenient to define 0*log(0) as 0, but Numpy reports it as NaN
            selectionEntropyTerm = frac * numpy.log2(frac)
            selectionEntropyTerm[numpy.isnan(selectionEntropyTerm)] = 0.0

            selectionEntropy -= selectionEntropyTerm

            # now do the same for the anti-selection
            numAtAndBeforeIndex = numAtAndBeforeIndex[-1] - numAtAndBeforeIndex
            frac = numAtAndBeforeIndex / numNotInSelection
            selectionEntropyTerm = frac * numpy.log2(frac)
            selectionEntropyTerm[numpy.isnan(selectionEntropyTerm)] = 0.0

            antiSelectionEntropy -= selectionEntropyTerm

        # calculate gain for all possible index splits
        # (even if some are not unique--- it's faster to just have Numpy scan over them than to try to skip them)
        gains = -(numInSelection/self.datasetSize)*selectionEntropy - (numNotInSelection/self.datasetSize)*antiSelectionEntropy

        # find the highest gain, preferring larger indexes if there's a tie
        # (that's why we reverse the order of the list with [::-1] and then correct for the index for the reversal)
        maxGainIndex = len(gains) - 1 - numpy.argmax(gains[::-1])
        gainTerm = gains[maxGainIndex]

        # for niceness, put the cut between the best index (assuming the cut is "<=") and the next one,
        # so that the cut could equivalently be "<"
        # (the next higher should be exactly one higher because we picked the larger indexes in a tie above)
        nextHigher = maxGainIndex
        while nextHigher < self.datasetSize - 1 and values[nextHigher] == values[maxGainIndex]:
            nextHigher += 1
        cutValue = (values[maxGainIndex] + values[nextHigher]) / 2.0

        return gainTerm, cutValue

    def categoricalEntropyGainTerm(self, field, maxSubsetSize=None):
        """Split a categorical predictor in such a way that maximizes entropic gain inside and outside of a subset of predictor values.

        :type field: titus.producer.cart.Dataset.Field
        :param field: the field to consider when calculating the entropy gain term
        :type maxSubsetSize: positive integer or ``None``
        :param maxSubsetSize: maximum size of subset splits of categorical regressors (approximation for optimization in ``categoricalEntropyGainTerm`` and ``categoricalNVarianceGainTerm``)
        :rtype: (number, list of strings)
        :return: (best gain term, best combination of regressor categories)
        """

        # get a selection array for each category of the variable we want to use to make the prediciton
        numPredictorCategories = len(field.strToInt)
        predictorSelection = numpy.zeros((self.datasetSize, numPredictorCategories), dtype=numpy.dtype(bool))
        for predictorCategory in xrange(numPredictorCategories):
            predictorSelection[:,predictorCategory] = (field.data == predictorCategory)

        # get a selection array for each category that we want to predict
        numPredictandCategories = len(self.predictand.strToInt)
        predictandSelection = numpy.zeros((self.datasetSize, numPredictandCategories), dtype=numpy.dtype(bool))
        for predictandCategory in xrange(numPredictandCategories):
            predictandSelection[:,predictandCategory] = (self.predictand.data == predictandCategory)

        # combine them for all combinations of predictor category and predictand category
        # bitwise_and is equivalent to logical_and (for these boolean arrays) and faster
        numInCategory = [numpy.array([numpy.sum(numpy.bitwise_and(predictorSelection[:,i], predictandSelection[:,j]))
                          for i in xrange(numPredictorCategories)])
                         for j in xrange(numPredictandCategories)]

        # for the denominators
        numMarginal = numpy.array([numpy.sum(predictorSelection[:,i]) for i in xrange(numPredictorCategories)])

        # only consider categories that still have instances at this depth of the tree
        remainingPredictorCategories = [i for i in xrange(numPredictorCategories) if numMarginal[i] > 0]
        remainingPredictandCategories = numpy.array([i for i in xrange(numPredictandCategories) if self.predictandDistribution[i] > 0])

        # we will iterate over (N choose k) for all k from 1 to the highest informative k (half of N, rounding up)
        # or a user-supplied maxSubsetSize (to avoid very long calculations)
        maxInformativeSubset = int(math.ceil(len(remainingPredictorCategories) / 2.0))
        if maxSubsetSize is None:
            maxSubsetSize = maxInformativeSubset
        else:
            maxSubsetSize = min(maxSubsetSize, maxInformativeSubset)

        bestGainTerm = None
        bestCombination = None
        for howMany in xrange(1, maxSubsetSize + 1):
            # itertools.combinations does (N choose k) for a specific k
            for categorySet in itertools.combinations(remainingPredictorCategories, howMany):
                selection = numpy.array(categorySet)
                antiselection = numpy.setdiff1d(remainingPredictorCategories, selection, assume_unique=True)

                numInSelection = 1.0 * numpy.sum(numMarginal[selection])
                numInAntiselection = self.datasetSize - numInSelection

                selectionEntropy = 0.0
                antiSelectionEntropy = 0.0

                for predictandCategory in remainingPredictandCategories:
                    # for the selection
                    numInPredictand = numpy.sum(numInCategory[predictandCategory][selection])
                    if numInPredictand != 0:
                        frac = numInPredictand / numInSelection
                        selectionEntropy -= frac * numpy.log2(frac)

                    # now for the antiselection
                    numInPredictand = numpy.sum(numInCategory[predictandCategory][antiselection])
                    if numInPredictand != 0:
                        frac = numInPredictand / numInAntiselection
                        antiSelectionEntropy -= frac * numpy.log2(frac)

                # gainTerm is scalar, driven by a Python for loop over itertools.combinations
                gainTerm = -(numInSelection/self.datasetSize)*selectionEntropy - (numInAntiselection/self.datasetSize)*antiSelectionEntropy

                # find the best one
                if bestGainTerm is None or gainTerm > bestGainTerm:
                    bestGainTerm = gainTerm
                    bestCombination = categorySet

        return bestGainTerm, bestCombination

    def numericalNVarianceGainTerm(self, field):
        """Split a numerical predictor in such a way that maximizes n-times-variance gain above and below the threshold of the split.
        
        :type field: titus.producer.cart.Dataset.Field
        :param field: the field to consider when calculating the n-times variance gain term
        :rtype: (number, number)
        :return: (best gain term, best cut value)
        """

        # sort values so that we can use running sums (numpy.cumsum)
        sortedIndexes = numpy.argsort(field.data, kind="heapsort")

        # work with the predictor (values) and predictand (predictands) for ascending values of the predictor
        values = field.data[sortedIndexes]
        predictands = self.predictand.data[sortedIndexes]

        # compute less-than-or-equal-to sums for each index using Numpy
        sum1sel = numpy.arange(1, len(values) + 1, dtype=numpy.dtype(float))
        sumxsel = numpy.cumsum(predictands)
        sumxxsel = numpy.cumsum(numpy.power(predictands, 2))

        # derive greater-than sums from the total
        sum1antisel = sum1sel[-1] - sum1sel
        sumxantisel = sumxsel[-1] - sumxsel
        sumxxantisel = sumxxsel[-1] - sumxxsel

        # skip the last entry (run over arrays as [:-1]) because it's the only one whose denominator is zero
        nTimesVarianceInSelection = sumxxsel[:-1] - (sumxsel[:-1]**2/sum1sel[:-1])
        nTimesVarianceInAntiselection = sumxxantisel[:-1] - (sumxantisel[:-1]**2/sum1antisel[:-1])

        # calculate gain for all possible index splits
        # (even if some are not unique--- it's faster to just have Numpy scan over them than to try to skip them)
        gains = -nTimesVarianceInSelection - nTimesVarianceInAntiselection

        # find the highest gain, preferring larger indexes if there's a tie
        # (that's why we reverse the order of the list with [::-1] and then correct for the index for the reversal)
        maxGainIndex = len(gains) - 1 - numpy.argmax(gains[::-1])
        gainTerm = gains[maxGainIndex]

        # for niceness, put the cut between the best index (assuming the cut is "<=") and the next one,
        # so that the cut could equivalently be "<"
        # (the next higher should be exactly one higher because we picked the larger indexes in a tie above)
        nextHigher = maxGainIndex
        while nextHigher < self.datasetSize - 1 and values[nextHigher] == values[maxGainIndex]:
            nextHigher += 1
        cutValue = (values[maxGainIndex] + values[nextHigher]) / 2.0

        return gainTerm, cutValue

    def categoricalNVarianceGainTerm(self, field, maxSubsetSize=None):
        """Split a categorical predictor in such a way that maximizes n-times-variance gain inside and outside of a subset of predictor values.

        :type field: titus.producer.cart.Dataset.Field
        :param field: the field to consider when calculating the n-times-variance gain term
        :type maxSubsetSize: positive integer or ``None``
        :param maxSubsetSize: maximum size of subset splits of categorical regressors (approximation for optimization in ``categoricalEntropyGainTerm`` and ``categoricalNVarianceGainTerm``)
        :rtype: (number, list of strings)
        :return: (best gain term, best combination of regressor categories)
        """

        # find unique values of the predictor field
        try:
            fieldUniques = numpy.unique(field.data)
        except TypeError:
            fieldUniques = numpy.unique1d(field.data)

        # compute a partial sum for each unique value
        sum1 = numpy.empty(len(fieldUniques), dtype=numpy.dtype(float))
        sumx = numpy.empty(len(fieldUniques), dtype=numpy.dtype(float))
        sumxx = numpy.empty(len(fieldUniques), dtype=numpy.dtype(float))
        for i, v in enumerate(fieldUniques):
            predictandValues = self.predictand.data[field.data == v]
            sum1[i] = len(predictandValues)
            sumx[i] = numpy.sum(predictandValues)
            sumxx[i] = numpy.sum(numpy.power(predictandValues, 2))

        # get the totals to more quickly compute antiselections
        sum1total = numpy.sum(sum1)
        sumxtotal = numpy.sum(sumx)
        sumxxtotal = numpy.sum(sumxx)

        # we will iterate over (N choose k) for all k from 1 to the highest informative k (half of N, rounding up)
        # or a user-supplied maxSubsetSize (to avoid very long calculations)
        maxInformativeSubset = int(math.ceil(len(fieldUniques) / 2.0))
        if maxSubsetSize is None:
            maxSubsetSize = maxInformativeSubset
        else:
            maxSubsetSize = min(maxSubsetSize, maxInformativeSubset)

        bestGainTerm = None
        bestCombination = None
        for howMany in xrange(1, maxSubsetSize + 1):
            # itertools.combinations does (N choose k) for a specific k
            for categorySet in itertools.combinations(xrange(len(fieldUniques)), howMany):
                selection = numpy.array(categorySet)

                # for the selection
                sum1sel = numpy.sum(sum1[selection])
                sumxsel = numpy.sum(sumx[selection])
                sumxxsel = numpy.sum(sumxx[selection])
                nTimesVarianceInSelection = (sumxxsel - sumxsel**2/sum1sel) if sum1sel > 0.0 else 0.0

                # for the antiselection
                sum1antisel = sum1total - sum1sel
                sumxantisel = sumxtotal - sumxsel
                sumxxantisel = sumxxtotal - sumxxsel
                nTimesVarianceInAntiselection = (sumxxantisel - sumxantisel**2/sum1antisel) if sum1antisel > 0.0 else 0.0

                # gainTerm is scalar, driven by a Python for loop over itertools.combinations
                gainTerm = -nTimesVarianceInSelection - nTimesVarianceInAntiselection

                # find the best one
                if bestGainTerm is None or gainTerm > bestGainTerm:
                    bestGainTerm = gainTerm
                    bestCombination = categorySet

        return bestGainTerm, bestCombination

    def pfaValueType(self, dataType):
        """Create an Avro schema representing the comparison value type.

        :type dataType: Pythonized JSON
        :param dataType: Avro record schema of the input data
        :rtype: Pythonized JSON
        :return: value type (``value`` field of the PFA ``TreeNode``)
        """

        if dataType.get("type", None) != "record":
            raise TypeError("dataType must be a record")

        dataTypeName = dataType["name"]
        dataFieldNames = [x["name"] for x in dataType["fields"]]

        if not set(self.dataset.names).issubset(set(dataFieldNames)):
            raise TypeError("dataType must be a record with at least as many fields as the ones used to train the tree")

        dataFieldTypes = []
        for field in dataType["fields"]:
            try:
                fieldIndex = self.dataset.names.index(field["name"])
            except ValueError:
                pass
            else:
                if self.dataset.fields[fieldIndex].tpe == numbers.Real:
                    if field["type"] not in ("int", "long", "float", "double"):
                        raise TypeError("dataType field \"{0}\" must be a numeric type, since this was a numeric type in the dataset training".format(field["name"]))
                    dataFieldTypes.append(field["type"])
                elif self.dataset.fields[fieldIndex].tpe == basestring:
                    if field["type"] != "string":
                        raise TypeError("dataType field \"{0}\" must be a string, since this was a string in the dataset training".format(field["name"]))
                    if self.maxSubsetSize == 1:
                        dataFieldTypes.append(field["type"])
                    else:
                        dataFieldTypes.append({"type": "array", "items": field["type"]})

        asjson = [json.dumps(x) for x in dataFieldTypes]
        astypes = ForwardDeclarationParser().parse(asjson)
        return LabelData.broadestType(astypes.values())

    def pfaScoreType(self):
        """Create an Avro schema representing the score type.

        :rtype: Pythonized JSON
        :return: score type (part of the ``pass`` and ``fail`` unions of the PFA ``TreeNode``)
        """

        if self.predictand.tpe == numbers.Real:
            return "double"
        elif self.predictand.tpe == basestring:
            return "string"
        else:
            raise RuntimeError

    def pfaType(self, dataType, treeTypeName, nodeScores=False, datasetSize=False, predictandDistribution=False, predictandUnique=False, entropy=False, nTimesVariance=False, gain=False):
        """Create a PFA type schema representing this tree.

        :type dataType: Pythonized JSON
        :param dataType: Avro record schema of the input data
        :type treeTypeName: string
        :param treeTypeName: name of the tree node record (usually ``TreeNode``)
        :type nodeScores: bool
        :param nodeScores: if ``True``, include a field for intermediate node scores
        :type datasetSize: bool
        :param datasetSize: if ``True``, include a field for the size of the training dataset at each node
        :type predictandDistribution: bool
        :param predictandDistribution: if ``True``, include a field for the distribution of training predictand values (only for classification trees)
        :type predictandUnique: bool
        :param predictandUnique: if ``True``, include a field for unique predictand values at each node
        :type entropy: bool
        :param entropy: if ``True``, include an entropy term at each node (only for classification trees)
        :type nTimesVariance: bool
        :param nTimesVariance: if ``True``, include an n-times-variance term at each node (only for regression trees)
        :type gain: bool
        :param gain: if ``True``, include a gain term at each node
        :rtype: Pythonized JSON
        :return: Avro schema for the tree node type
        """

        valueType = self.pfaValueType(dataType)
        scoreType = self.pfaScoreType()

        out = OrderedDict([("type", "record"),
                           ("name", treeTypeName),
                           ("fields", [{"name": "field", "type": {"type": "enum",
                                                                  "name": dataType["name"] + "Fields",
                                                                  "symbols": [x["name"] for x in dataType["fields"]]}},
                                       {"name": "operator", "type": "string"},
                                       {"name": "value", "type": json.loads(str(valueType.schema))},
                                       {"name": "pass", "type": [scoreType, treeTypeName]},
                                       {"name": "fail", "type": [scoreType, treeTypeName]}])])
        
        if nodeScores:
            out["fields"].append({"name": "score", "type": scoreType})

        if datasetSize:
            out["fields"].append({"name": "datasetSize", "type": "int"})

        if predictandDistribution:
            if self.predictand.tpe != basestring:
                raise TypeError("predictandDistribution can only be used if the predictand is a string (classification trees)")
            out["fields"].append({"name": "scoreDistribution", "type": {"type": "map", "values": "int"}})

        if predictandUnique:
            out["fields"].append({"name": "scoreValues", "type": {"type": "array", "items": scoreType}})

        if entropy:
            if self.predictand.tpe != basestring:
                raise TypeError("entropy can only be used if the predictand is a string (classification trees)")
            out["fields"].append({"name": "entropy", "type": "double"})

        if nTimesVariance:
            if self.predictand.tpe != numbers.Real:
                raise TypeError("nTimesVariance can only be used if the predictand is a number (regression trees)")
            out["fields"].append({"name": "nTimesVariance", "type": "double"})

        if gain:
            out["fields"].append({"name": "gain", "type": "double"})

        return out

    def pfaValue(self, dataType, treeTypeName, nodeScores=False, datasetSize=False, predictandDistribution=False, predictandUnique=False, entropy=False, nTimesVariance=False, gain=False, valueType=None):
        """Create a PFA data structure representing this tree.

        :type dataType: Pythonized JSON
        :param dataType: Avro record schema of the input data
        :type treeTypeName: string
        :param treeTypeName: name of the tree node record (usually ``TreeNode``)
        :type nodeScores: bool
        :param nodeScores: if ``True``, include a field for intermediate node scores
        :type datasetSize: bool
        :param datasetSize: if ``True``, include a field for the size of the training dataset at each node
        :type predictandDistribution: bool
        :param predictandDistribution: if ``True``, include a field for the distribution of training predictand values (only for classification trees)
        :type predictandUnique: bool
        :param predictandUnique: if ``True``, include a field for unique predictand values at each node
        :type entropy: bool
        :param entropy: if ``True``, include an entropy term at each node (only for classification trees)
        :type nTimesVariance: bool
        :param nTimesVariance: if ``True``, include an n-times-variance term at each node (only for regression trees)
        :type gain: bool
        :param gain: if ``True``, include a gain term at each node
        :type valueType: Pythonized JSON or ``None``
        :param valueType: if ``None``, call ``self.pfaValueType(dataType)`` to generate a value type; otherwise, take the given value
        :rtype: Pythonized JSON
        :return: PFA data structure for the tree, to be inserted into the cell or pool's ``init`` field
        """

        scoreType = self.pfaScoreType()
            
        if not hasattr(self, "passBranch") and not hasattr(self, "failBranch"):
            return {scoreType: self.score()}

        else:
            fieldName = self.dataset.names[self.fieldIndex]

            if self.field.tpe == numbers.Real:
                operator = "<="
                value = self.split
            elif self.field.tpe == basestring:
                if self.maxSubsetSize == 1:
                    operator = "=="
                    value = self.dataset.fields[self.fieldIndex].intToStr[self.split[0]]
                else:
                    operator = "in"
                    value = [self.dataset.fields[self.fieldIndex].intToStr[x] for x in self.split]
            else:
                raise RuntimeError

            if valueType is None:
                valueType = self.pfaValueType(dataType)
            if isinstance(valueType, AvroUnion):
                specificType, = [x["type"] for x in dataType["fields"] if x["name"] == fieldName]
                if isinstance(specificType, dict):
                    specificType = specificType["type"]
                value = {specificType: value}
            
            passBranch = self.passBranch.pfaValue(dataType, treeTypeName, nodeScores, datasetSize, predictandDistribution, predictandUnique, entropy, nTimesVariance, gain, valueType)
            failBranch = self.failBranch.pfaValue(dataType, treeTypeName, nodeScores, datasetSize, predictandDistribution, predictandUnique, entropy, nTimesVariance, gain, valueType)

            if passBranch.keys() != [scoreType]:
                passBranch = {treeTypeName: passBranch}
            if failBranch.keys() != [scoreType]:
                failBranch = {treeTypeName: failBranch}

            out = OrderedDict([("field", fieldName),
                               ("operator", operator),
                               ("value", value),
                               ("pass", passBranch),
                               ("fail", failBranch)])

            if nodeScores:
                out["score"] = self.score()

            if datasetSize:
                out["datasetSize"] = self.datasetSize

            if predictandDistribution:
                if self.predictand.tpe != basestring:
                    raise TypeError("predictandDistribution can only be used if the predictand is a string (classification trees)")
                out["scoreDistribution"] = OrderedDict(sorted((self.predictand.intToStr[i], x) for i, x in enumerate(self.predictandDistribution)))

            if predictandUnique:
                if hasattr(self, "predictandDistribution"):
                    out["scoreValues"] = sorted(self.predictand.intToStr[i] for i, x in enumerate(self.predictandDistribution) if x > 0.0)
                elif hasattr(self, "predictandUnique"):
                    out["scoreValues"] = sorted(self.predictandUnique)
                else:
                    raise RuntimeError

            if entropy:
                if self.predictand.tpe != basestring:
                    raise TypeError("entropy can only be used if the predictand is a string (classification trees)")
                out["entropy"] = self.entropy

            if nTimesVariance:
                if self.predictand.tpe != numbers.Real:
                    raise TypeError("nTimesVariance can only be used if the predictand is a number (regression trees)")
                out["nTimesVariance"] = self.nTimesVariance

            if gain:
                out["gain"] = self.gain

            return out

    def pfaDocument(self, inputType, treeTypeName, dataType=None, preprocess=None, nodeScores=False, datasetSize=False, predictandDistribution=False, predictandUnique=False, entropy=False, nTimesVariance=False, gain=False):
        """Create a PFA document to score with this tree.

        :type inputType: Pythonized JSON
        :param inputType: Avro record schema of the input data
        :type treeTypeName: string
        :param treeTypeName: name of the tree node record (usually ``TreeNode``)
        :type dataType: Pythonized JSON
        :param dataType: Avro record schema of the data that goes to the tree, possibly preprocessed
        :type preprocess: PrettyPFA substitution or ``None``
        :param preprocess: pre-processing expression
        :type nodeScores: bool
        :param nodeScores: if ``True``, include a field for intermediate node scores
        :type datasetSize: bool
        :param datasetSize: if ``True``, include a field for the size of the training dataset at each node
        :type predictandDistribution: bool
        :param predictandDistribution: if ``True``, include a field for the distribution of training predictand values (only for classification trees)
        :type predictandUnique: bool
        :param predictandUnique: if ``True``, include a field for unique predictand values at each node
        :type entropy: bool
        :param entropy: if ``True``, include an entropy term at each node (only for classification trees)
        :type nTimesVariance: bool
        :param nTimesVariance: if ``True``, include an n-times-variance term at each node (only for regression trees)
        :type gain: bool
        :param gain: if ``True``, include a gain term at each node
        :rtype: Pythonized JSON
        :return: complete PFA document for running tree classification or regression
        """

        if dataType is None:
            dataType = inputType
        dataTypeName = dataType["name"]

        scoreType = self.pfaScoreType()
        treeType = self.pfaType(dataType, treeTypeName, nodeScores, datasetSize, predictandDistribution, predictandUnique, entropy, nTimesVariance, gain)
        treeValue = self.pfaValue(dataType, treeTypeName, nodeScores, datasetSize, predictandDistribution, predictandUnique, entropy, nTimesVariance, gain)
        if preprocess is None:
            preprocess = "input"

        return titus.prettypfa.jsonNode('''
input: <<inputType>>
output: <<scoreType>>
cells:
  tree(<<treeType>>) = <<treeValue>>
action:
  model.tree.simpleWalk(<<preprocess>>, tree,
    fcn(d: <<dataTypeName>>, t: <<treeTypeName>> -> boolean)
      model.tree.simpleTest(d, t))
''', **vars())
