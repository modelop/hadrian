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

import version_independent as ind    

namespace = "http://www.dmg.org/PMML-4_2"

class PMML(ind.PMML):
    """Represents a <PMML> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(PMML, self).__init__()
        self.version = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.AssociationModel = []
        self.BaselineModel = []
        self.ClusteringModel = []
        self.DataDictionary = []
        self.Extension = []
        self.GeneralRegressionModel = []
        self.Header = []
        self.MiningBuildTask = []
        self.MiningModel = []
        self.NaiveBayesModel = []
        self.NearestNeighborModel = []
        self.NeuralNetwork = []
        self.RegressionModel = []
        self.RuleSetModel = []
        self.Scorecard = []
        self.SequenceModel = []
        self.SupportVectorMachineModel = []
        self.TextModel = []
        self.TimeSeriesModel = []
        self.TransformationDictionary = []
        self.TreeModel = []

    def models(self):
        return [x for x in self.AssociationModel + self.BaselineModel + self.ClusteringModel + self.GeneralRegressionModel + self.MiningModel + self.NaiveBayesModel + self.NearestNeighborModel + self.NeuralNetwork + self.RegressionModel + self.RuleSetModel + self.Scorecard + self.SequenceModel + self.SupportVectorMachineModel + self.TextModel + self.TimeSeriesModel + self.TreeModel if x.isScorable != False]

class ARIMA(ind.ARIMA):
    """Represents a <ARIMA> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ARIMA, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        
class Aggregate(ind.Aggregate):
    """Represents a <Aggregate> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Aggregate, self).__init__()
        self.field = None
        self.function = None
        self.groupField = None
        self.sqlWhere = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Alternate(ind.Alternate):
    """Represents a <Alternate> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Alternate, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.AnyDistribution = []
        self.Extension = []
        self.GaussianDistribution = []
        self.PoissonDistribution = []
        self.UniformDistribution = []
        
class Annotation(ind.Annotation):
    """Represents a <Annotation> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Annotation, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Anova(ind.Anova):
    """Represents a <Anova> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Anova, self).__init__()
        self.target = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.AnovaRow = []
        self.Extension = []
        
class AnovaRow(ind.AnovaRow):
    """Represents a <AnovaRow> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(AnovaRow, self).__init__()
        self.type = None
        self.sumOfSquares = None
        self.degreesOfFreedom = None
        self.meanOfSquares = None
        self.fValue = None
        self.pValue = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class AntecedentSequence(ind.AntecedentSequence):
    """Represents a <AntecedentSequence> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(AntecedentSequence, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.SequenceReference = []
        self.Time = []
        
class AnyDistribution(ind.AnyDistribution):
    """Represents a <AnyDistribution> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(AnyDistribution, self).__init__()
        self.mean = None
        self.variance = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Application(ind.Application):
    """Represents a <Application> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Application, self).__init__()
        self.name = None
        self.version = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Apply(ind.Apply):
    """Represents a <Apply> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Apply, self).__init__()
        self.function = None
        self.mapMissingTo = None
        self.defaultValue = None
        self.invalidValueTreatment = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Aggregate = []
        self.Apply = []
        self.Constant = []
        self.Discretize = []
        self.Extension = []
        self.FieldRef = []
        self.MapValues = []
        self.NormContinuous = []
        self.NormDiscrete = []
        self.TextIndex = []
        
class Array(ind.Array):
    """Represents a <Array> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Array, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        
class AssociationModel(ind.AssociationModel):
    """Represents a <AssociationModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(AssociationModel, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.numberOfTransactions = None
        self.maxNumberOfItemsPerTA = None
        self.avgNumberOfItemsPerTA = None
        self.minimumSupport = None
        self.minimumConfidence = None
        self.lengthLimit = None
        self.numberOfItems = None
        self.numberOfItemsets = None
        self.numberOfRules = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.AssociationRule = []
        self.Extension = []
        self.Item = []
        self.Itemset = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Output = []
        
class AssociationRule(ind.AssociationRule):
    """Represents a <AssociationRule> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(AssociationRule, self).__init__()
        self.antecedent = None
        self.consequent = None
        self.support = None
        self.confidence = None
        self.lift = None
        self.leverage = None
        self.affinity = None
        self.id = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Attribute(ind.Attribute):
    """Represents a <Attribute> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Attribute, self).__init__()
        self.reasonCode = None
        self.partialScore = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.ComplexPartialScore = []
        self.CompoundPredicate = []
        self.Extension = []
        self.AlwaysFalse = []
        self.SimplePredicate = []
        self.SimpleSetPredicate = []
        self.AlwaysTrue = []
        
class BaseCumHazardTables(ind.BaseCumHazardTables):
    """Represents a <BaseCumHazardTables> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(BaseCumHazardTables, self).__init__()
        self.maxTime = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.BaselineCell = []
        self.BaselineStratum = []
        self.Extension = []
        
class Baseline(ind.Baseline):
    """Represents a <Baseline> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Baseline, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.AnyDistribution = []
        self.CountTable = []
        self.Extension = []
        self.FieldRef = []
        self.GaussianDistribution = []
        self.NormalizedCountTable = []
        self.PoissonDistribution = []
        self.UniformDistribution = []

    def distribution(self):
        return (self.AnyDistribution + self.GaussianDistribution + self.PoissonDistribution + self.UniformDistribution)[0]
        
class BaselineCell(ind.BaselineCell):
    """Represents a <BaselineCell> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(BaselineCell, self).__init__()
        self.time = None
        self.cumHazard = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class BaselineModel(ind.BaselineModel):
    """Represents a <BaselineModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(BaselineModel, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Output = []
        self.Targets = []
        self.TestDistributions = []
        
class BaselineStratum(ind.BaselineStratum):
    """Represents a <BaselineStratum> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(BaselineStratum, self).__init__()
        self.value = None
        self.label = None
        self.maxTime = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.BaselineCell = []
        self.Extension = []
        
class BayesInput(ind.BayesInput):
    """Represents a <BayesInput> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(BayesInput, self).__init__()
        self.fieldName = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.DerivedField = []
        self.Extension = []
        self.PairCounts = []
        self.TargetValueStats = []
        
class BayesInputs(ind.BayesInputs):
    """Represents a <BayesInputs> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(BayesInputs, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.BayesInput = []
        self.Extension = []
        
class BayesOutput(ind.BayesOutput):
    """Represents a <BayesOutput> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(BayesOutput, self).__init__()
        self.fieldName = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.TargetValueCounts = []
        
class BoundaryValueMeans(ind.BoundaryValueMeans):
    """Represents a <BoundaryValueMeans> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(BoundaryValueMeans, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Extension = []
        
class BoundaryValues(ind.BoundaryValues):
    """Represents a <BoundaryValues> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(BoundaryValues, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Extension = []
        
class CategoricalPredictor(ind.CategoricalPredictor):
    """Represents a <CategoricalPredictor> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(CategoricalPredictor, self).__init__()
        self.name = None
        self.value = None
        self.coefficient = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Categories(ind.Categories):
    """Represents a <Categories> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Categories, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Category = []
        self.Extension = []
        
class Category(ind.Category):
    """Represents a <Category> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Category, self).__init__()
        self.value = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Characteristic(ind.Characteristic):
    """Represents a <Characteristic> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Characteristic, self).__init__()
        self.name = None
        self.reasonCode = None
        self.baselineScore = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Attribute = []
        self.Extension = []
        
class Characteristics(ind.Characteristics):
    """Represents a <Characteristics> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Characteristics, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Characteristic = []
        self.Extension = []
        
class ChildParent(ind.ChildParent):
    """Represents a <ChildParent> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ChildParent, self).__init__()
        self.childField = None
        self.parentField = None
        self.parentLevelField = None
        self.isRecursive = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.FieldColumnPair = []
        self.InlineTable = []
        self.TableLocator = []
        
class ClassLabels(ind.ClassLabels):
    """Represents a <ClassLabels> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ClassLabels, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Extension = []
        
class Cluster(ind.Cluster):
    """Represents a <Cluster> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Cluster, self).__init__()
        self.id = None
        self.name = None
        self.size = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Covariances = []
        self.Extension = []
        self.KohonenMap = []
        self.Partition = []
        
class ClusteringField(ind.ClusteringField):
    """Represents a <ClusteringField> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ClusteringField, self).__init__()
        self.field = None
        self.isCenterField = None
        self.fieldWeight = None
        self.similarityScale = None
        self.compareFunction = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Comparisons = []
        self.Extension = []
        
class ClusteringModel(ind.ClusteringModel):
    """Represents a <ClusteringModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ClusteringModel, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.modelClass = None
        self.numberOfClusters = None
        self.isScorable = None
        for key, value in attribs.items():
            """Represents a <= None
        self.numberOfClusters = None
        self.isScorable = None
        for key, value in attribs.items> tag in v4.2 and provides methods to convert to PFA."""
            setattr(self, key, value)
        self.Cluster = []
        self.ClusteringField = []
        self.ComparisonMeasure = []
        self.Extension = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.MissingValueWeights = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Output = []
        
class ClusteringModelQuality(ind.ClusteringModelQuality):
    """Represents a <ClusteringModelQuality> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ClusteringModelQuality, self).__init__()
        self.dataName = None
        self.SSE = None
        self.SSB = None
        for key, value in attribs.items():
            setattr(self, key, value)
        
class Coefficient(ind.Coefficient):
    """Represents a <Coefficient> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Coefficient, self).__init__()
        self.value = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Coefficients(ind.Coefficients):
    """Represents a <Coefficients> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Coefficients, self).__init__()
        self.numberOfCoefficients = None
        self.absoluteValue = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Coefficient = []
        self.Extension = []
        
class ComparisonMeasure(ind.ComparisonMeasure):
    """Represents a <ComparisonMeasure> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ComparisonMeasure, self).__init__()
        self.kind = None
        self.compareFunction = None
        self.minimum = None
        self.maximum = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.binarySimilarity = []
        self.chebychev = []
        self.cityBlock = []
        self.euclidean = []
        self.jaccard = []
        self.minkowski = []
        self.simpleMatching = []
        self.squaredEuclidean = []
        self.tanimoto = []
        
class Comparisons(ind.Comparisons):
    """Represents a <Comparisons> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Comparisons, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Matrix = []
        
class ComplexPartialScore(ind.ComplexPartialScore):
    """Represents a <ComplexPartialScore> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ComplexPartialScore, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Aggregate = []
        self.Apply = []
        self.Constant = []
        self.Discretize = []
        self.Extension = []
        self.FieldRef = []
        self.MapValues = []
        self.NormContinuous = []
        self.NormDiscrete = []
        self.TextIndex = []
        
class CompoundPredicate(ind.CompoundPredicate):
    """Represents a <CompoundPredicate> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(CompoundPredicate, self).__init__()
        self.booleanOperator = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.CompoundPredicate = []
        self.Extension = []
        self.AlwaysFalse = []
        self.SimplePredicate = []
        self.SimpleSetPredicate = []
        self.AlwaysTrue = []
        
class CompoundRule(ind.CompoundRule):
    """Represents a <CompoundRule> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(CompoundRule, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.CompoundPredicate = []
        self.CompoundRule = []
        self.Extension = []
        self.AlwaysFalse = []
        self.SimplePredicate = []
        self.SimpleRule = []
        self.SimpleSetPredicate = []
        self.AlwaysTrue = []
        
class Con(ind.Con):
    """Represents a <Con> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Con, self).__init__()
        self.isfrom = None
        self.weight = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class ConfusionMatrix(ind.ConfusionMatrix):
    """Represents a <ConfusionMatrix> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ConfusionMatrix, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.ClassLabels = []
        self.Extension = []
        self.Matrix = []
        
class ConsequentSequence(ind.ConsequentSequence):
    """Represents a <ConsequentSequence> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ConsequentSequence, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.SequenceReference = []
        self.Time = []
        
class Constant(ind.Constant):
    """Represents a <Constant> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Constant, self).__init__()
        self.dataType = None
        for key, value in attribs.items():
            setattr(self, key, value)
        
class Constraints(ind.Constraints):
    """Represents a <Constraints> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Constraints, self).__init__()
        self.minimumNumberOfItems = None
        self.maximumNumberOfItems = None
        self.minimumNumberOfAntecedentItems = None
        self.maximumNumberOfAntecedentItems = None
        self.minimumNumberOfConsequentItems = None
        self.maximumNumberOfConsequentItems = None
        self.minimumSupport = None
        self.minimumConfidence = None
        self.minimumLift = None
        self.minimumTotalSequenceTime = None
        self.maximumTotalSequenceTime = None
        self.minimumItemsetSeparationTime = None
        self.maximumItemsetSeparationTime = None
        self.minimumAntConsSeparationTime = None
        self.maximumAntConsSeparationTime = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class ContStats(ind.ContStats):
    """Represents a <ContStats> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ContStats, self).__init__()
        self.totalValuesSum = None
        self.totalSquaresSum = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Extension = []
        self.Interval = []
        
class CorrelationFields(ind.CorrelationFields):
    """Represents a <CorrelationFields> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(CorrelationFields, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Extension = []
        
class CorrelationMethods(ind.CorrelationMethods):
    """Represents a <CorrelationMethods> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(CorrelationMethods, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Matrix = []
        
class CorrelationValues(ind.CorrelationValues):
    """Represents a <CorrelationValues> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(CorrelationValues, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Matrix = []
        
class Correlations(ind.Correlations):
    """Represents a <Correlations> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Correlations, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.CorrelationFields = []
        self.CorrelationMethods = []
        self.CorrelationValues = []
        self.Extension = []
        
class CountTable(ind.CountTable):
    """Represents a <CountTable> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(CountTable, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        
class Counts(ind.Counts):
    """Represents a <Counts> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Counts, self).__init__()
        self.totalFreq = None
        self.missingFreq = None
        self.invalidFreq = None
        self.cardinality = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Covariances(ind.Covariances):
    """Represents a <Covariances> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Covariances, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Matrix = []
        
class CovariateList(ind.CovariateList):
    """Represents a <CovariateList> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(CovariateList, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Predictor = []
        
class DataDictionary(ind.DataDictionary):
    """Represents a <DataDictionary> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(DataDictionary, self).__init__()
        self.numberOfFields = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.DataField = []
        self.Extension = []
        self.Taxonomy = []
        
class DataField(ind.DataField):
    """Represents a <DataField> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(DataField, self).__init__()
        self.name = None
        self.displayName = None
        self.optype = None
        self.dataType = None
        self.taxonomy = None
        self.isCyclic = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Interval = []
        self.Value = []
        
class Decision(ind.Decision):
    """Represents a <Decision> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Decision, self).__init__()
        self.value = None
        self.displayValue = None
        self.description = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class DecisionTree(ind.DecisionTree):
    """Represents a <DecisionTree> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(DecisionTree, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.missingValueStrategy = None
        self.missingValuePenalty = None
        self.noTrueChildStrategy = None
        self.splitCharacteristic = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.LocalTransformations = []
        self.ModelStats = []
        self.Node = []
        self.Output = []
        self.ResultField = []
        self.Targets = []
        
class Decisions(ind.Decisions):
    """Represents a <Decisions> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Decisions, self).__init__()
        self.businessProblem = None
        self.description = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Decision = []
        self.Extension = []
        
class DefineFunction(ind.DefineFunction):
    """Represents a <DefineFunction> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(DefineFunction, self).__init__()
        self.name = None
        self.optype = None
        self.dataType = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Aggregate = []
        self.Apply = []
        self.Constant = []
        self.Discretize = []
        self.Extension = []
        self.FieldRef = []
        self.MapValues = []
        self.NormContinuous = []
        self.NormDiscrete = []
        self.ParameterField = []
        self.TextIndex = []
        
class Delimiter(ind.Delimiter):
    """Represents a <Delimiter> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Delimiter, self).__init__()
        self.delimiter = None
        self.gap = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class DerivedField(ind.DerivedField):
    """Represents a <DerivedField> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(DerivedField, self).__init__()
        self.name = None
        self.displayName = None
        self.optype = None
        self.dataType = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Aggregate = []
        self.Apply = []
        self.Constant = []
        self.Discretize = []
        self.Extension = []
        self.FieldRef = []
        self.MapValues = []
        self.NormContinuous = []
        self.NormDiscrete = []
        self.TextIndex = []
        self.Value = []

class DiscrStats(ind.DiscrStats):
    """Represents a <DiscrStats> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(DiscrStats, self).__init__()
        self.modalValue = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Extension = []
        
class Discretize(ind.Discretize):
    """Represents a <Discretize> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Discretize, self).__init__()
        self.field = None
        self.mapMissingTo = None
        self.defaultValue = None
        self.dataType = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.DiscretizeBin = []
        self.Extension = []
        
class DiscretizeBin(ind.DiscretizeBin):
    """Represents a <DiscretizeBin> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(DiscretizeBin, self).__init__()
        self.binValue = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Interval = []
        
class DocumentTermMatrix(ind.DocumentTermMatrix):
    """Represents a <DocumentTermMatrix> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(DocumentTermMatrix, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Matrix = []
        
class EventValues(ind.EventValues):
    """Represents a <EventValues> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(EventValues, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Interval = []
        self.Value = []
        
class ExponentialSmoothing(ind.ExponentialSmoothing):
    """Represents a <ExponentialSmoothing> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ExponentialSmoothing, self).__init__()
        self.RMSE = None
        self.transformation = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Level = []
        self.Seasonality_ExpoSmooth = []
        self.TimeValue = []
        self.Trend_ExpoSmooth = []
        
class Extension(ind.Extension):
    """Represents a <Extension> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Extension, self).__init__()
        self.extender = None
        self.name = None
        self.value = None
        for key, value in attribs.items():
            setattr(self, key, value)
        
class FactorList(ind.FactorList):
    """Represents a <FactorList> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(FactorList, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Predictor = []
        
class AlwaysFalse(ind.AlwaysFalse):
    """Represents a <AlwaysFalse> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(AlwaysFalse, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
    @property
    def tag(self):
        return "False"

class FieldColumnPair(ind.FieldColumnPair):
    """Represents a <FieldColumnPair> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(FieldColumnPair, self).__init__()
        self.field = None
        self.column = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class FieldRef(ind.FieldRef):
    """Represents a <FieldRef> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(FieldRef, self).__init__()
        self.field = None
        self.mapMissingTo = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class FieldValue(ind.FieldValue):
    """Represents a <FieldValue> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(FieldValue, self).__init__()
        self.field = None
        self.value = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.FieldValue = []
        self.FieldValueCount = []
        
class FieldValueCount(ind.FieldValueCount):
    """Represents a <FieldValueCount> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(FieldValueCount, self).__init__()
        self.field = None
        self.value = None
        self.count = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class GaussianDistribution(ind.GaussianDistribution):
    """Represents a <GaussianDistribution> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(GaussianDistribution, self).__init__()
        self.mean = None
        self.variance = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class GeneralRegressionModel(ind.GeneralRegressionModel):
    """Represents a <GeneralRegressionModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(GeneralRegressionModel, self).__init__()
        self.targetVariableName = None
        self.modelType = None
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.targetReferenceCategory = None
        self.cumulativeLink = None
        self.linkFunction = None
        self.linkParameter = None
        self.trialsVariable = None
        self.trialsValue = None
        self.distribution = None
        self.distParameter = None
        self.offsetVariable = None
        self.offsetValue = None
        self.modelDF = None
        self.endTimeVariable = None
        self.startTimeVariable = None
        self.subjectIDVariable = None
        self.statusVariable = None
        self.baselineStrataVariable = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.BaseCumHazardTables = []
        self.CovariateList = []
        self.EventValues = []
        self.Extension = []
        self.FactorList = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Output = []
        self.PCovMatrix = []
        self.PPMatrix = []
        self.ParamMatrix = []
        self.ParameterList = []
        self.Targets = []
        
class Header(ind.Header):
    """Represents a <Header> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Header, self).__init__()
        self.copyright = None
        self.description = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Annotation = []
        self.Application = []
        self.Extension = []
        self.Timestamp = []
        
class INT_Entries(ind.INT_Entries):
    """Represents a <INT_Entries> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(INT_Entries, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        
    @property
    def tag(self):
        return "INT-Entries"

class INT_SparseArray(ind.INT_SparseArray):
    """Represents a <INT_SparseArray> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(INT_SparseArray, self).__init__()
        self.n = None
        self.defaultValue = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.INT_Entries = []
        self.Indices = []
        
    @property
    def tag(self):
        return "INT-SparseArray"

class Indices(ind.Indices):
    """Represents a <Indices> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Indices, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        
class InlineTable(ind.InlineTable):
    """Represents a <InlineTable> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(InlineTable, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.row = []
        
class InstanceField(ind.InstanceField):
    """Represents a <InstanceField> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(InstanceField, self).__init__()
        self.field = None
        self.column = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class InstanceFields(ind.InstanceFields):
    """Represents a <InstanceFields> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(InstanceFields, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.InstanceField = []
        
class Interval(ind.Interval):
    """Represents a <Interval> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Interval, self).__init__()
        self.closure = None
        self.leftMargin = None
        self.rightMargin = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Item(ind.Item):
    """Represents a <Item> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Item, self).__init__()
        self.id = None
        self.value = None
        self.mappedValue = None
        self.weight = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class ItemRef(ind.ItemRef):
    """Represents a <ItemRef> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ItemRef, self).__init__()
        self.itemRef = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Itemset(ind.Itemset):
    """Represents a <Itemset> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Itemset, self).__init__()
        self.id = None
        self.support = None
        self.numberOfItems = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.ItemRef = []
        
class KNNInput(ind.KNNInput):
    """Represents a <KNNInput> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(KNNInput, self).__init__()
        self.field = None
        self.fieldWeight = None
        self.compareFunction = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class KNNInputs(ind.KNNInputs):
    """Represents a <KNNInputs> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(KNNInputs, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.KNNInput = []
        
class KohonenMap(ind.KohonenMap):
    """Represents a <KohonenMap> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(KohonenMap, self).__init__()
        self.coord1 = None
        self.coord2 = None
        self.coord3 = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Level(ind.Level):
    """Represents a <Level> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Level, self).__init__()
        self.alpha = None
        self.smoothedValue = None
        for key, value in attribs.items():
            setattr(self, key, value)
        
class LiftData(ind.LiftData):
    """Represents a <LiftData> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(LiftData, self).__init__()
        self.targetFieldValue = None
        self.targetFieldDisplayValue = None
        self.rankingQuality = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.ModelLiftGraph = []
        self.OptimumLiftGraph = []
        self.RandomLiftGraph = []
        
class LiftGraph(ind.LiftGraph):
    """Represents a <LiftGraph> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(LiftGraph, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.BoundaryValueMeans = []
        self.BoundaryValues = []
        self.Extension = []
        self.XCoordinates = []
        self.YCoordinates = []
        
class LinearKernelType(ind.LinearKernelType):
    """Represents a <LinearKernelType> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(LinearKernelType, self).__init__()
        self.description = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class LinearNorm(ind.LinearNorm):
    """Represents a <LinearNorm> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(LinearNorm, self).__init__()
        self.orig = None
        self.norm = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class LocalTransformations(ind.LocalTransformations):
    """Represents a <LocalTransformations> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(LocalTransformations, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.DerivedField = []
        self.Extension = []
        
class MapValues(ind.MapValues):
    """Represents a <MapValues> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(MapValues, self).__init__()
        self.mapMissingTo = None
        self.defaultValue = None
        self.outputColumn = None
        self.dataType = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.FieldColumnPair = []
        self.InlineTable = []
        self.TableLocator = []
        
class MatCell(ind.MatCell):
    """Represents a <MatCell> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(MatCell, self).__init__()
        self.row = None
        self.col = None
        for key, value in attribs.items():
            setattr(self, key, value)
        
class Matrix(ind.Matrix):
    """Represents a <Matrix> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Matrix, self).__init__()
        self.kind = None
        self.nbRows = None
        self.nbCols = None
        self.diagDefault = None
        self.offDiagDefault = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.MatCell = []
        
class MiningBuildTask(ind.MiningBuildTask):
    """Represents a <MiningBuildTask> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(MiningBuildTask, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class MiningField(ind.MiningField):
    """Represents a <MiningField> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(MiningField, self).__init__()
        self.name = None
        self.usageType = None
        self.optype = None
        self.importance = None
        self.outliers = None
        self.lowValue = None
        self.highValue = None
        self.missingValueReplacement = None
        self.missingValueTreatment = None
        self.invalidValueTreatment = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class MiningModel(ind.MiningModel):
    """Represents a <MiningModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(MiningModel, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.DecisionTree = []
        self.Extension = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Output = []
        self.Regression = []
        self.Segmentation = []
        self.Targets = []
        
class MiningSchema(ind.MiningSchema):
    """Represents a <MiningSchema> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(MiningSchema, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.MiningField = []
        
class MissingValueWeights(ind.MissingValueWeights):
    """Represents a <MissingValueWeights> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(MissingValueWeights, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Extension = []
        
class ModelExplanation(ind.ModelExplanation):
    """Represents a <ModelExplanation> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ModelExplanation, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.ClusteringModelQuality = []
        self.Correlations = []
        self.Extension = []
        self.PredictiveModelQuality = []
        
class ModelLiftGraph(ind.ModelLiftGraph):
    """Represents a <ModelLiftGraph> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ModelLiftGraph, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.LiftGraph = []
        
class ModelStats(ind.ModelStats):
    """Represents a <ModelStats> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ModelStats, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.MultivariateStats = []
        self.UnivariateStats = []
        
class ModelVerification(ind.ModelVerification):
    """Represents a <ModelVerification> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ModelVerification, self).__init__()
        self.recordCount = None
        self.fieldCount = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.InlineTable = []
        self.VerificationFields = []
        
class MultivariateStat(ind.MultivariateStat):
    """Represents a <MultivariateStat> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(MultivariateStat, self).__init__()
        self.name = None
        self.category = None
        self.exponent = None
        self.isIntercept = None
        self.importance = None
        self.stdError = None
        self.tValue = None
        self.chiSquareValue = None
        self.fStatistic = None
        self.dF = None
        self.pValueAlpha = None
        self.pValueInitial = None
        self.pValueFinal = None
        self.confidenceLevel = None
        self.confidenceLowerBound = None
        self.confidenceUpperBound = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class MultivariateStats(ind.MultivariateStats):
    """Represents a <MultivariateStats> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(MultivariateStats, self).__init__()
        self.targetCategory = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.MultivariateStat = []
        
class NaiveBayesModel(ind.NaiveBayesModel):
    """Represents a <NaiveBayesModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(NaiveBayesModel, self).__init__()
        self.modelName = None
        self.threshold = None
        self.functionName = None
        self.algorithmName = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.BayesInputs = []
        self.BayesOutput = []
        self.Extension = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Output = []
        self.Targets = []
        
class NearestNeighborModel(ind.NearestNeighborModel):
    """Represents a <NearestNeighborModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(NearestNeighborModel, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.numberOfNeighbors = None
        self.continuousScoringMethod = None
        self.categoricalScoringMethod = None
        self.instanceIdVariable = None
        self.threshold = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.ComparisonMeasure = []
        self.Extension = []
        self.KNNInputs = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Output = []
        self.Targets = []
        self.TrainingInstances = []
        
class NeuralInput(ind.NeuralInput):
    """Represents a <NeuralInput> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(NeuralInput, self).__init__()
        self.id = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.DerivedField = []
        self.Extension = []
        
class NeuralInputs(ind.NeuralInputs):
    """Represents a <NeuralInputs> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(NeuralInputs, self).__init__()
        self.numberOfInputs = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.NeuralInput = []
        
class NeuralLayer(ind.NeuralLayer):
    """Represents a <NeuralLayer> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(NeuralLayer, self).__init__()
        self.numberOfNeurons = None
        self.activationFunction = None
        self.threshold = None
        self.width = None
        self.altitude = None
        self.normalizationMethod = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Neuron = []
        
class NeuralNetwork(ind.NeuralNetwork):
    """Represents a <NeuralNetwork> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(NeuralNetwork, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.activationFunction = None
        self.normalizationMethod = None
        self.threshold = None
        self.width = None
        self.altitude = None
        self.numberOfLayers = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.NeuralInputs = []
        self.NeuralLayer = []
        self.NeuralOutputs = []
        self.Output = []
        self.Targets = []
        
class NeuralOutput(ind.NeuralOutput):
    """Represents a <NeuralOutput> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(NeuralOutput, self).__init__()
        self.outputNeuron = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.DerivedField = []
        self.Extension = []
        
class NeuralOutputs(ind.NeuralOutputs):
    """Represents a <NeuralOutputs> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(NeuralOutputs, self).__init__()
        self.numberOfOutputs = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.NeuralOutput = []
        
class Neuron(ind.Neuron):
    """Represents a <Neuron> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Neuron, self).__init__()
        self.id = None
        self.bias = None
        self.width = None
        self.altitude = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Con = []
        self.Extension = []
        
class Node(ind.Node):
    """Represents a <Node> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Node, self).__init__()
        self.id = None
        self.score = None
        self.recordCount = None
        self.defaultChild = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.CompoundPredicate = []
        self.DecisionTree = []
        self.Extension = []
        self.AlwaysFalse = []
        self.Node = []
        self.Partition = []
        self.Regression = []
        self.ScoreDistribution = []
        self.SimplePredicate = []
        self.SimpleSetPredicate = []
        self.AlwaysTrue = []
        
class NormContinuous(ind.NormContinuous):
    """Represents a <NormContinuous> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(NormContinuous, self).__init__()
        self.mapMissingTo = None
        self.field = None
        self.outliers = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.LinearNorm = []
        
class NormDiscrete(ind.NormDiscrete):
    """Represents a <NormDiscrete> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(NormDiscrete, self).__init__()
        self.field = None
        self.method = None
        self.value = None
        self.mapMissingTo = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class NormalizedCountTable(ind.NormalizedCountTable):
    """Represents a <NormalizedCountTable> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(NormalizedCountTable, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        
class NumericInfo(ind.NumericInfo):
    """Represents a <NumericInfo> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(NumericInfo, self).__init__()
        self.minimum = None
        self.maximum = None
        self.mean = None
        self.standardDeviation = None
        self.median = None
        self.interQuartileRange = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Quantile = []
        
class NumericPredictor(ind.NumericPredictor):
    """Represents a <NumericPredictor> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(NumericPredictor, self).__init__()
        self.name = None
        self.exponent = None
        self.coefficient = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class OptimumLiftGraph(ind.OptimumLiftGraph):
    """Represents a <OptimumLiftGraph> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(OptimumLiftGraph, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.LiftGraph = []
        
class Output(ind.Output):
    """Represents a <Output> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Output, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.OutputField = []
        
class OutputField(ind.OutputField):
    """Represents a <OutputField> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(OutputField, self).__init__()
        self.name = None
        self.displayName = None
        self.optype = None
        self.dataType = None
        self.targetField = None
        self.feature = None
        self.value = None
        self.ruleFeature = None
        self.algorithm = None
        self.rank = None
        self.rankBasis = None
        self.rankOrder = None
        self.isMultiValued = None
        self.segmentId = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Aggregate = []
        self.Apply = []
        self.Constant = []
        self.Decisions = []
        self.Discretize = []
        self.Extension = []
        self.FieldRef = []
        self.MapValues = []
        self.NormContinuous = []
        self.NormDiscrete = []
        self.TextIndex = []
        
class PCell(ind.PCell):
    """Represents a <PCell> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(PCell, self).__init__()
        self.targetCategory = None
        self.parameterName = None
        self.beta = None
        self.df = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class PCovCell(ind.PCovCell):
    """Represents a <PCovCell> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(PCovCell, self).__init__()
        self.pRow = None
        self.pCol = None
        self.tRow = None
        self.tCol = None
        self.value = None
        self.targetCategory = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class PCovMatrix(ind.PCovMatrix):
    """Represents a <PCovMatrix> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(PCovMatrix, self).__init__()
        self.type = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.PCovCell = []
        
class PPCell(ind.PPCell):
    """Represents a <PPCell> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(PPCell, self).__init__()
        self.value = None
        self.predictorName = None
        self.parameterName = None
        self.targetCategory = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class PPMatrix(ind.PPMatrix):
    """Represents a <PPMatrix> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(PPMatrix, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.PPCell = []
        
class PairCounts(ind.PairCounts):
    """Represents a <PairCounts> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(PairCounts, self).__init__()
        self.value = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.TargetValueCounts = []
        
class ParamMatrix(ind.ParamMatrix):
    """Represents a <ParamMatrix> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ParamMatrix, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.PCell = []
        
class Parameter(ind.Parameter):
    """Represents a <Parameter> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Parameter, self).__init__()
        self.name = None
        self.label = None
        self.referencePoint = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class ParameterField(ind.ParameterField):
    """Represents a <ParameterField> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ParameterField, self).__init__()
        self.name = None
        self.optype = None
        self.dataType = None
        for key, value in attribs.items():
            setattr(self, key, value)
        
class ParameterList(ind.ParameterList):
    """Represents a <ParameterList> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ParameterList, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Parameter = []
        
class Partition(ind.Partition):
    """Represents a <Partition> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Partition, self).__init__()
        self.name = None
        self.size = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.PartitionFieldStats = []
        
class PartitionFieldStats(ind.PartitionFieldStats):
    """Represents a <PartitionFieldStats> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(PartitionFieldStats, self).__init__()
        self.field = None
        self.weighted = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Counts = []
        self.Extension = []
        self.NumericInfo = []
        
class PoissonDistribution(ind.PoissonDistribution):
    """Represents a <PoissonDistribution> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(PoissonDistribution, self).__init__()
        self.mean = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class PolynomialKernelType(ind.PolynomialKernelType):
    """Represents a <PolynomialKernelType> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(PolynomialKernelType, self).__init__()
        self.description = None
        self.gamma = None
        self.coef0 = None
        self.degree = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class PredictiveModelQuality(ind.PredictiveModelQuality):
    """Represents a <PredictiveModelQuality> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(PredictiveModelQuality, self).__init__()
        self.targetField = None
        self.dataName = None
        self.dataUsage = None
        self.meanError = None
        self.meanAbsoluteError = None
        self.meanSquaredError = None
        self.rootMeanSquaredError = None
        self.r_squared = None
        self.adj_r_squared = None
        self.sumSquaredError = None
        self.sumSquaredRegression = None
        self.numOfRecords = None
        self.numOfRecordsWeighted = None
        self.numOfPredictors = None
        self.degreesOfFreedom = None
        self.fStatistic = None
        self.AIC = None
        self.BIC = None
        self.AICc = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.ConfusionMatrix = []
        self.Extension = []
        self.LiftData = []
        self.ROC = []
        
class Predictor(ind.Predictor):
    """Represents a <Predictor> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Predictor, self).__init__()
        self.name = None
        self.contrastMatrixType = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Categories = []
        self.Extension = []
        self.Matrix = []
        
class PredictorTerm(ind.PredictorTerm):
    """Represents a <PredictorTerm> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(PredictorTerm, self).__init__()
        self.name = None
        self.coefficient = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.FieldRef = []
        
class Quantile(ind.Quantile):
    """Represents a <Quantile> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Quantile, self).__init__()
        self.quantileLimit = None
        self.quantileValue = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class REAL_Entries(ind.REAL_Entries):
    """Represents a <REAL_Entries> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(REAL_Entries, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        
    @property
    def tag(self):
        return "REAL-Entries"

class REAL_SparseArray(ind.REAL_SparseArray):
    """Represents a <REAL_SparseArray> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(REAL_SparseArray, self).__init__()
        self.n = None
        self.defaultValue = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Indices = []
        self.REAL_Entries = []
        
    @property
    def tag(self):
        return "REAL-SparseArray"

class ROC(ind.ROC):
    """Represents a <ROC> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ROC, self).__init__()
        self.positiveTargetFieldValue = None
        self.positiveTargetFieldDisplayValue = None
        self.negativeTargetFieldValue = None
        self.negativeTargetFieldDisplayValue = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.ROCGraph = []
        
class ROCGraph(ind.ROCGraph):
    """Represents a <ROCGraph> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ROCGraph, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.BoundaryValues = []
        self.Extension = []
        self.XCoordinates = []
        self.YCoordinates = []
        
class RadialBasisKernelType(ind.RadialBasisKernelType):
    """Represents a <RadialBasisKernelType> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(RadialBasisKernelType, self).__init__()
        self.description = None
        self.gamma = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class RandomLiftGraph(ind.RandomLiftGraph):
    """Represents a <RandomLiftGraph> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(RandomLiftGraph, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.LiftGraph = []
        
class Regression(ind.Regression):
    """Represents a <Regression> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Regression, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.normalizationMethod = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.LocalTransformations = []
        self.ModelStats = []
        self.Output = []
        self.RegressionTable = []
        self.ResultField = []
        self.Targets = []
        
class RegressionModel(ind.RegressionModel):
    """Represents a <RegressionModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(RegressionModel, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.modelType = None
        self.targetFieldName = None
        self.normalizationMethod = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Output = []
        self.RegressionTable = []
        self.Targets = []
        
class RegressionTable(ind.RegressionTable):
    """Represents a <RegressionTable> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(RegressionTable, self).__init__()
        self.intercept = None
        self.targetCategory = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.CategoricalPredictor = []
        self.Extension = []
        self.NumericPredictor = []
        self.PredictorTerm = []
        
class ResultField(ind.ResultField):
    """Represents a <ResultField> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ResultField, self).__init__()
        self.name = None
        self.displayName = None
        self.optype = None
        self.dataType = None
        self.feature = None
        self.value = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class RuleSelectionMethod(ind.RuleSelectionMethod):
    """Represents a <RuleSelectionMethod> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(RuleSelectionMethod, self).__init__()
        self.criterion = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class RuleSet(ind.RuleSet):
    """Represents a <RuleSet> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(RuleSet, self).__init__()
        self.recordCount = None
        self.nbCorrect = None
        self.defaultScore = None
        self.defaultConfidence = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.CompoundRule = []
        self.Extension = []
        self.RuleSelectionMethod = []
        self.ScoreDistribution = []
        self.SimpleRule = []
        
class RuleSetModel(ind.RuleSetModel):
    """Represents a <RuleSetModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(RuleSetModel, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Output = []
        self.RuleSet = []
        self.Targets = []
        
class ScoreDistribution(ind.ScoreDistribution):
    """Represents a <ScoreDistribution> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(ScoreDistribution, self).__init__()
        self.value = None
        self.recordCount = None
        self.confidence = None
        self.probability = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Scorecard(ind.Scorecard):
    """Represents a <Scorecard> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Scorecard, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.initialScore = None
        self.useReasonCodes = None
        self.reasonCodeAlgorithm = None
        self.baselineScore = None
        self.baselineMethod = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Characteristics = []
        self.Extension = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Output = []
        self.Targets = []
        
class SeasonalTrendDecomposition(ind.SeasonalTrendDecomposition):
    """Represents a <SeasonalTrendDecomposition> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SeasonalTrendDecomposition, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        
class Seasonality_ExpoSmooth(ind.Seasonality_ExpoSmooth):
    """Represents a <Seasonality_ExpoSmooth> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Seasonality_ExpoSmooth, self).__init__()
        self.type = None
        self.period = None
        self.unit = None
        self.phase = None
        self.delta = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        
class Segment(ind.Segment):
    """Represents a <Segment> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Segment, self).__init__()
        self.id = None
        self.weight = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.AssociationModel = []
        self.BaselineModel = []
        self.ClusteringModel = []
        self.CompoundPredicate = []
        self.Extension = []
        self.AlwaysFalse = []
        self.GeneralRegressionModel = []
        self.MiningModel = []
        self.NaiveBayesModel = []
        self.NearestNeighborModel = []
        self.NeuralNetwork = []
        self.RegressionModel = []
        self.RuleSetModel = []
        self.Scorecard = []
        self.SequenceModel = []
        self.SimplePredicate = []
        self.SimpleSetPredicate = []
        self.SupportVectorMachineModel = []
        self.TextModel = []
        self.TimeSeriesModel = []
        self.TreeModel = []
        self.AlwaysTrue = []
        
class Segmentation(ind.Segmentation):
    """Represents a <Segmentation> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Segmentation, self).__init__()
        self.multipleModelMethod = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Segment = []
        
class Sequence(ind.Sequence):
    """Represents a <Sequence> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Sequence, self).__init__()
        self.id = None
        self.numberOfSets = None
        self.occurrence = None
        self.support = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Delimiter = []
        self.Extension = []
        self.SetReference = []
        self.Time = []
        
class SequenceModel(ind.SequenceModel):
    """Represents a <SequenceModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SequenceModel, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.numberOfTransactions = None
        self.maxNumberOfItemsPerTransaction = None
        self.avgNumberOfItemsPerTransaction = None
        self.numberOfTransactionGroups = None
        self.maxNumberOfTAsPerTAGroup = None
        self.avgNumberOfTAsPerTAGroup = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Constraints = []
        self.Extension = []
        self.Item = []
        self.Itemset = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelStats = []
        self.Sequence = []
        self.SequenceRule = []
        self.SetPredicate = []
        
class SequenceReference(ind.SequenceReference):
    """Represents a <SequenceReference> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SequenceReference, self).__init__()
        self.seqId = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class SequenceRule(ind.SequenceRule):
    """Represents a <SequenceRule> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SequenceRule, self).__init__()
        self.id = None
        self.numberOfSets = None
        self.occurrence = None
        self.support = None
        self.confidence = None
        self.lift = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.AntecedentSequence = []
        self.ConsequentSequence = []
        self.Delimiter = []
        self.Extension = []
        self.Time = []
        
class SetPredicate(ind.SetPredicate):
    """Represents a <SetPredicate> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SetPredicate, self).__init__()
        self.id = None
        self.field = None
        self.operator = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Extension = []
        
class SetReference(ind.SetReference):
    """Represents a <SetReference> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SetReference, self).__init__()
        self.setId = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class SigmoidKernelType(ind.SigmoidKernelType):
    """Represents a <SigmoidKernelType> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SigmoidKernelType, self).__init__()
        self.description = None
        self.gamma = None
        self.coef0 = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class SimplePredicate(ind.SimplePredicate):
    """Represents a <SimplePredicate> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SimplePredicate, self).__init__()
        self.field = None
        self.operator = None
        self.value = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class SimpleRule(ind.SimpleRule):
    """Represents a <SimpleRule> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SimpleRule, self).__init__()
        self.id = None
        self.score = None
        self.recordCount = None
        self.nbCorrect = None
        self.confidence = None
        self.weight = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.CompoundPredicate = []
        self.Extension = []
        self.AlwaysFalse = []
        self.ScoreDistribution = []
        self.SimplePredicate = []
        self.SimpleSetPredicate = []
        self.AlwaysTrue = []
        
class SimpleSetPredicate(ind.SimpleSetPredicate):
    """Represents a <SimpleSetPredicate> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SimpleSetPredicate, self).__init__()
        self.field = None
        self.booleanOperator = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Extension = []
        
class SpectralAnalysis(ind.SpectralAnalysis):
    """Represents a <SpectralAnalysis> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SpectralAnalysis, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        
class SupportVector(ind.SupportVector):
    """Represents a <SupportVector> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SupportVector, self).__init__()
        self.vectorId = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class SupportVectorMachine(ind.SupportVectorMachine):
    """Represents a <SupportVectorMachine> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SupportVectorMachine, self).__init__()
        self.targetCategory = None
        self.alternateTargetCategory = None
        self.threshold = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Coefficients = []
        self.Extension = []
        self.SupportVectors = []
        
class SupportVectorMachineModel(ind.SupportVectorMachineModel):
    """Represents a <SupportVectorMachineModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SupportVectorMachineModel, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.threshold = None
        self.svmRepresentation = None
        self.classificationMethod = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.LinearKernelType = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Output = []
        self.PolynomialKernelType = []
        self.RadialBasisKernelType = []
        self.SigmoidKernelType = []
        self.SupportVectorMachine = []
        self.Targets = []
        self.VectorDictionary = []
        
class SupportVectors(ind.SupportVectors):
    """Represents a <SupportVectors> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(SupportVectors, self).__init__()
        self.numberOfSupportVectors = None
        self.numberOfAttributes = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.SupportVector = []
        
class TableLocator(ind.TableLocator):
    """Represents a <TableLocator> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TableLocator, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Target(ind.Target):
    """Represents a <Target> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Target, self).__init__()
        self.field = None
        self.optype = None
        self.castInteger = None
        self.min = None
        self.max = None
        self.rescaleConstant = None
        self.rescaleFactor = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.TargetValue = []
        
class TargetValue(ind.TargetValue):
    """Represents a <TargetValue> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TargetValue, self).__init__()
        self.value = None
        self.displayValue = None
        self.priorProbability = None
        self.defaultValue = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Partition = []
        
class TargetValueCount(ind.TargetValueCount):
    """Represents a <TargetValueCount> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TargetValueCount, self).__init__()
        self.value = None
        self.count = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class TargetValueCounts(ind.TargetValueCounts):
    """Represents a <TargetValueCounts> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TargetValueCounts, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.TargetValueCount = []
        
class TargetValueStat(ind.TargetValueStat):
    """Represents a <TargetValueStat> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TargetValueStat, self).__init__()
        self.value = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.AnyDistribution = []
        self.Extension = []
        self.GaussianDistribution = []
        self.PoissonDistribution = []
        self.UniformDistribution = []
        
class TargetValueStats(ind.TargetValueStats):
    """Represents a <TargetValueStats> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TargetValueStats, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.TargetValueStat = []
        
class Targets(ind.Targets):
    """Represents a <Targets> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Targets, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.Target = []
        
class Taxonomy(ind.Taxonomy):
    """Represents a <Taxonomy> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Taxonomy, self).__init__()
        self.name = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.ChildParent = []
        self.Extension = []
        
class TestDistributions(ind.TestDistributions):
    """Represents a <TestDistributions> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TestDistributions, self).__init__()
        self.field = None
        self.testStatistic = None
        self.resetValue = None
        self.windowSize = None
        self.weightField = None
        self.normalizationScheme = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Alternate = []
        self.Baseline = []
        self.Extension = []
        
class TextCorpus(ind.TextCorpus):
    """Represents a <TextCorpus> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TextCorpus, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.TextDocument = []
        
class TextDictionary(ind.TextDictionary):
    """Represents a <TextDictionary> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TextDictionary, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Extension = []
        self.Taxonomy = []
        
class TextDocument(ind.TextDocument):
    """Represents a <TextDocument> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TextDocument, self).__init__()
        self.id = None
        self.name = None
        self.length = None
        self.file = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class TextIndex(ind.TextIndex):
    """Represents a <TextIndex> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TextIndex, self).__init__()
        self.textField = None
        self.localTermWeights = None
        self.isCaseSensitive = None
        self.maxLevenshteinDistance = None
        self.countHits = None
        self.wordSeparatorCharacterRE = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Aggregate = []
        self.Apply = []
        self.Constant = []
        self.Discretize = []
        self.Extension = []
        self.FieldRef = []
        self.MapValues = []
        self.NormContinuous = []
        self.NormDiscrete = []
        self.TextIndex = []
        self.TextIndexNormalization = []
        
class TextIndexNormalization(ind.TextIndexNormalization):
    """Represents a <TextIndexNormalization> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TextIndexNormalization, self).__init__()
        self.inField = None
        self.outField = None
        self.regexField = None
        self.recursive = None
        self.isCaseSensitive = None
        self.maxLevenshteinDistance = None
        self.wordSeparatorCharacterRE = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.InlineTable = []
        self.TableLocator = []
        
class TextModel(ind.TextModel):
    """Represents a <TextModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TextModel, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.numberOfTerms = None
        self.numberOfDocuments = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.DocumentTermMatrix = []
        self.Extension = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Output = []
        self.Targets = []
        self.TextCorpus = []
        self.TextDictionary = []
        self.TextModelNormalization = []
        self.TextModelSimiliarity = []
        
class TextModelNormalization(ind.TextModelNormalization):
    """Represents a <TextModelNormalization> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TextModelNormalization, self).__init__()
        self.localTermWeights = None
        self.globalTermWeights = None
        self.documentNormalization = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class TextModelSimiliarity(ind.TextModelSimiliarity):
    """Represents a <TextModelSimiliarity> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TextModelSimiliarity, self).__init__()
        self.similarityType = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class Time(ind.Time):
    """Represents a <Time> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Time, self).__init__()
        self.min = None
        self.max = None
        self.mean = None
        self.standardDeviation = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class TimeAnchor(ind.TimeAnchor):
    """Represents a <TimeAnchor> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TimeAnchor, self).__init__()
        self.type = None
        self.offset = None
        self.stepsize = None
        self.displayName = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.TimeCycle = []
        self.TimeException = []
        
class TimeCycle(ind.TimeCycle):
    """Represents a <TimeCycle> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TimeCycle, self).__init__()
        self.length = None
        self.type = None
        self.displayName = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        
class TimeException(ind.TimeException):
    """Represents a <TimeException> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TimeException, self).__init__()
        self.type = None
        self.count = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        
class TimeSeries(ind.TimeSeries):
    """Represents a <TimeSeries> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TimeSeries, self).__init__()
        self.usage = None
        self.startTime = None
        self.endTime = None
        self.interpolationMethod = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.TimeAnchor = []
        self.TimeValue = []
        
class TimeSeriesModel(ind.TimeSeriesModel):
    """Represents a <TimeSeriesModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TimeSeriesModel, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.bestFit = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.ARIMA = []
        self.ExponentialSmoothing = []
        self.Extension = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Output = []
        self.SeasonalTrendDecomposition = []
        self.SpectralAnalysis = []
        self.TimeSeries = []
        
class TimeValue(ind.TimeValue):
    """Represents a <TimeValue> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TimeValue, self).__init__()
        self.index = None
        self.time = None
        self.value = None
        self.standardError = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Timestamp = []
        
class Timestamp(ind.Timestamp):
    """Represents a <Timestamp> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Timestamp, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class TrainingInstances(ind.TrainingInstances):
    """Represents a <TrainingInstances> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TrainingInstances, self).__init__()
        self.isTransformed = None
        self.recordCount = None
        self.fieldCount = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.InlineTable = []
        self.InstanceFields = []
        self.TableLocator = []
        
class TransformationDictionary(ind.TransformationDictionary):
    """Represents a <TransformationDictionary> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TransformationDictionary, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.DefineFunction = []
        self.DerivedField = []
        self.Extension = []
        
class TreeModel(ind.TreeModel):
    """Represents a <TreeModel> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(TreeModel, self).__init__()
        self.modelName = None
        self.functionName = None
        self.algorithmName = None
        self.missingValueStrategy = None
        self.missingValuePenalty = None
        self.noTrueChildStrategy = None
        self.splitCharacteristic = None
        self.isScorable = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.LocalTransformations = []
        self.MiningSchema = []
        self.ModelExplanation = []
        self.ModelStats = []
        self.ModelVerification = []
        self.Node = []
        self.Output = []
        self.Targets = []
        
class Trend_ExpoSmooth(ind.Trend_ExpoSmooth):
    """Represents a <Trend_ExpoSmooth> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Trend_ExpoSmooth, self).__init__()
        self.trend = None
        self.gamma = None
        self.phi = None
        self.smoothedValue = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        
class AlwaysTrue(ind.AlwaysTrue):
    """Represents a <AlwaysTrue> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(AlwaysTrue, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
    @property
    def tag(self):
        return "True"

class UniformDistribution(ind.UniformDistribution):
    """Represents a <UniformDistribution> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(UniformDistribution, self).__init__()
        self.lower = None
        self.upper = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class UnivariateStats(ind.UnivariateStats):
    """Represents a <UnivariateStats> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(UnivariateStats, self).__init__()
        self.field = None
        self.weighted = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Anova = []
        self.ContStats = []
        self.Counts = []
        self.DiscrStats = []
        self.Extension = []
        self.NumericInfo = []
        
class Value(ind.Value):
    """Represents a <Value> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(Value, self).__init__()
        self.value = None
        self.displayValue = None
        self.property = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class VectorDictionary(ind.VectorDictionary):
    """Represents a <VectorDictionary> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(VectorDictionary, self).__init__()
        self.numberOfVectors = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.VectorFields = []
        self.VectorInstance = []
        
class VectorFields(ind.VectorFields):
    """Represents a <VectorFields> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(VectorFields, self).__init__()
        self.numberOfFields = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.FieldRef = []
        
class VectorInstance(ind.VectorInstance):
    """Represents a <VectorInstance> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(VectorInstance, self).__init__()
        self.id = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Extension = []
        self.REAL_SparseArray = []
        
class VerificationField(ind.VerificationField):
    """Represents a <VerificationField> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(VerificationField, self).__init__()
        self.field = None
        self.column = None
        self.precision = None
        self.zeroThreshold = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class VerificationFields(ind.VerificationFields):
    """Represents a <VerificationFields> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(VerificationFields, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        self.VerificationField = []
        
class XCoordinates(ind.XCoordinates):
    """Represents a <XCoordinates> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(XCoordinates, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Extension = []
        
class YCoordinates(ind.YCoordinates):
    """Represents a <YCoordinates> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(YCoordinates, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Array = []
        self.Extension = []
        
class binarySimilarity(ind.binarySimilarity):
    """Represents a <binarySimilarity> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(binarySimilarity, self).__init__()
        self.c00_parameter = None
        self.c01_parameter = None
        self.c10_parameter = None
        self.c11_parameter = None
        self.d00_parameter = None
        self.d01_parameter = None
        self.d10_parameter = None
        self.d11_parameter = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class chebychev(ind.chebychev):
    """Represents a <chebychev> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(chebychev, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class cityBlock(ind.cityBlock):
    """Represents a <cityBlock> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(cityBlock, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class euclidean(ind.euclidean):
    """Represents a <euclidean> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(euclidean, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class jaccard(ind.jaccard):
    """Represents a <jaccard> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(jaccard, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class minkowski(ind.minkowski):
    """Represents a <minkowski> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(minkowski, self).__init__()
        self.p_parameter = None
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class row(ind.row):
    """Represents a <row> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(row, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        
class simpleMatching(ind.simpleMatching):
    """Represents a <simpleMatching> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(simpleMatching, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class squaredEuclidean(ind.squaredEuclidean):
    """Represents a <squaredEuclidean> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(squaredEuclidean, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
class tanimoto(ind.tanimoto):
    """Represents a <tanimoto> tag in v4.2 and provides methods to convert to PFA."""
    def __init__(self, attribs):
        super(tanimoto, self).__init__()
        for key, value in attribs.items():
            setattr(self, key, value)
        self.Extension = []
        
tagToClass = {
    "ARIMA": ARIMA,
    "Aggregate": Aggregate,
    "Alternate": Alternate,
    "Annotation": Annotation,
    "Anova": Anova,
    "AnovaRow": AnovaRow,
    "AntecedentSequence": AntecedentSequence,
    "AnyDistribution": AnyDistribution,
    "Application": Application,
    "Apply": Apply,
    "Array": Array,
    "AssociationModel": AssociationModel,
    "AssociationRule": AssociationRule,
    "Attribute": Attribute,
    "BaseCumHazardTables": BaseCumHazardTables,
    "Baseline": Baseline,
    "BaselineCell": BaselineCell,
    "BaselineModel": BaselineModel,
    "BaselineStratum": BaselineStratum,
    "BayesInput": BayesInput,
    "BayesInputs": BayesInputs,
    "BayesOutput": BayesOutput,
    "BoundaryValueMeans": BoundaryValueMeans,
    "BoundaryValues": BoundaryValues,
    "CategoricalPredictor": CategoricalPredictor,
    "Categories": Categories,
    "Category": Category,
    "Characteristic": Characteristic,
    "Characteristics": Characteristics,
    "ChildParent": ChildParent,
    "ClassLabels": ClassLabels,
    "Cluster": Cluster,
    "ClusteringField": ClusteringField,
    "ClusteringModel": ClusteringModel,
    "ClusteringModelQuality": ClusteringModelQuality,
    "Coefficient": Coefficient,
    "Coefficients": Coefficients,
    "ComparisonMeasure": ComparisonMeasure,
    "Comparisons": Comparisons,
    "ComplexPartialScore": ComplexPartialScore,
    "CompoundPredicate": CompoundPredicate,
    "CompoundRule": CompoundRule,
    "Con": Con,
    "ConfusionMatrix": ConfusionMatrix,
    "ConsequentSequence": ConsequentSequence,
    "Constant": Constant,
    "Constraints": Constraints,
    "ContStats": ContStats,
    "CorrelationFields": CorrelationFields,
    "CorrelationMethods": CorrelationMethods,
    "CorrelationValues": CorrelationValues,
    "Correlations": Correlations,
    "CountTable": CountTable,
    "Counts": Counts,
    "Covariances": Covariances,
    "CovariateList": CovariateList,
    "DataDictionary": DataDictionary,
    "DataField": DataField,
    "Decision": Decision,
    "DecisionTree": DecisionTree,
    "Decisions": Decisions,
    "DefineFunction": DefineFunction,
    "Delimiter": Delimiter,
    "DerivedField": DerivedField,
    "DiscrStats": DiscrStats,
    "Discretize": Discretize,
    "DiscretizeBin": DiscretizeBin,
    "DocumentTermMatrix": DocumentTermMatrix,
    "EventValues": EventValues,
    "ExponentialSmoothing": ExponentialSmoothing,
    "Extension": Extension,
    "FactorList": FactorList,
    "False": AlwaysFalse,
    "FieldColumnPair": FieldColumnPair,
    "FieldRef": FieldRef,
    "FieldValue": FieldValue,
    "FieldValueCount": FieldValueCount,
    "GaussianDistribution": GaussianDistribution,
    "GeneralRegressionModel": GeneralRegressionModel,
    "Header": Header,
    "INT-Entries": INT_Entries,
    "INT-SparseArray": INT_SparseArray,
    "Indices": Indices,
    "InlineTable": InlineTable,
    "InstanceField": InstanceField,
    "InstanceFields": InstanceFields,
    "Interval": Interval,
    "Item": Item,
    "ItemRef": ItemRef,
    "Itemset": Itemset,
    "KNNInput": KNNInput,
    "KNNInputs": KNNInputs,
    "KohonenMap": KohonenMap,
    "Level": Level,
    "LiftData": LiftData,
    "LiftGraph": LiftGraph,
    "LinearKernelType": LinearKernelType,
    "LinearNorm": LinearNorm,
    "LocalTransformations": LocalTransformations,
    "MapValues": MapValues,
    "MatCell": MatCell,
    "Matrix": Matrix,
    "MiningBuildTask": MiningBuildTask,
    "MiningField": MiningField,
    "MiningModel": MiningModel,
    "MiningSchema": MiningSchema,
    "MissingValueWeights": MissingValueWeights,
    "ModelExplanation": ModelExplanation,
    "ModelLiftGraph": ModelLiftGraph,
    "ModelStats": ModelStats,
    "ModelVerification": ModelVerification,
    "MultivariateStat": MultivariateStat,
    "MultivariateStats": MultivariateStats,
    "NaiveBayesModel": NaiveBayesModel,
    "NearestNeighborModel": NearestNeighborModel,
    "NeuralInput": NeuralInput,
    "NeuralInputs": NeuralInputs,
    "NeuralLayer": NeuralLayer,
    "NeuralNetwork": NeuralNetwork,
    "NeuralOutput": NeuralOutput,
    "NeuralOutputs": NeuralOutputs,
    "Neuron": Neuron,
    "Node": Node,
    "NormContinuous": NormContinuous,
    "NormDiscrete": NormDiscrete,
    "NormalizedCountTable": NormalizedCountTable,
    "NumericInfo": NumericInfo,
    "NumericPredictor": NumericPredictor,
    "OptimumLiftGraph": OptimumLiftGraph,
    "Output": Output,
    "OutputField": OutputField,
    "PCell": PCell,
    "PCovCell": PCovCell,
    "PCovMatrix": PCovMatrix,
    "PMML": PMML,
    "PPCell": PPCell,
    "PPMatrix": PPMatrix,
    "PairCounts": PairCounts,
    "ParamMatrix": ParamMatrix,
    "Parameter": Parameter,
    "ParameterField": ParameterField,
    "ParameterList": ParameterList,
    "Partition": Partition,
    "PartitionFieldStats": PartitionFieldStats,
    "PoissonDistribution": PoissonDistribution,
    "PolynomialKernelType": PolynomialKernelType,
    "PredictiveModelQuality": PredictiveModelQuality,
    "Predictor": Predictor,
    "PredictorTerm": PredictorTerm,
    "Quantile": Quantile,
    "REAL-Entries": REAL_Entries,
    "REAL-SparseArray": REAL_SparseArray,
    "ROC": ROC,
    "ROCGraph": ROCGraph,
    "RadialBasisKernelType": RadialBasisKernelType,
    "RandomLiftGraph": RandomLiftGraph,
    "Regression": Regression,
    "RegressionModel": RegressionModel,
    "RegressionTable": RegressionTable,
    "ResultField": ResultField,
    "RuleSelectionMethod": RuleSelectionMethod,
    "RuleSet": RuleSet,
    "RuleSetModel": RuleSetModel,
    "ScoreDistribution": ScoreDistribution,
    "Scorecard": Scorecard,
    "SeasonalTrendDecomposition": SeasonalTrendDecomposition,
    "Seasonality_ExpoSmooth": Seasonality_ExpoSmooth,
    "Segment": Segment,
    "Segmentation": Segmentation,
    "Sequence": Sequence,
    "SequenceModel": SequenceModel,
    "SequenceReference": SequenceReference,
    "SequenceRule": SequenceRule,
    "SetPredicate": SetPredicate,
    "SetReference": SetReference,
    "SigmoidKernelType": SigmoidKernelType,
    "SimplePredicate": SimplePredicate,
    "SimpleRule": SimpleRule,
    "SimpleSetPredicate": SimpleSetPredicate,
    "SpectralAnalysis": SpectralAnalysis,
    "SupportVector": SupportVector,
    "SupportVectorMachine": SupportVectorMachine,
    "SupportVectorMachineModel": SupportVectorMachineModel,
    "SupportVectors": SupportVectors,
    "TableLocator": TableLocator,
    "Target": Target,
    "TargetValue": TargetValue,
    "TargetValueCount": TargetValueCount,
    "TargetValueCounts": TargetValueCounts,
    "TargetValueStat": TargetValueStat,
    "TargetValueStats": TargetValueStats,
    "Targets": Targets,
    "Taxonomy": Taxonomy,
    "TestDistributions": TestDistributions,
    "TextCorpus": TextCorpus,
    "TextDictionary": TextDictionary,
    "TextDocument": TextDocument,
    "TextIndex": TextIndex,
    "TextIndexNormalization": TextIndexNormalization,
    "TextModel": TextModel,
    "TextModelNormalization": TextModelNormalization,
    "TextModelSimiliarity": TextModelSimiliarity,
    "Time": Time,
    "TimeAnchor": TimeAnchor,
    "TimeCycle": TimeCycle,
    "TimeException": TimeException,
    "TimeSeries": TimeSeries,
    "TimeSeriesModel": TimeSeriesModel,
    "TimeValue": TimeValue,
    "Timestamp": Timestamp,
    "TrainingInstances": TrainingInstances,
    "TransformationDictionary": TransformationDictionary,
    "TreeModel": TreeModel,
    "Trend_ExpoSmooth": Trend_ExpoSmooth,
    "True": AlwaysTrue,
    "UniformDistribution": UniformDistribution,
    "UnivariateStats": UnivariateStats,
    "Value": Value,
    "VectorDictionary": VectorDictionary,
    "VectorFields": VectorFields,
    "VectorInstance": VectorInstance,
    "VerificationField": VerificationField,
    "VerificationFields": VerificationFields,
    "XCoordinates": XCoordinates,
    "YCoordinates": YCoordinates,
    "binarySimilarity": binarySimilarity,
    "chebychev": chebychev,
    "cityBlock": cityBlock,
    "euclidean": euclidean,
    "jaccard": jaccard,
    "minkowski": minkowski,
    "row": row,
    "simpleMatching": simpleMatching,
    "squaredEuclidean": squaredEuclidean,
    "tanimoto": tanimoto,
    }
